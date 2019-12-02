package org.skyline.tools.es.server;

import com.alibaba.fastjson.JSONObject;
import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.io.FileUtils;
import org.apache.lucene.index.SegmentCommitInfo;
import org.apache.lucene.index.SegmentInfo;
import org.apache.lucene.index.SegmentInfos;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.FSDirectory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;

/**
 * @author Sean Liu
 * @date 2019-11-26
 */
@Slf4j
@Service
public class IndexBuilder {

  @Value("#{'${workDir}'.split(',')}")
  private String[] workDirs;

  @Autowired
  private HdfsClient hdfsClient;

  @Autowired
  private ESClient esClient;

  private static final String STATE_DIR = "_state";

  public boolean build(Map<String, List<String>> idToShards, JSONObject configData) {
    String hdfsWorkDir = configData.getString("hdfsWorkDir");
    String indexName = configData.getString("indexName");

    Path localStateDir = Paths.get(Utils.mostFreeDir(workDirs), indexName);
    try {

      String hdfsStateDir = Paths.get(hdfsWorkDir, indexName, STATE_DIR).toString();
      String hdfsStateFile = hdfsClient.largestFileInDirectory(hdfsStateDir);

      hdfsClient.downloadFile(hdfsStateFile, localStateDir.resolve(STATE_DIR + ".zip").toString());
      Utils.unzip(localStateDir.resolve(STATE_DIR + ".zip"), localStateDir);
      Files.deleteIfExists(localStateDir.resolve(STATE_DIR + ".zip"));

    } catch (IOException e) {
      log.error("Download state file from hdfs failed", e);
      return false;
    }

    // TODO 这里先单线程操作,下载一个shard，merge一个shard
    for (String nodeId : idToShards.keySet()) {
      List<String> shards = idToShards.get(nodeId);
      String[] dataPaths = esClient.getDataPathByNodeId(nodeId);
      for (String shardId : shards) {
        // 选择最空闲的一个路径放索引
        String dataPath = Utils.mostFreeDir(dataPaths);
        log.info("Most free data dir is {}", dataPath);

        String srcPath = Paths.get(hdfsWorkDir, indexName, shardId).toString();
        String workDir = Utils.sameDiskDir(workDirs, dataPath);
        log.info("Chosen work dir is {}", workDir);
        String destPath = Paths.get(workDir, indexName, shardId).toString();

        log.info("Build index shard [{}] for node [{}]", shardId, nodeId);
        try {
          hdfsClient.downloadFolder(srcPath, destPath);
          log.info("Download index bundle from hdfs[{}] to local[{}]", srcPath, destPath);
          unzipBundles(destPath);
          log.info("Unzip index bundle path {}", destPath);
          String finalIndexPath = mergeIndex(destPath);
          log.info("Merge index bundle in dir[{}] ", destPath);

          // 从临时目录把索引移到es的data下面
          Path from = Paths.get(finalIndexPath);
          Path to = Paths.get(dataPath, "indices", indexName);

          Files.move(from, to);
          log.info("Move index from {} to {}", from, to);

          if (Files.list(to).filter(p -> p.toString().endsWith(STATE_DIR)).count() < 0) {
            Files.copy(localStateDir.resolve(STATE_DIR), to);
            log.info("Copy state file from {} to {}", localStateDir.resolve(STATE_DIR), to);
          } else {
            log.info("State file exists");
          }
        } catch (IOException e) {
          log.error(
              "Build index bundle from hdfs[" + srcPath + "] failed", e);
          return false;
        }
      }
    }
    try {
      Files.deleteIfExists(localStateDir);
    } catch (IOException e) {
      log.error("delete state file error", e);
    }
    return true;
  }


  private String mergeIndex(String indexBundlePath) throws IOException {
    Path path = Paths.get(indexBundlePath);

    log.info("start merge index for shard " + path.getFileName());
    List<Path> indexList = Files.list(path).filter(p -> Files.isDirectory(p))
        .collect(Collectors.toList());
    Collections.sort(indexList);
    try (
        FSDirectory directory = FSDirectory.open(indexList.get(0).resolve("index"))
    ) {
      SegmentInfos segmentInfos = SegmentInfos.readLatestCommit(directory);
      String originSegmentFileName = segmentInfos.getSegmentsFileName();
      Path originSegmentPath = directory.getDirectory().resolve(originSegmentFileName);
      log.info("Original segment info file path is {}", originSegmentPath.toString());
      List<SegmentCommitInfo> infos = new ArrayList<>();

      for (int i = 1; i < indexList.size(); i++) {
        FSDirectory dir = FSDirectory.open(indexList.get(i).resolve("index"));
        SegmentInfos sis = SegmentInfos.readLatestCommit(dir);
        for (SegmentCommitInfo info : sis) {
          String newSegName = newSegmentName(segmentInfos);
          log.info("New segment name is {}", newSegName);
          infos.add(copySegmentAsIs(directory, info, newSegName));
        }
      }
      segmentInfos.addAll(infos);
      SegmentInfos pendingCommit = segmentInfos.clone();
      Method prepareCommit = pendingCommit.getClass()
          .getDeclaredMethod("prepareCommit", Directory.class);
      prepareCommit.setAccessible(true);
      prepareCommit.invoke(pendingCommit, directory);

      log.info("Add pending segment info file");

      Method updateGeneration = segmentInfos.getClass()
          .getDeclaredMethod("updateGeneration", SegmentInfos.class);
      updateGeneration.setAccessible(true);
      updateGeneration.invoke(segmentInfos, pendingCommit);

      log.info("Update segment info generation");

      Method finishCommit = pendingCommit.getClass()
          .getDeclaredMethod("finishCommit", Directory.class);
      finishCommit.setAccessible(true);
      finishCommit.invoke(pendingCommit, directory);

      log.info("Finish segment info commit");

      Files.delete(originSegmentPath);
      log.info("Delete origin segment info file {}", originSegmentPath.toString());

      log.info("merge index for shard " + path.getFileName() + " done");
    } catch (IOException | NoSuchMethodException | IllegalAccessException | InvocationTargetException e) {
      log.error("Merge index for shard " + path.getFileName() + " error", e);
    }
    return indexList.get(0).toString();
  }

  private SegmentCommitInfo copySegmentAsIs(Directory directory, SegmentCommitInfo info,
      String segName) throws IOException {

    SegmentInfo newInfo = new SegmentInfo(directory, info.info.getVersion(), segName,
        info.info.maxDoc(),
        info.info.getUseCompoundFile(), info.info.getCodec(),
        info.info.getDiagnostics(), info.info.getId(), info.info.getAttributes());
    SegmentCommitInfo newInfoPerCommit = new SegmentCommitInfo(newInfo, info.getDelCount(),
        info.getDelGen(),
        info.getFieldInfosGen(), info.getDocValuesGen());

    newInfo.setFiles(info.files());

    boolean success = false;

    Set<String> copiedFiles = new HashSet<>();
    try {
      // Copy the segment's files
      for (String file : info.files()) {
        Method namedForThisSegment = newInfo.getClass()
            .getDeclaredMethod("namedForThisSegment", String.class);
        namedForThisSegment.setAccessible(true);
        final String newFileName = (String) namedForThisSegment.invoke(newInfo, file);

        FSDirectory srcDir = (FSDirectory) info.info.dir;
        FSDirectory destDir = (FSDirectory) directory;
        Path srcFile = srcDir.getDirectory().resolve(file);
        Path destFile = destDir.getDirectory().resolve(newFileName);
        Files.move(srcFile, destFile);
        log.info("Move index file from {} to {}", srcFile.toString(), destFile.toString());
        copiedFiles.add(newFileName);
      }
      success = true;
    } catch (NoSuchMethodException | IllegalAccessException | InvocationTargetException e) {
      log.error("Get new segment name error", e);
    } finally {
      if (!success) {
//        deleteNewFiles(copiedFiles);
      }
    }

    assert copiedFiles.equals(newInfoPerCommit.files());

    return newInfoPerCommit;
  }

  public String newSegmentName(SegmentInfos segmentInfos) {
    segmentInfos.changed();
    return "_" + Integer.toString(segmentInfos.counter++, Character.MAX_RADIX);
  }

  public void unzipBundles(String indexBundlePath) throws IOException {
    Path zipPath = Paths.get(indexBundlePath);
    Files.list(zipPath).filter(x -> !x.toString().endsWith("crc")).parallel().forEach(path -> {
      try {
        log.info("unzip index bundle : " + path.toString());
        Utils.unzip(path, zipPath);
        FileUtils.forceDelete(path.toFile());
        log.info("delete index bundle file : " + path.toString());
      } catch (IOException e) {
        log.error("unzip index bundle failed : " + path.toString());
      }
    });
  }

}
