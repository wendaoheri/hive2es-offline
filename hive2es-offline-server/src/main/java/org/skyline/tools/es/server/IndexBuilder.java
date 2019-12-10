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

  private static final String SHARD_STATE = "_shard_state";

  public boolean build(Map<String, List<String>> idToShards, JSONObject configData) {

    String hdfsWorkDir = configData.getString("hdfsWorkDir");
    String indexName = configData.getString("indexName");

    Path localStateDir = Paths.get(Utils.mostFreeDir(workDirs), indexName);
    if (downloadStateFile(hdfsWorkDir, indexName, localStateDir)) {
      if (downloadAndMergeAllShards(idToShards, hdfsWorkDir, indexName, localStateDir)) {
        try {
          FileUtils.deleteDirectory(localStateDir.resolve(STATE_DIR).toFile());
          log.info("Delete state file {}", localStateDir.resolve(STATE_DIR));
        } catch (Exception e) {
          log.error("delete state file error", e);
        }
        return true;
      }
    }
    return false;
  }

  private boolean downloadAndMergeAllShards(Map<String, List<String>> idToShards,
      String hdfsWorkDir, String indexName, Path localStateDir) {
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
          Path to = Paths.get(dataPath, "indices", indexName, shardId);

          if (!Files.exists(to.getParent())) {
            log.info("Create index folder : {}", to.getParent());
            Files.createDirectories(to.getParent());
          }

          log.info("Move index from {} to {}", from, to);
          Files.move(from, to);
          log.info("Delete old shard _state file {}", to.resolve(STATE_DIR));
          FileUtils.deleteDirectory(to.resolve(STATE_DIR).toFile());
          log.info("Copy new shard _state file from {} to {}", localStateDir.resolve(SHARD_STATE),
              to.resolve(STATE_DIR));
          FileUtils.copyDirectory(localStateDir.resolve(SHARD_STATE).toFile(),
              to.resolve(STATE_DIR).toFile());

          if (Files.list(to.getParent()).filter(p -> p.toString().endsWith(STATE_DIR)).count()
              == 0L) {
            FileUtils.copyDirectory(localStateDir.resolve(STATE_DIR).toFile(),
                to.getParent().resolve(STATE_DIR).toFile());
            Utils.setPermissionRecursive(to.getParent().resolve(STATE_DIR));
            Utils.setPermissions(to.getParent());
            log.info("Copy state file from {} to {}", localStateDir.resolve(STATE_DIR),
                to.getParent().resolve(STATE_DIR));
          } else {
            log.info("State file exists");
          }

          Utils.setPermissionRecursive(to);
        } catch (IOException e) {
          log.error(
              "Build index bundle from hdfs[" + srcPath + "] failed", e);
          return true;
        }
      }
    }
    return true;
  }

  private boolean downloadStateFile(String hdfsWorkDir, String indexName, Path localStateDir) {
    log.info("Local state dir is {}", localStateDir.toString());
    try {
      // download & unzip index state
      String hdfsStateDir = Paths.get(hdfsWorkDir, indexName, STATE_DIR).toString();
      String hdfsStateFile = hdfsClient.largestFileInDirectory(hdfsStateDir);
      log.info("Download index state file from {}", hdfsStateFile);
      hdfsClient.downloadFile(hdfsStateFile, localStateDir.resolve(STATE_DIR + ".zip").toString());
      Utils.unzip(localStateDir.resolve(STATE_DIR + ".zip"), localStateDir);
      Files.deleteIfExists(localStateDir.resolve(STATE_DIR + ".zip"));

      // download & unzip shard state
      String hdfsShardStateFile = Paths.get(hdfsWorkDir, indexName, SHARD_STATE + ".zip")
          .toString();
      log.info("Download shard state file from {}", hdfsShardStateFile);
      hdfsClient
          .downloadFile(hdfsShardStateFile, localStateDir.resolve(SHARD_STATE + ".zip").toString());
      Utils.unzip(localStateDir.resolve(SHARD_STATE + ".zip"), localStateDir);
      Files.deleteIfExists(localStateDir.resolve(SHARD_STATE + ".zip"));
      return true;
    } catch (IOException e) {
      log.error("Download state file from hdfs failed", e);
      return false;
    }
  }

  /**
   * 使用Lucene合并索引会非常慢，所以这里直接进行文件移动，然后重新生成segment信息
   */
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
        log.debug("Move index file from {} to {}", srcFile.toString(), destFile.toString());
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
    List<Path> zipFiles = Files.list(zipPath)
        .filter(x -> !x.toString().endsWith("crc") && !x.toFile().isDirectory())
        .collect(Collectors.toList());
    zipFiles.parallelStream().forEach(path -> {
      try {
        log.info("unzip index bundle : " + path.toString());
        Utils.unzip(path, zipPath);
        FileUtils.forceDelete(path.toFile());
        log.info("delete index bundle file : " + path.toString());
      } catch (IOException e) {
        log.error("unzip index bundle failed", e);
      }
    });
  }

}
