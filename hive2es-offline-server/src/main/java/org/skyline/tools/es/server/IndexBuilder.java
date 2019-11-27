package org.skyline.tools.es.server;

import com.alibaba.fastjson.JSONObject;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.io.FileUtils;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
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

  @Value("${workDir}")
  private String workDir;

  @Autowired
  private HdfsClient hdfsClient;

  @Autowired
  private ESClient esClient;

  public void build(Map<String, List<String>> idToShards, JSONObject configData) {
    String hdfsWorkDir = configData.getString("hdfsWorkDir");
    String indexName = configData.getString("indexName");

    // TODO 这里先单线程操作,下载一个shard，merge一个shard
    for (String id : idToShards.keySet()) {
      List<String> shards = idToShards.get(id);
      String[] dataPaths = esClient.getDataPathByNodeId(id);
      for (String shardId : shards) {
        String srcPath = Paths.get(hdfsWorkDir, indexName, shardId).toString();
        String destPath = Paths.get(workDir, indexName, shardId).toString();
        log.info("Build index shard [{}] for node [{}]", shardId, id);
        try {
          hdfsClient.downloadFolder(srcPath, destPath);
          log.info("Download index bundle from hdfs[{}] to local[{}]", srcPath, destPath);
          unzipBundles(destPath);
          log.info("Unzip index bundle path {}", destPath);
          String finalIndexPath = mergeIndex(destPath);
          log.info("Merge index bundle in dir[{}] ", destPath);

          String dataPath = Utils.mostFreeDir(dataPaths);
          log.info("Most free data dir is {}", dataPath);

          // 从临时目录把索引移到es的data下面
          Path from = Paths.get(finalIndexPath);
          Path to = Paths.get(dataPath, "indices", indexName);
          Files.move(from, to);
          log.info("Move index from {} to {}", from, to);
        } catch (IOException e) {
          log.error(
              "Build index bundle from hdfs[" + srcPath + "] failed", e);
        }

      }
    }

  }

  private String mergeIndex(String indexBundlePath) throws IOException {
    Path path = Paths.get(indexBundlePath);

    log.info("start merge index for shard " + path.getFileName());
    List<Path> indexList = Files.list(path).filter(p -> Files.isDirectory(p))
        .collect(Collectors.toList());
    Collections.sort(indexList);
    try (
        FSDirectory directory = FSDirectory.open(indexList.get(0).resolve("index"));
        IndexWriter writer = new IndexWriter(directory,
            new IndexWriterConfig(null)
                .setOpenMode(IndexWriterConfig.OpenMode.APPEND))) {
      Directory[] indexes = new Directory[indexList.size() - 1];
      for (int i = 1; i < indexList.size(); i++) {
        indexes[i - 1] = FSDirectory.open(indexList.get(i).resolve("index"));
      }
      writer.addIndexes(indexes);
      writer.flush();
      writer.forceMerge(1);
      log.info("merge index for shard " + path.getFileName() + " done");
    } catch (IOException e) {
      log.error("Merge index for shard " + path.getFileName() + " error", e);
    }
    return indexList.get(0).toString();
  }

  public void unzipBundles(String indexBundlePath) throws IOException {
    Path zipPath = Paths.get(indexBundlePath);
    Files.list(zipPath).parallel().forEach(path -> {
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
