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
        } catch (IOException e) {
          log.error(
              "Download index bundle from hdfs[" + srcPath + "] to local[" + destPath + "] failed",
              e);
        }
        String dataPath = Utils.mostFreeDir(dataPaths);
        log.info("Most free data dir is {}", dataPath);
        try {
          buildIndex(destPath, dataPath);
          log.info("Build index from bundle dir[{}] to dataPath[{}]", destPath, dataPath);
        } catch (IOException e) {
          log.info(
              "Build index from bundle dir[{" + destPath + "}] to dataPath[{" + dataPath
                  + "}] failed", e);
        }
      }
    }

  }

  private void buildIndex(String destPath, String nodeDataPath) throws IOException {
    Path indexBundlePath = Paths.get(destPath);
    unzipBundles(indexBundlePath);

    Map<Path, List<Path>> indexGroup = Files.walk(indexBundlePath, 2)
        .filter(p -> !p.getFileName().toString().startsWith("_") && Files.isDirectory(p)
            && p.relativize(indexBundlePath).getNameCount() > 1)
        .collect(Collectors.groupingBy(p -> p.getFileName()));

    indexGroup.entrySet().parallelStream().forEach(entry -> {
      log.info("start merge index for shard " + entry.getKey());
      List<Path> indexList = entry.getValue();
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
        log.info("merge index for shard " + entry.getKey() + " done");
      } catch (IOException e) {
        e.printStackTrace();
      }
    });
  }

  public void unzipBundles(Path indexBundlePath) throws IOException {
    Files.list(indexBundlePath).parallel().forEach(path -> {
      try {
        log.info("unzip index bundle : " + path.toString());
        Utils.unzip(path, indexBundlePath);
        FileUtils.forceDelete(path.toFile());
        log.info("delete index bundle file : " + path.toString());
      } catch (IOException e) {
        log.error("unzip index bundle failed : " + path.toString());
      }
    });
  }


}
