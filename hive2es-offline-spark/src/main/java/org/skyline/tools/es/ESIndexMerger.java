package org.skyline.tools.es;

import org.apache.commons.io.FileUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.FSDirectory;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

/**
 * @author Sean Liu
 * @date 2019-09-25
 */

public class ESIndexMerger {

  private static final Log log = LogFactory.getLog(ESIndexMerger.class);

  public static void unzipBundles(Path indexBundlePath) throws IOException {
    Files.list(indexBundlePath).parallel().forEach(path -> {
      try {
        log.info("unzip index bundle : " + path.toString());
        CompressionUtils.unzip(path, indexBundlePath);
        FileUtils.forceDelete(path.toFile());
        log.info("delete index bundle file : " + path.toString());
      } catch (IOException e) {
        log.error("unzip index bundle failed : " + path.toString());
      }
    });
  }

  public static void merge(Path indexBundlePath) throws IOException {
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

  public static void main(String[] args) throws IOException {
    Path indexBundlePath = Paths.get("/Users/sean/data/es/bundles");
    merge(indexBundlePath);
  }

}
