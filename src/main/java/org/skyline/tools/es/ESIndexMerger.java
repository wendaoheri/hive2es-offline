package org.skyline.tools.es;

import static org.skyline.tools.es.CompressionUtils.unzip;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;
import org.apache.commons.io.FileUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.FSDirectory;

/**
 * @author Sean Liu
 * @date 2019-09-25
 */

public class ESIndexMerger {

  private static final Log log = LogFactory.getLog(ESIndexMerger.class);

  public static void unzipBundles(Path indexBundlePath) throws IOException {
    Files.walk(indexBundlePath, 2).filter(p -> p.toString().endsWith(".zip")).parallel()
        .forEach(path -> {
          try {
            log.info("unzip index bundle : " + path.toString());
            unzip(path, path.getParent());
            FileUtils.forceDelete(path.toFile());
            log.info("delete index bundle file : " + path.toString());
          } catch (IOException e) {
            log.error("unzip index bundle failed : " + path.toString());
          }
        });
  }


  public static void merge(Path indexBundlePath) throws IOException {
    unzipBundles(indexBundlePath);
    Files.list(indexBundlePath).filter(p -> !p.getFileName().toString().startsWith("_")).parallel()
        .forEach(path -> {
          log.info("start merge shard " + path);
          try {

            List<Path> indexList = Files.list(path).collect(Collectors.toList());
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
              writer.close();
              directory.close();

              // delete merged index folder and move kept index to parent folder

              for (int i = 1; i < indexList.size(); i++) {
                indexes[i - 1].close();
                FileUtils.deleteDirectory(indexList.get(i).toFile());
              }
              Files.list(indexList.get(0)).forEach(i -> {
                try {
                  FileUtils
                      .moveToDirectory(i.toFile(), indexList.get(0).getParent().toFile(), false);
                } catch (IOException e) {
                  e.printStackTrace();
                }
              });
              FileUtils.deleteDirectory(indexList.get(0).toFile());
            } catch (IOException e) {
              e.printStackTrace();
            }
            log.info("end merge shard " + path);
          } catch (IOException e) {
            e.printStackTrace();
          }
        });
    // keep first index state file
    Path stateFolder = indexBundlePath.resolve("_state");
    Path state = Files.walk(stateFolder).filter(p -> !Files.isDirectory(p)).sorted().findFirst().get();
    String stateName = state.getFileName().toString();
    FileUtils.moveToDirectory(state.toFile(), indexBundlePath.toFile(), false);
    state = indexBundlePath.resolve(stateName);
    FileUtils.deleteDirectory(stateFolder.toFile());
    FileUtils.moveToDirectory(state.toFile(), stateFolder.toFile(), true);

  }

  public static void main(String[] args) throws IOException {
    Path indexBundlePath = Paths.get("/Users/sean/data/es/hdfs/test2");
    merge(indexBundlePath);
  }

}
