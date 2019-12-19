package org.skyline.tools.es;

import java.nio.file.Paths;
import org.apache.commons.compress.archivers.zip.Zip64Mode;
import org.apache.commons.compress.archivers.zip.ZipArchiveEntry;
import org.apache.commons.compress.archivers.zip.ZipArchiveOutputStream;
import org.apache.commons.io.IOUtils;

import java.io.*;
import java.nio.file.Files;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

/**
 * @author Sean Liu
 * @date 2019-09-25
 */

public class CompressionUtils {

  private static Log log = LogFactory.getLog(CompressionUtils.class);

  public static void zipAndUpload(String from, String to, String rootDirName, FileSystem fs)
      throws IOException {

    java.nio.file.Path fromPath = Paths.get(from);
    Path toPath = new Path(to);

    if (!fs.exists(new Path(toPath.getParent().toString()))) {
      log.info(String.format("hdfs path %s not exist and create it", toPath.getParent()));
      fs.mkdirs(toPath);
    }
    Path tmpToPath = new Path(to + "_tmp");

    try (
        FSDataOutputStream fsDataOutputStream = fs.create(tmpToPath, true, 1024 * 1024);
        ZipArchiveOutputStream out = new ZipArchiveOutputStream(fsDataOutputStream)
    ) {
      out.setUseZip64(Zip64Mode.AsNeeded);
      Files.walk(fromPath, Integer.MAX_VALUE)
          .filter(path -> !Files.isDirectory(path))
          .forEach(path -> {
            ZipArchiveEntry zipEntry = new ZipArchiveEntry(
                rootDirName + "/" + fromPath.relativize(path).toString());
            try {
              out.putArchiveEntry(zipEntry);
              IOUtils.copy(Files.newInputStream(path), out);
              out.closeArchiveEntry();
            } catch (IOException e) {
              e.printStackTrace();
            }
          });
    }
    fs.rename(tmpToPath, toPath);
  }

}
