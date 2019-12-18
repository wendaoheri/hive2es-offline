package org.skyline.tools.es;

import org.apache.commons.compress.archivers.zip.Zip64Mode;
import org.apache.commons.compress.archivers.zip.ZipArchiveEntry;
import org.apache.commons.compress.archivers.zip.ZipArchiveOutputStream;
import org.apache.commons.io.IOUtils;

import java.io.*;
import java.nio.file.Files;
import java.nio.file.Path;

/**
 * @author Sean Liu
 * @date 2019-09-25
 */
public class CompressionUtils {

  public static void zip(Path src, Path dest, String rootDirName) throws IOException {
    Path destParent = dest.getParent();
    if (!Files.exists(destParent)) {
      destParent.toFile().mkdirs();
    }
    try (
        ZipArchiveOutputStream out = new ZipArchiveOutputStream(
            new BufferedOutputStream(Files.newOutputStream(dest)))
    ) {
      out.setUseZip64(Zip64Mode.AsNeeded);
      Files.walk(src, Integer.MAX_VALUE)
          .filter(path -> !Files.isDirectory(path))
          .forEach(path -> {
            ZipArchiveEntry zipEntry = new ZipArchiveEntry(
                rootDirName + "/" + src.relativize(path).toString());
            try {
              out.putArchiveEntry(zipEntry);
              IOUtils.copy(Files.newInputStream(path), out);
              out.closeArchiveEntry();
            } catch (IOException e) {
              e.printStackTrace();
            }
          });
    }
  }

}
