package org.skyline.tools.es;

import org.apache.commons.compress.archivers.ArchiveEntry;
import org.apache.commons.compress.archivers.zip.Zip64Mode;
import org.apache.commons.compress.archivers.zip.ZipArchiveEntry;
import org.apache.commons.compress.archivers.zip.ZipArchiveInputStream;
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

  public static void unzip(Path src, Path dest) throws IOException {
    try (
        ZipArchiveInputStream in = new ZipArchiveInputStream(
            new BufferedInputStream(Files.newInputStream(src)))
    ) {
      ArchiveEntry entry;
      while ((entry = in.getNextEntry()) != null) {
        if (!in.canReadEntryData(entry)) {
          // log something?
          continue;
        }

        File f = dest.resolve(entry.getName()).toFile();
        if (entry.isDirectory()) {
          if (!f.isDirectory() && !f.mkdirs()) {
            throw new IOException("failed to create directory " + f);
          }
        } else {
          File parent = f.getParentFile();
          if (!parent.isDirectory() && !parent.mkdirs()) {
            throw new IOException("failed to create directory " + parent);
          }
          try (OutputStream o = Files.newOutputStream(f.toPath())) {
            IOUtils.copy(in, o);
          }
        }
      }
    }
  }
}
