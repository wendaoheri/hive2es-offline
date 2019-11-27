package org.skyline.tools.es.server;

import com.google.common.collect.Lists;
import java.io.BufferedInputStream;
import java.io.File;
import java.io.IOException;
import java.io.OutputStream;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.stream.Collectors;
import org.apache.commons.compress.archivers.ArchiveEntry;
import org.apache.commons.compress.archivers.zip.ZipArchiveInputStream;
import org.apache.commons.io.IOUtils;

/**
 * @author sean
 */
public class Utils {

  public static String getHostName() throws UnknownHostException {
    InetAddress ia = InetAddress.getLocalHost();
    return ia.getHostName();
  }

  public static String getIp() throws UnknownHostException {
    InetAddress ia = InetAddress.getLocalHost();
    return ia.getHostAddress();
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

  /**
   * 获取剩余空间最大的目录
   */
  public static String mostFreeDir(String[] paths) {
    String result = paths[0];
    long mostFree = 0L;
    for (String path : paths) {
      long freeSpace = new File(path).getFreeSpace();
      if (freeSpace > mostFree) {
        mostFree = freeSpace;
        result = path;
      }
    }
    return result;
  }

}
