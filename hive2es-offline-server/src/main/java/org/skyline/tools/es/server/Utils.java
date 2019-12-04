package org.skyline.tools.es.server;

import com.fasterxml.jackson.annotation.ObjectIdGenerators.UUIDGenerator;
import java.io.BufferedInputStream;
import java.io.File;
import java.io.IOException;
import java.io.OutputStream;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.nio.file.FileStore;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.attribute.PosixFilePermission;
import java.nio.file.attribute.PosixFilePermissions;
import java.util.Set;
import java.util.UUID;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.compress.archivers.ArchiveEntry;
import org.apache.commons.compress.archivers.zip.ZipArchiveInputStream;
import org.apache.commons.io.IOUtils;

/**
 * @author sean
 */
@Slf4j
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

  /**
   * 从dirs中选择一个和referenceDir在同一块磁盘的目录 如果没有在同一块磁盘上的则选择剩余空间最大的
   */
  public static String sameDiskDir(String[] dirs, String referenceDir) {
    try {
      String refFileStore = getFileStore(referenceDir);
      for (String dir : dirs) {
        String fileStore = getFileStore(dir);
        if (fileStore.equalsIgnoreCase(refFileStore)) {
          return dir;
        }
      }
    } catch (IOException e) {
      log.error("Get file store error", e);
    }
    log.warn("No same disk, use most free disk");
    return mostFreeDir(dirs);
  }

  /**
   * 获取path所在的mountPoint
   */
  public static String mountPoint(String path) throws IOException {
    FileStore store = Files.getFileStore(Paths.get(path));
    return getMountPointLinux(store);
  }

  public static String getFileStore(String path) throws IOException {
    return Files.getFileStore(Paths.get(path)).name();
  }

  private static String getMountPointLinux(FileStore store) {
    String desc = store.toString();
    int index = desc.lastIndexOf(" (");
    if (index != -1) {
      return desc.substring(0, index);
    } else {
      return desc;
    }
  }

  public static void setPermissionRecursive(Path path) throws IOException {
    Set<PosixFilePermission> perms = PosixFilePermissions.fromString("rwxrwxrwx");
    Files.walk(path)
        .forEach(p -> {
          try {
            Files.setPosixFilePermissions(p, perms);
          } catch (IOException e) {
            log.error("Set permission error", e);
          }
        });
    log.info("Set permission  recursive to path {}", path);
  }

  public static void setPermissions(Path path) throws IOException {
    Set<PosixFilePermission> perms = PosixFilePermissions.fromString("rwxrwxrwx");
    Files.setPosixFilePermissions(path, perms);

  }

  public static String getId() {
    String result;
    try {
      result = getHostName();
    } catch (UnknownHostException e) {
      try {
        result = getIp();
      } catch (UnknownHostException ex) {
        result = UUID.randomUUID().toString();
      }
    }
    return result;
  }
}
