package org.skyline.tools.es.server;

import com.google.common.collect.Lists;
import java.io.File;
import java.io.IOException;
import java.util.List;
import java.util.concurrent.Future;
import javax.annotation.PostConstruct;
import lombok.extern.slf4j.Slf4j;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.fs.LocatedFileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.RemoteIterator;
import org.springframework.scheduling.annotation.Async;
import org.springframework.stereotype.Component;

/**
 * @author Sean Liu
 * @date 2019-11-26
 */
@Component
@Slf4j
public class HdfsClient {

  private FileSystem fs;

  @PostConstruct
  public void init() {
    try {
      fs = FileSystem.get(getConfiguration());
    } catch (IOException e) {
      log.error("Create hadoop FileSystem failed", e);
    }
  }

  private Configuration getConfiguration() {
    Configuration conf = new Configuration();
    return conf;
  }

  public List<String> listFiles(String path) {
    List<String> results = Lists.newArrayList();
    try {
      Path[] children = FileUtil.stat2Paths(fs.listStatus(new Path(path)));
      for (Path child : children) {
        results.add(child.toString());
      }
    } catch (IOException e) {
      log.error("List folder error", e);
    }
    return results;
  }

  public void downloadFolder(String srcPath, String dstPath) throws IOException {
    File dstDir = new File(dstPath);
    if (!dstDir.exists()) {
      dstDir.mkdirs();
    }
    FileStatus[] srcFileStatus = fs.listStatus(new Path(srcPath));
    Path[] srcFilePath = FileUtil.stat2Paths(srcFileStatus);
    for (int i = 0; i < srcFilePath.length; i++) {
      String srcFile = srcFilePath[i].toString();
      int fileNamePosi = srcFile.lastIndexOf('/');
      String fileName = srcFile.substring(fileNamePosi + 1);
      download(srcPath + '/' + fileName, dstPath + '/' + fileName);
    }
  }

  public void download(String srcPath, String dstPath) throws IOException {
    if (fs.isFile(new Path(srcPath))) {
      downloadFile(srcPath, dstPath);
    } else {
      downloadFolder(srcPath, dstPath);
    }
  }

  public void downloadFile(String srcPath, String dstPath) throws IOException {
    log.info("Download from hdfs {} to local {}", srcPath, dstPath);
    fs.copyToLocalFile(false, new Path(srcPath), new Path(dstPath), true);
  }

  public String largestFileInDirectory(String dir) throws IOException {

    RemoteIterator<LocatedFileStatus> files = fs
        .listFiles(new Path(dir), false);
    long max = 0;
    String path = "";
    while (files.hasNext()) {
      LocatedFileStatus file = files.next();
      if (file.getBlockSize() > max) {
        max = file.getBlockSize();
        path = file.getPath().toString();
      }
    }
    return path;
  }
}
