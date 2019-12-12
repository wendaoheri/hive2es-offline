package org.skyline.tools.es.server;

import com.google.common.collect.Lists;
import java.io.File;
import java.io.IOException;
import java.util.List;
import java.util.concurrent.ExecutionException;
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
import org.springframework.scheduling.annotation.AsyncResult;
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

  public void downloadFolder(String srcPath, String dstPath) throws IOException {
    File dstDir = new File(dstPath);
    if (!dstDir.exists()) {
      dstDir.mkdirs();
    }
    FileStatus[] srcFileStatus = fs.listStatus(new Path(srcPath));
    Path[] srcFilePath = FileUtil.stat2Paths(srcFileStatus);
    List<Future<Boolean>> futures = Lists.newArrayList();
    for (int i = 0; i < srcFilePath.length; i++) {
      String srcFile = srcFilePath[i].toString();
      int fileNamePosi = srcFile.lastIndexOf('/');
      String fileName = srcFile.substring(fileNamePosi + 1);
      futures.add(downloadFile(srcPath + '/' + fileName, dstPath + '/' + fileName));
    }
    futures.forEach(x -> {
      try {
        x.get();
      } catch (InterruptedException | ExecutionException e) {
        log.error("Wait download error", e);
      }
    });
  }


  @Async("downloadTaskExecutor")
  public Future<Boolean> downloadFile(String srcPath, String dstPath) throws IOException {
    log.info("Download from hdfs {} to local {}", srcPath, dstPath);
    fs.copyToLocalFile(false, new Path(srcPath), new Path(dstPath), true);
    return new AsyncResult(true);
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
