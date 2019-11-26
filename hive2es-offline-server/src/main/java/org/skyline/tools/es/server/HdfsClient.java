package org.skyline.tools.es.server;

import java.io.IOException;
import javax.annotation.PostConstruct;
import lombok.extern.slf4j.Slf4j;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
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
      log.error("Create hadoop file system failed", e);
    }
  }

  private Configuration getConfiguration() {
    Configuration conf = new Configuration();
    return conf;
  }

  public void download(){
  }
}
