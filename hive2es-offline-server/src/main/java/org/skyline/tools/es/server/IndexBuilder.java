package org.skyline.tools.es.server;

import com.alibaba.fastjson.JSONObject;
import java.util.List;
import java.util.Map;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;

/**
 * @author Sean Liu
 * @date 2019-11-26
 */
@Slf4j
@Service
public class IndexBuilder {

  public void build(Map<String, List<String>> idToShards, JSONObject configData) {
    String hdfsWorkDir = configData.getString("hdfsWorkDir");
    String indexName = configData.getString("indexName");
    downloadIndexArchiveFromHdfs();
  }

  private void downloadIndexArchiveFromHdfs() {

  }

}
