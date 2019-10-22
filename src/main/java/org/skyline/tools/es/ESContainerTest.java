package org.skyline.tools.es;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import org.elasticsearch.action.bulk.BackoffPolicy;
import org.elasticsearch.action.bulk.BulkProcessor;
import org.elasticsearch.action.bulk.BulkProcessor.Listener;
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.ByteSizeUnit;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.node.Node;
import org.elasticsearch.node.NodeBuilder;

/**
 * @author Sean Liu
 * @date 2019-10-22
 */
public class ESContainerTest {

  private Settings settings;
  private Node node;
  private String workDir;
  private BulkProcessor processor;

  private volatile int counter;

  public ESContainerTest() {
    workDir = "/Users/sean/data/es";
    settings = Settings
        .settingsBuilder()
        .put("http.enabled", false)
        .put("node.name", "es_node")
        .put("path.home", workDir)
        .put("path.data", workDir)
        .put("Dlog4j2.enable.threadlocals", false)
        .put("cluster.routing.allocation.disk.threshold_enabled", false)
        .putArray("discovery.zen.ping.unicast.hosts")
        .build();

    node = NodeBuilder.nodeBuilder()
        .client(false)
        .local(true)
        .data(true)
        .clusterName("elasticsearch")
        .settings(settings)
        .build()
        .start();

    processor = BulkProcessor.builder(node.client(), new Listener() {
      @Override
      public void beforeBulk(long executionId, BulkRequest request) {
        System.out.println(
            "BEFORE ===> executionId : [" + executionId + "] request size : [" + request
                .numberOfActions()
                + "]");
      }

      @Override
      public void afterBulk(long executionId, BulkRequest request, BulkResponse response) {
        System.out.println(
            "AFTER ===> executionId : [" + executionId + "] request size : [" + request
                .numberOfActions()
                + "]");
        counter += request.numberOfActions();

        System.out.println(
            "TOTAL ===> executionId : [" + executionId + "] total size : [" + counter + "]");
      }

      @Override
      public void afterBulk(long executionId, BulkRequest request, Throwable failure) {

      }
    }).setBulkActions(10000)
        .setBulkSize(new ByteSizeValue(10, ByteSizeUnit.MB))
        .setFlushInterval(TimeValue.timeValueSeconds(5))
        .setConcurrentRequests(1)
        .setBackoffPolicy(BackoffPolicy.exponentialBackoff(TimeValue.timeValueSeconds(100), 3))
        .build();
  }

  public static void main(String[] args) throws InterruptedException {
    ESContainerTest container = new ESContainerTest();
    container.createIndex();
    JSONObject doc = JSON.parseObject("{\n"
        + "    \"title\" : \"title\",\n"
        + "    \"content\" : \"content\"\n"
        + "}");
    int id = 0;
    while (true) {
      id++;
      container.put("id_" + id, doc);
      Thread.sleep(5);
    }
  }

  public void createIndex() {
    node.client().admin().indices().prepareCreate("test")
        .setSettings("{\n"
            + "    \"index\": {\n"
            + "        \"number_of_replicas\": \"0\",\n"
            + "        \"refresh_interval\": \"5s\",\n"
            + "        \"number_of_shards\": \"5\",\n"
            + "        \"translog\":{\n"
            + "            \"sync_interval\":\"600s\",\n"
            + "            \"durability\":\"async\"\n"
            + "        }\n"
            + "    }\n"
            + "}")
        .get();
  }

  public void put(String id, JSONObject doc) {
    processor.add(new IndexRequest("test", "test", id).source(doc));
  }

}
