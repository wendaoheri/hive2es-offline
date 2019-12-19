package org.skyline.tools.es.server;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import javax.annotation.PostConstruct;
import javax.annotation.PreDestroy;
import lombok.Data;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.collections.MapUtils;
import org.apache.commons.lang.ArrayUtils;
import org.apache.commons.lang.StringUtils;
import org.skyline.tools.es.server.utils.IpUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

/**
 * @author Sean Liu
 * @date 2019-12-05
 */
@Component
@Slf4j
public class NodeService {

  private static final String NODE_PATH = "nodes";
  private static final String ES_NODE_JOINER = ",";
  @Autowired
  private RegistryCenter registryCenter;

  @Getter
  private Node localNode;

  @Autowired
  private LeaderSelectorController leaderSelectorController;

  @Autowired
  private IndicesChangeController indicesChangeController;

  @Autowired
  private ESClient esClient;

  @Autowired
  private IndexBuilder indexBuilder;

  private volatile boolean started = false;
  private static final String ASSIGN_FLAG = "_assigned";

  @PostConstruct
  public void init() throws Exception {
    String nodeId = IpUtils.getId();
    localNode = new Node(nodeId);
    this.start();
  }

  public void start() throws Exception {
    if (started) {
      return;
    }
    log.info("Start node {}", localNode.getNodeId());
    this.registerNode();
    leaderSelectorController.start();
    indicesChangeController.start();
  }

  @PreDestroy
  public void close() throws IOException {
    registryCenter.close();
    leaderSelectorController.close();
    indicesChangeController.close();
  }

  private void registerNode() {
    log.info("Register node {} on path {}", localNode.getNodeId(),
        registryCenter.getFullPath(localNode.getZKPath()));
    registryCenter.persistEphemeral(localNode.getZKPath(), "");
    this.updateESNodeInfo();
  }


  public void buildIndex(String indexPath, String data) {
    new Thread(() -> {
      String indexNodePath = indexPath + "/" + localNode.getNodeId();
      // 在开始build之前先各自更新一下当前节点上运行es data node数量，防止data node掉线
      this.updateESNodeInfo();
      JSONObject configData = JSON.parseObject(data);
      if (leaderSelectorController.hasLeadership()) {
        assignShards(configData, indexPath);
        registryCenter.persistEphemeral(indexPath + "/" + ASSIGN_FLAG, "");
      }
      Map<String, List<String>> currentNodeShards = getCurrentNodeShards(indexPath, indexNodePath);

      if (MapUtils.isNotEmpty(currentNodeShards)) {
        log.info("Current node shards is : {}", currentNodeShards);
        if (MapUtils.isNotEmpty(currentNodeShards)) {
          boolean success = indexBuilder.build(currentNodeShards, configData);
          if (success) {
            registryCenter.delete(indexNodePath);
            log.info("Build index for {} complete", indexNodePath);
          }
        }
      }

      if (leaderSelectorController.hasLeadership()) {
        waitAllNodeComplete(indexPath);
        esClient.triggerClusterChange();
        String finalIndexSetting = configData.getString("finalIndexSetting");
        String indexName = configData.getString("indexName");
        if (StringUtils.isNotEmpty(finalIndexSetting)) {
          try {
            log.info("Update final index setting : {}", finalIndexSetting);
            esClient.updateIndexSetting(indexName, finalIndexSetting);
          } catch (Exception e) {
            log.error("Update final index setting failed", e);
          }
        }
        registryCenter.delete(indexPath);
        log.info("Build index for {} all complete", indexPath);
      }
    }).start();
  }

  public void markIndexComplete(String indexPath, String data) {
    JSONObject configData = JSON.parseObject(data);
    boolean completed = configData.getString("state").equalsIgnoreCase("completed");
    if (completed) {
      String indexName = configData.getString("indexName");
      indexBuilder.markIndexCompleted(indexName);
    }
  }

  private void waitAllNodeComplete(String indexPath) {
    int leftCount;
    while ((leftCount = registryCenter.getNumChildren(indexPath)) != 1) {
      try {
        Thread.sleep(1000);
        log.info("Wait all node complete, [{}] node left ,sleep 1000 ms", leftCount - 1);
      } catch (InterruptedException e) {
        log.error("Wait all node complete error", e);
      }
    }
    log.info("All node completed for indexPath : {}", indexPath);
  }

  public Map<String, String[]> getAllRegisteredNode() {
    Map<String, String[]> result = Maps.newHashMap();
    List<String> childrenPaths = registryCenter.getChildrenPaths(NODE_PATH);
    log.info("All registered node is {}", childrenPaths);
    for (String path : childrenPaths) {
      String esNodesStr = registryCenter.getValue(NODE_PATH + "/" + path);
      String[] esNodes = esNodesStr.split(ES_NODE_JOINER);
      log.info("Node to ES node is {} : {}", path, Lists.newArrayList(esNodes));
      if (ArrayUtils.isNotEmpty(esNodes) && StringUtils.isNotEmpty(esNodesStr)) {
        result.put(path, esNodes);
      }
    }
    return result;
  }

  private void assignShards(JSONObject configData, String indexPath) {
    log.info("Start assign shard");
    Map<String, String[]> allNodes = this.getAllRegisteredNode();
    List<String> ids = allNodes.values().stream()
        .flatMap(x -> Lists.newArrayList(x).stream())
        .filter(x -> StringUtils.isNotEmpty(x))
        .collect(Collectors.toList());
    log.info("ES node id sequence is : {}", ids);
    // nodeId 0: shard0 shard1 shard2
    // nodeId 1: shard3 shard 4 shard5
    List<Integer>[] result = new ArrayList[ids.size()];

    // 按照shardId对dataNode数量进行取余，余数是多少就分配给对应的dataNode
    Integer numberShards = configData.getInteger("numberShards");
    for (int i = 0; i < numberShards; i++) {
      int mod = i % ids.size();
      List<Integer> shards = result[mod];
      if (shards == null) {
        shards = Lists.newArrayList();
        result[mod] = shards;
      }
      shards.add(i);

    }
    log.info("Shard result is : {}", JSON.toJSONString(result));

    // 将每一个host对应的node和shardID写到zk
    allNodes.entrySet().forEach(x -> {
      String nodeId = x.getKey();
      Map<String, List<Integer>> idToShards = Maps.newHashMap();
      for (String id : x.getValue()) {
        List<Integer> shards = result[ids.indexOf(id)];
        if (shards != null && !shards.isEmpty()) {
          idToShards.put(id, shards);
        }
      }
      if (MapUtils.isNotEmpty(idToShards)) {
        String idToShardsJSON = JSON.toJSONString(idToShards);
        registryCenter.persistEphemeral(indexPath + "/" + nodeId, idToShardsJSON);
        log.info("Assign shards {} to host [{}]", idToShardsJSON, nodeId);
      }
    });
    log.info("End assign shard");
  }

  private Map<String, List<String>> getCurrentNodeShards(String indexPath, String indexNodePath) {
    Map<String, List<String>> result = Maps.newHashMap();
    while (true) {
      if (registryCenter.isExisted(indexPath + "/" + ASSIGN_FLAG)) {
        break;
      }
      try {
        Thread.sleep(1000);
        log.info("Wait shard assign and sleep 1000 ms");
      } catch (InterruptedException e) {
        log.error("Wait shard assign error", e);
      }
    }
    if (registryCenter.isExisted(indexNodePath)) {
      JSONObject data = JSON.parseObject(registryCenter.getValue(indexNodePath));
      data.keySet().forEach(x -> result.put(x, data.getJSONArray(x).toJavaList(String.class)));
    }
    return result;
  }

  public void updateESNodeInfo() {
    Set<String> nodeNames = esClient.getNodeNameOnHost();
    String nodes = String.join(ES_NODE_JOINER, nodeNames);
    log.info("Update local es node info {}", nodes);
    registryCenter.update(localNode.getZKPath(), nodes);
  }

  @Data
  public static class Node {

    public Node(String nodeId) {
      this.nodeId = nodeId;
    }


    private String nodeId;

    public String getZKPath() {
      return NODE_PATH + "/" + nodeId;
    }

  }

}
