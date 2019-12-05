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
  }

  public void buildIndex(String indexPath, String data) {

    String indexNodePath = indexPath + "/" + localNode.getNodeId();
    // 在开始build之前先各自更新一下当前节点上运行es data node数量，防止data node掉线
    this.updateESNodeInfo();
    JSONObject configData = JSON.parseObject(data);
    if (leaderSelectorController.hasLeadership()) {
      assignShards(configData, indexPath);
    }
    Map<String, List<String>> currentNodeShards = getCurrentNodeShards(indexNodePath);
    boolean success = indexBuilder.build(currentNodeShards, configData);
    if (success) {
      registryCenter.delete(indexNodePath);
      log.info("Build index for {} complete", indexNodePath);

      if (leaderSelectorController.hasLeadership()) {
        waitAllNodeComplete(indexPath);
        esClient.triggerDanglingIndexProcess();
        registryCenter.delete(indexPath);
        log.info("Build index for {} all complete", indexPath);
      }
    }
  }

  private void waitAllNodeComplete(String indexPath) {
    while (registryCenter.getNumChildren(indexPath) != 0) {
      try {
        Thread.sleep(1000);
      } catch (InterruptedException e) {
        log.error("Wait all node complete error", e);
      }
    }
    log.info("All node completed for indexPath : {}", indexPath);
  }

  public Map<String, String[]> getAllRegisteredNode() {
    Map<String, String[]> result = Maps.newHashMap();
    List<String> childrenPaths = registryCenter.getChildrenPaths(NODE_PATH);
    for (String path : childrenPaths) {
      String[] esNodes = registryCenter.getValue(path).split(ES_NODE_JOINER);
      result.put(path, esNodes);
    }
    return result;
  }

  private void assignShards(JSONObject configData, String indexPath) {
    Map<String, String[]> allNodes = this.getAllRegisteredNode();
    List<String> ids = allNodes.values().stream().flatMap(x -> Lists.newArrayList(x).stream())
        .collect(Collectors.toList());
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

    // 将每一个host对应的node和shardID写到zk
    allNodes.entrySet().forEach(x -> {
      String nodeId = x.getKey();
      Map<String, List<Integer>> idToShards = Maps.newHashMap();
      for (String id : x.getValue()) {
        idToShards.put(id, result[ids.indexOf(id)]);
      }
      String idToShardsJSON = JSON.toJSONString(idToShards);
      registryCenter.persistEphemeral(indexPath + "/" + nodeId, idToShardsJSON);
      log.info("Assign shards {} to host [{}]", idToShardsJSON, nodeId);
    });

  }

  private Map<String, List<String>> getCurrentNodeShards(String indexNodePath) {
    Map<String, List<String>> result = Maps.newHashMap();
    while (true) {
      if (registryCenter.isExisted(indexNodePath)) {
        break;
      }
      try {
        Thread.sleep(1000);
      } catch (InterruptedException e) {
        log.error("Wait shard assign error", e);
      }
    }
    JSONObject data = JSON.parseObject(registryCenter.getValue(indexNodePath));
    data.keySet().forEach(x -> result.put(x, data.getJSONArray(x).toJavaList(String.class)));
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
