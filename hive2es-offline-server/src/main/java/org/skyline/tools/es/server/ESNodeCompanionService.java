package org.skyline.tools.es.server;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.stream.Collectors;
import javax.annotation.PostConstruct;
import lombok.extern.slf4j.Slf4j;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.recipes.cache.PathChildrenCache;
import org.apache.curator.framework.recipes.cache.PathChildrenCacheEvent;
import org.apache.curator.framework.recipes.cache.PathChildrenCacheEvent.Type;
import org.apache.curator.framework.recipes.leader.LeaderSelector;
import org.apache.curator.framework.recipes.leader.LeaderSelectorListenerAdapter;
import org.apache.zookeeper.CreateMode;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

/**
 * @author sean
 */
@Component
@Slf4j
public class ESNodeCompanionService extends LeaderSelectorListenerAdapter {

  @Autowired
  private CuratorFramework client;
  @Autowired
  private ESClient esClient;
  @Autowired
  private IndexBuilder indexBuilder;

  private LeaderSelector leaderSelector;
  private CountDownLatch latch = new CountDownLatch(1);

  public static final String BASE_PATH = "/es_offline";
  public static final String INDICES_PATH = BASE_PATH + "/indices";
  public static final String NODE_PATH = BASE_PATH + "/nodes";
  public static final String LEADER_PATH = BASE_PATH + "/leader";

  @PostConstruct
  public void init() {
    try {

      String nodePath = currentNodePath();

      if (!isExisted(nodePath)) {
        client.create().creatingParentsIfNeeded().withMode(CreateMode.EPHEMERAL)
            .forPath(nodePath);
        log.info("Node path not exists and create it {}", nodePath);
      }
      updateESNodeInfo();
      log.info("Register node {} on path {}", Utils.getHostName(), nodePath);

      registerIndicesListener();

      leaderSelector = new LeaderSelector(client, LEADER_PATH, this);
      leaderSelector.autoRequeue();
      leaderSelector.start();
    } catch (Exception e) {
      log.error("Register node failed", e);
    }
  }

  public void registerIndicesListener() throws Exception {
    PathChildrenCache cache = new PathChildrenCache(client, INDICES_PATH, true);
    cache.getListenable().addListener((c, event) -> handle(event));
    cache.start();
    log.info("Register indices listener");
  }

  private void handle(PathChildrenCacheEvent event) {
    Type eventType = event.getType();
    String path = event.getData() == null ? null : event.getData().getPath();
    String data = event.getData() != null && event.getData().getData() != null ? new String(
        event.getData().getData()) : null;
    log.info("Got event type [{}] path [{}] data {} ", eventType, path, data);
    switch (event.getType()) {
      case CHILD_ADDED:
        buildIndex(path, data);
        break;
      default:
        break;
    }
  }

  private void buildIndex(String path, String data) {
    // 在开始build之前先各自更新一下当前节点上运行es data node数量，防止datanode掉线
    updateESNodeInfo();
    JSONObject configData = JSON.parseObject(data);
    if (leaderSelector.hasLeadership()) {
      Map<String, String[]> allNodes = this.getAllNodes();
      assignShards(allNodes, configData, path);
    }
    Map<String, List<String>> currentNodeShards = getCurrentNodeShards(path);
    indexBuilder.build(currentNodeShards, configData);
    try {
      log.info("Delete index path : {}", path);
      client.delete().forPath(path);
    } catch (Exception e) {
      log.error("Delete path error", e);
    }
  }

  private void assignShards(Map<String, String[]> allNodes, JSONObject configData,
      String indexZKPath) {

    // 建立nodeId -> host之间的映射
    Map<String, String> nodeToHostMap = Maps.newHashMap();
    allNodes.entrySet().forEach(x -> {
      for (String nodeId : x.getValue()) {
        nodeToHostMap.put(nodeId, x.getKey());
      }
    });

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
      String host = x.getKey();
      Map<String, List<Integer>> idToShards = Maps.newHashMap();
      for (String id : x.getValue()) {
        idToShards.put(id, result[ids.indexOf(id)]);
      }
      String idToShardsJSON = JSON.toJSONString(idToShards);
      try {
        client.create().creatingParentsIfNeeded().withMode(CreateMode.EPHEMERAL)
            .forPath(indexZKPath + "/" + host, idToShardsJSON.getBytes());
        log.info("Assign shards {} to host [{}]", idToShardsJSON, host);
      } catch (Exception e) {
        log.error("Assign shard to host [{" + host + "}] error", e);
      }
    });

  }

  private Map<String, List<String>> getCurrentNodeShards(String path) {
    Map<String, List<String>> result = Maps.newHashMap();
    try {
      String hostPath = path + "/" + Utils.getHostName();
      while (true) {
        if (isExisted(hostPath)) {
          break;
        }
        Thread.sleep(100);
      }
      JSONObject data = JSON
          .parseObject(new String(client.getData().forPath(hostPath)));
      data.keySet().forEach(x -> result.put(x, data.getJSONArray(x).toJavaList(String.class))
      );
    } catch (Exception e) {
      log.info("Get current node shards failed", e);
    }
    return result;
  }

  private boolean isExisted(String path) {
    try {
      return null != client.checkExists().forPath(path);
    } catch (Exception e) {
      log.error("Check path [" + path + "] exists failed", e);
    }
    return false;
  }

  public String currentNodePath() throws UnknownHostException {
    String hostName = Utils.getHostName();
    String nodePath = NODE_PATH + "/" + hostName;
    return nodePath;
  }

  public void updateESNodeInfo() {

    try {
      String nodePath = currentNodePath();
      Set<String> nodeNames = esClient.getNodeNameOnHost();
      client.setData().forPath(nodePath, String.join(",", nodeNames).getBytes());
    } catch (Exception e) {
      log.error("Update es node number failed", e);
    }
  }


  public Map<String, String[]> getAllNodes() {
    Map<String, String[]> result = Maps.newHashMap();
    try {
      client.getChildren().forPath(NODE_PATH).forEach(x -> {
        try {
          String[] nodes = new String(client.getData().forPath(NODE_PATH + "/" + x)).split(",");
          result.put(x, nodes);
          log.info("Host [{}] has active es nodes {}", x, nodes);
        } catch (Exception e) {
          log.info("Get data error for path " + x, e);
        }
      });
    } catch (Exception e) {
      log.error("Get all nodes error", e);
    }
    return result;
  }

  @Override
  public void takeLeadership(CuratorFramework curatorFramework) throws Exception {
    log.info("take leader ship");
    // 成了leader之后就别放了
    latch.await();
  }
}
