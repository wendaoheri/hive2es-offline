package org.skyline.tools.es.server;

import javax.annotation.PostConstruct;
import lombok.extern.slf4j.Slf4j;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.recipes.cache.PathChildrenCache;
import org.apache.curator.framework.recipes.cache.PathChildrenCacheEvent.Type;
import org.apache.zookeeper.CreateMode;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

@Component
@Slf4j
public class ESNodeCompanionService {

  @Autowired
  private CuratorFramework client;
  @Autowired
  private IndexHandler indexHandler;
  @Autowired
  private ESClient esClient;

  public static final String BASE_PATH = "/es_offline";
  public static final String INDICES_PATH = BASE_PATH + "/indices";
  public static final String NODE_PATH = BASE_PATH + "/nodes";

  @PostConstruct
  public void init() {
    try {
      String hostName = Utils.getHostName();
      String nodePath = NODE_PATH + "/" + hostName;
      int nodeOnHostNumber = esClient.getNodeOnHostNumber();

      client.create().creatingParentsIfNeeded().withMode(CreateMode.EPHEMERAL)
          .forPath(nodePath, String.valueOf(nodeOnHostNumber).getBytes());
      log.info("Register node {} on path {}", hostName, nodePath);

      registerIndicesListener();
    } catch (Exception e) {
      log.error("Register node failed", e);
    }
  }

  public void registerIndicesListener() throws Exception {
    PathChildrenCache cache = new PathChildrenCache(client, INDICES_PATH, false);
    cache.getListenable().addListener((c, event) -> {
      Type eventType = event.getType();
      String path = event.getData() == null ? null : event.getData().getPath();
      log.info("Got event type [{}] path [{}] ", eventType, path);

    });
    cache.start();
    log.info("Register indices listener");
  }


}
