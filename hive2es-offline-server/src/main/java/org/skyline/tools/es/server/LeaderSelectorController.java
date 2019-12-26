package org.skyline.tools.es.server;

import java.util.concurrent.CountDownLatch;
import javax.annotation.PostConstruct;
import lombok.extern.slf4j.Slf4j;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.recipes.leader.LeaderSelector;
import org.apache.curator.framework.recipes.leader.LeaderSelectorListenerAdapter;
import org.apache.curator.framework.state.ConnectionState;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Lazy;
import org.springframework.stereotype.Component;

/**
 * @author Sean Liu
 * @date 2019-12-05
 */
@Component
@Slf4j
public class LeaderSelectorController extends LeaderSelectorListenerAdapter {

  private CountDownLatch latch = new CountDownLatch(1);

  @Autowired
  private RegistryCenter registryCenter;

  @Autowired
  @Lazy
  private NodeService nodeService;

  private String LEADER_PATH = "leader";

  private LeaderSelector leaderSelector;

  @PostConstruct
  public void init() {
    leaderSelector = new LeaderSelector(registryCenter.getClient(),
        registryCenter.getFullPath(LEADER_PATH), this);
    leaderSelector.autoRequeue();
  }

  @Override
  public void takeLeadership(CuratorFramework curatorFramework) throws Exception {
    String nodeId = nodeService.getLocalNode().getNodeId();
    log.info("Take leader ship and set leader node id to {}", nodeId);
    registryCenter.persist(LEADER_PATH, nodeId);
    latch.await();
    log.info("Node {} is not leader now", nodeId);
  }

  @Override
  public void stateChanged(CuratorFramework client, ConnectionState newState) {
    if (client.getConnectionStateErrorPolicy().isErrorState(newState)) {
      this.close();
    } else if (newState == ConnectionState.RECONNECTED) {
      this.start();
    }
    super.stateChanged(client, newState);
  }

  public boolean hasLeadership() {
    return leaderSelector.hasLeadership();
  }

  public void start() {
    log.info("Start leader election");
    leaderSelector.start();
  }

  public void close() {
    log.info("Stop leader election");
    if (hasLeadership()) {
      log.info("Clean leader id : {}", nodeService.getLocalNode().getNodeId());
      registryCenter.update(LEADER_PATH, "");
    }
    leaderSelector.close();
  }
}
