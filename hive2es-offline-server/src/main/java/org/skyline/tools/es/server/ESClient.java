package org.skyline.tools.es.server;

import com.google.common.collect.Maps;
import com.google.common.io.Files;
import java.io.File;
import java.io.IOException;
import java.net.UnknownHostException;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.compress.utils.Lists;
import org.apache.commons.io.FileUtils;
import org.elasticsearch.action.admin.cluster.node.info.NodeInfo;
import org.elasticsearch.action.admin.cluster.node.info.NodesInfoResponse;
import org.elasticsearch.action.admin.cluster.node.stats.NodesStatsResponse;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.monitor.fs.FsInfo.Path;
import org.elasticsearch.node.Node;
import org.elasticsearch.node.NodeBuilder;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

/**
 * @author sean
 */
@Component
@Slf4j
public class ESClient {

  @Autowired
  private TransportClient client;

  public Set<String> getNodeNameOnHost() {
    return this.getDataNodeInfoOnHost().keySet();
  }

  public Map<String, NodeInfo> getDataNodeInfoOnHost() {
    Map<String, NodeInfo> result = Maps.newHashMap();
    NodesInfoResponse response = client.admin().cluster().prepareNodesInfo().get();
    for (NodeInfo info : response.getNodes()) {
      boolean currentHostDataNode = isCurrentHostDataNode(info);
      if (currentHostDataNode) {

        result.put(info.getNode().getId(), info);
      }
    }
    return result;
  }

  private boolean isCurrentHostDataNode(NodeInfo info) {
    try {
      return info.getNode().isDataNode() && (
          info.getHostname().equalsIgnoreCase(Utils.getHostName())
              || info.getNode().getHostAddress().equalsIgnoreCase(Utils.getIp()));
    } catch (UnknownHostException e) {
      log.error("Check host error", e);
    }
    return false;
  }

  public String[] getDataPathByNodeId(String nodeId) {
    NodesStatsResponse resp = client.admin().cluster().prepareNodesStats(nodeId).setFs(true)
        .get();
    List<String> result = Lists.newArrayList();
    if (resp.getNodes().length > 0) {
      Iterator<Path> it = resp.getNodes()[0].getFs().iterator();
      while (it.hasNext()) {
        result.add(it.next().getPath());
      }

    }
    return result.toArray(new String[result.size()]);
  }

  /**
   * es会将直接复制进来的索引文件视为Dangling index，且不能自动发现
   * 这里模拟一个es node加入到集群，触发集群的cluster_state update
   * 也许有更好的API可以使用
   * @return
   */
  public boolean triggerDanglingIndexProcess() {
    File homeDir = Files.createTempDir();
    log.info("Fake node home dir is {}", homeDir.toString());
    Set<String> allHostSet = client.listedNodes()
        .stream()
        .map(x -> x.getAddress().getHost() + ":" + x.getAddress().getPort())
        .collect(Collectors.toSet());

    // 这里一定要将启动的节点设置成 NOT master NOT data node，不然有可能引起索引重分片或者master迁移
    Settings settings = Settings.builder()
        .put("cluster.name", client.settings().get("cluster.name"))
        .put("node.master", false)
        .put("http.enabled", false)
        .put("path.home", homeDir.toString())
        .putArray("discovery.zen.ping.unicast.hosts",
            allHostSet.toArray(new String[allHostSet.size()]))
        .build();
    Node node = NodeBuilder.nodeBuilder()
        .client(false)
        .local(false)
        .data(false)
        .settings(settings)
        .build();
    try {
      log.info("Fake node start...");
      node.start();
    } finally {
      node.close();
      log.info("Fake node stop...");
      try {
        FileUtils.deleteDirectory(homeDir);
        log.info("Delete fake node home dir {}", homeDir.toString());
      } catch (Exception e) {
        log.error("Delete fake node home dir error", e);
      }
    }

    return true;
  }

}
