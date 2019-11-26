package org.skyline.tools.es.server;

import com.google.common.collect.Maps;
import java.net.UnknownHostException;
import java.util.List;
import java.util.Map;
import java.util.Set;
import lombok.extern.slf4j.Slf4j;
import org.elasticsearch.action.admin.cluster.node.info.NodeInfo;
import org.elasticsearch.action.admin.cluster.node.info.NodesInfoResponse;
import org.elasticsearch.client.transport.TransportClient;
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
    Map<String, NodeInfo> nodInfos = getDataNodeInfoOnHost();
    return nodInfos.get(nodeId).getSettings().getAsArray("data.path");
  }

}
