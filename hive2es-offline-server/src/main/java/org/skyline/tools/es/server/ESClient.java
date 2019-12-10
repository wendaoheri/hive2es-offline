package org.skyline.tools.es.server;

import com.google.common.collect.Maps;
import java.net.SocketException;
import java.net.UnknownHostException;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.compress.utils.Lists;
import org.elasticsearch.action.admin.cluster.node.info.NodeInfo;
import org.elasticsearch.action.admin.cluster.node.info.NodesInfoResponse;
import org.elasticsearch.action.admin.cluster.node.stats.NodesStatsResponse;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.monitor.fs.FsInfo.Path;
import org.skyline.tools.es.server.utils.IpUtils;
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
          info.getHostname().equalsIgnoreCase(IpUtils.getHostName())
              || info.getNode().getHostAddress().equalsIgnoreCase(IpUtils.getIp()));
    } catch (UnknownHostException | SocketException e) {
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


  public void triggerClusterChange() {
    client.admin().cluster().prepareUpdateSettings()
        .setTransientSettings(Settings.builder().put("logger._root", "INFO").build()).get();
  }

}
