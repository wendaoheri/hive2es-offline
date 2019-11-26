package org.skyline.tools.es.server;

import java.net.UnknownHostException;
import lombok.extern.slf4j.Slf4j;
import org.elasticsearch.action.admin.cluster.node.info.NodeInfo;
import org.elasticsearch.action.admin.cluster.node.info.NodesInfoResponse;
import org.elasticsearch.client.transport.TransportClient;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

@Component
@Slf4j
public class ESClient {

  @Autowired
  private TransportClient client;


  /**
   * 获取当前主机上运行的es node数，一台机器上可能会运行多个es 实例
   */
  public int getNodeOnHostNumber() throws UnknownHostException {
    NodesInfoResponse response = client.admin().cluster().prepareNodesInfo().get();
    int count = 0;
    for (NodeInfo info : response.getNodes()) {
      if (info.getHostname().equalsIgnoreCase(Utils.getHostName())) {
        count++;
      }
    }

    return count;
  }
}
