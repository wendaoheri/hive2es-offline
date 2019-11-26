package org.skyline.tools.es.server.config;

import java.net.InetAddress;
import java.net.UnknownHostException;
import lombok.Data;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.InetSocketTransportAddress;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.stereotype.Component;

/**
 * @author Sean Liu
 * @date 2019-11-26
 */
@Data
@Component
@ConfigurationProperties(prefix = "elasticsearch")
@Configuration
public class ESConfig {

  private String clusterNodes;
  private String clusterName;

  @Bean
  public TransportClient transportClient() throws UnknownHostException {
    Settings settings = Settings.settingsBuilder().put("cluster.name", clusterName).build();
    TransportClient client = TransportClient.builder().settings(settings).build();
    for (String addr : clusterNodes.split(",")) {
      String[] addrs = addr.split(":");
      client.addTransportAddress(
          new InetSocketTransportAddress(InetAddress.getByName(addrs[0]),
              Integer.valueOf(addrs[1])));
    }
    return client;
  }
}
