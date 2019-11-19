package org.skyline.tools.es;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.List;
import java.util.stream.Collectors;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.node.Node;
import org.elasticsearch.node.NodeBuilder;

/**
 * @author Sean Liu
 * @date 2019-11-06
 */
public class ESNodeTest {

  public static void main(String[] args) throws InterruptedException, IOException {
//    Settings settings = Settings
//        .settingsBuilder()
//        .put("http.enabled", true)
//        .put("node.name", "node1")
//        .put("path.home", "/Users/sean/data/es")
//        .put("path.data", "/Users/sean/data/es")
//        .put("Dlog4j2.enable.threadlocals", false)
//        .put("cluster.routing.allocation.disk.threshold_enabled", false)
//        .putArray("discovery.zen.ping.unicast.hosts")
//        .build();
//    Node node = NodeBuilder.nodeBuilder()
//        .client(false)
//        .local(false)
//        .data(true)
//        .clusterName("test")
//        .settings(settings)
//        .build()
//        .start();
//
//    while (true){
//      Thread.sleep(1000);
//    }

    List<Path> paths = Files.list(Paths.get("/Users/sean/data/es/test/0/elasticsearch_0/nodes"))
        .collect(Collectors.toList());
    System.out.println(paths);

  }
}
