package org.skyline.tools.es.server;

import com.google.common.base.Charsets;
import java.io.IOException;
import javax.annotation.PostConstruct;
import lombok.extern.slf4j.Slf4j;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.recipes.cache.PathChildrenCache;
import org.apache.curator.framework.recipes.cache.PathChildrenCacheEvent;
import org.apache.curator.framework.recipes.cache.PathChildrenCacheEvent.Type;
import org.apache.curator.framework.recipes.cache.PathChildrenCacheListener;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Lazy;
import org.springframework.stereotype.Component;

/**
 * @author Sean Liu
 * @date 2019-12-05
 */
@Component
@Slf4j
public class IndicesChangeController implements PathChildrenCacheListener {

  private static final String INDICES_PATH = "indices";

  @Autowired
  private RegistryCenter registryCenter;

  @Autowired
  @Lazy
  private NodeService nodeService;

  private PathChildrenCache cache;


  @PostConstruct
  private void init() {
    cache = new PathChildrenCache(registryCenter.getClient(),
        registryCenter.getFullPath(INDICES_PATH), true);
    cache.getListenable().addListener(this);
  }

  public void start() throws Exception {
    log.info("Start path children cache on {}", registryCenter.getFullPath(INDICES_PATH));
    this.cache.start();
  }

  public void close() throws IOException {
    log.info("Path children cache close");
    cache.close();
  }


  @Override
  public void childEvent(CuratorFramework curatorFramework,
      PathChildrenCacheEvent event) throws Exception {
    Type eventType = event.getType();
    String data = null;
    String path = null;

    if (event.getData() != null) {
      data = new String(event.getData().getData(), Charsets.UTF_8);
      path = registryCenter.getShortPath(event.getData().getPath());
    }

    log.info("Got event type [{}] path [{}] data {} ", eventType, path, data);

    switch (event.getType()) {
      case CHILD_ADDED:
        nodeService.buildIndex(path, data);
        break;
//      case CHILD_UPDATED:
//        nodeService.markIndexComplete(path,data);
//        break;
      default:
        break;
    }
  }

}
