package org.skyline.tools.es.server.config;

import lombok.extern.slf4j.Slf4j;
import org.apache.curator.framework.CuratorFramework;
import org.elasticsearch.client.transport.TransportClient;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.skyline.tools.es.server.ESClient;
import org.skyline.tools.es.server.HdfsClient;
import org.skyline.tools.es.server.IndicesChangeController;
import org.skyline.tools.es.server.LeaderSelectorController;
import org.skyline.tools.es.server.NodeService;
import org.skyline.tools.es.server.RegistryCenter;
import org.skyline.tools.es.server.TestEntry;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.mock.mockito.MockBean;
import org.springframework.scheduling.concurrent.ThreadPoolTaskExecutor;
import org.springframework.test.context.junit4.SpringRunner;

/**
 * @author Sean Liu
 * @date 2019-12-12
 */
@SpringBootTest(classes = TestEntry.class)
@RunWith(SpringRunner.class)
@Slf4j
public class ThreadPoolConfigTest {

  @MockBean
  private ESClient esClient;

  @MockBean
  private NodeService nodeService;

  @MockBean
  private RegistryCenter registryCenter;

  @MockBean
  private HdfsClient hdfsClient;

  @MockBean
  private IndicesChangeController indicesChangeController;

  @MockBean
  private LeaderSelectorController leaderSelectorController;

  @MockBean
  private CuratorFramework curatorFramework;

  @MockBean
  private TransportClient transportClient;

  @Autowired
  private ThreadPoolTaskExecutor processTaskExecutor;


  @Test
  public void test() {
    assert processTaskExecutor != null;

  }

}
