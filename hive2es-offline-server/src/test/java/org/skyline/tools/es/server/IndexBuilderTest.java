package org.skyline.tools.es.server;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import java.util.List;
import java.util.Map;
import lombok.extern.slf4j.Slf4j;
import org.apache.curator.framework.CuratorFramework;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.mock.mockito.MockBean;
import org.springframework.test.context.junit4.SpringRunner;

/**
 * @author Sean Liu
 * @date 2019-12-12
 */
@SpringBootTest(classes = TestEntry.class)
@RunWith(SpringRunner.class)
@Slf4j
public class IndexBuilderTest {

  @MockBean
  private HdfsClient hdfsClient;

  @MockBean
  private CuratorFramework curatorFramework;

  @MockBean
  private NodeService nodeService;

  @Autowired
  private ESClient esClient;

  @Autowired
  private IndexBuilder indexBuilder;

  private static Map<String, List<String>> idToShards;
  private static JSONObject configData;

  @BeforeClass
  public static void beforeClass() {
    idToShards = Maps.newHashMap();
    idToShards.put("es_node_id_1", Lists.newArrayList("0", "1"));
    idToShards.put("es_node_id_2", Lists.newArrayList("2", "3", "4"));

    configData = JSON.parseObject("{\"indexName\":\"test_index\",\"numberShards\":5,\"hdfsWorkDir\":\"/tmp/es\"}");
  }

  @Test
  public void testBuild() {
    indexBuilder.build(idToShards, configData);
  }

}
