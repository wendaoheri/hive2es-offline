package org.skyline.tools.es.server;

import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.doCallRealMethod;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.when;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.List;
import java.util.Map;
import lombok.extern.slf4j.Slf4j;
import org.apache.curator.framework.CuratorFramework;
import org.elasticsearch.client.transport.TransportClient;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;
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

  @MockBean
  private TransportClient client;

  @Autowired
  private ESClient esClient;

  @Autowired
  private IndexBuilder indexBuilder;

  private static Map<String, List<String>> idToShards;
  private static JSONObject configData;
  private static Path tmpDir;

  @BeforeClass
  public static void beforeClass() throws IOException {
    idToShards = Maps.newHashMap();
    idToShards.put("es_node_id_1", Lists.newArrayList("0", "1"));
    idToShards.put("es_node_id_2", Lists.newArrayList("2", "3", "4"));

    configData = JSON.parseObject(
        "{\"indexName\":\"test_index\",\"numberShards\":5,\"hdfsWorkDir\":\"/tmp/es_offline/hdfs\"}");

    Path dataSource = Paths
        .get(IndexBuilderTest.class.getClassLoader().getResource("kline_daily.zip").getPath());
    tmpDir = Paths.get("/tmp/es_offline/hdfs");
    Utils.unzip(dataSource, tmpDir);

  }

  @Test
  public void testBuild() {
    indexBuilder.build(idToShards, configData);
  }

}
