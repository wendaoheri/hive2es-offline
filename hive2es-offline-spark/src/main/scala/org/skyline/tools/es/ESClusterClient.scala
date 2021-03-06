package org.skyline.tools.es

import java.net.{InetAddress, InetSocketAddress}

import com.alibaba.fastjson.JSONObject
import org.apache.commons.logging.LogFactory
import org.elasticsearch.client.transport.TransportClient
import org.elasticsearch.common.settings.Settings
import org.elasticsearch.common.transport.InetSocketTransportAddress
import org.elasticsearch.client.transport.TransportClient
import org.elasticsearch.common.transport.InetSocketTransportAddress
import java.net.InetAddress
//spark driver使用的es client
class ESClusterClient(val indexName: String,val shardsNum: Int,val typeName:String) {

  @transient private lazy val log = LogFactory.getLog(getClass)

  def createIndex(): Boolean = {
    val settings = Settings.builder
      .put("number_of_replicas", 0)
      .put("number_of_shards", shardsNum)
      .put("cluster.name","paic-elasticsearch")
      .build



    val client = TransportClient.builder.build
      .addTransportAddress(new InetSocketTransportAddress(InetAddress.getByName("26.6.0.90"), 9300))
      .addTransportAddress(new InetSocketTransportAddress(InetAddress.getByName("26.6.0.90"), 9400))
      .addTransportAddress(new InetSocketTransportAddress(InetAddress.getByName("26.6.0.91"), 9300))
      .addTransportAddress(new InetSocketTransportAddress(InetAddress.getByName("26.6.0.90"), 9400))

    val createRespon: Boolean = client.admin().indices().prepareCreate(indexName).setSettings(settings).get().isAcknowledged()
    return createRespon
  }

  def putMapping2ESClusterIndex(mapping: JSONObject): Boolean = {
    val node1 = new InetSocketTransportAddress(InetAddress.getByName("26.6.0.90"), 9400)
    val node2 = new InetSocketTransportAddress(InetAddress.getByName("26.6.0.91"), 9400)
    val client: TransportClient = TransportClient.builder().build().addTransportAddresses(node1, node2)
    log.info("put mapping start")
    val root = new JSONObject()
    root.put("properties", mapping)
    val result: Boolean = client.admin().indices().preparePutMapping(indexName).setType(typeName).setSource(root).execute().actionGet().isAcknowledged
    return result
  }
}
