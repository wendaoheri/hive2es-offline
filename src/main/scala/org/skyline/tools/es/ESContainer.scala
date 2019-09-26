package org.skyline.tools.es

import java.nio.file._
import java.util.concurrent.TimeUnit
import java.util.concurrent.atomic.AtomicLong

import com.alibaba.fastjson.JSONObject
import org.apache.commons.io.FileUtils
import org.apache.commons.logging.LogFactory
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.FileSystem
import org.elasticsearch.action.bulk.{BackoffPolicy, BulkProcessor, BulkRequest, BulkResponse}
import org.elasticsearch.action.index.IndexRequest
import org.elasticsearch.common.settings.Settings
import org.elasticsearch.common.unit.{ByteSizeUnit, ByteSizeValue, TimeValue}
import org.elasticsearch.node.NodeBuilder
import org.skyline.tools.es.Hive2ES.Config

/**
  * @author Sean Liu
  * @date 2019-09-17
  */
class ESContainer(val config: Config,val partitionId: Int) {

  @transient private lazy val log = LogFactory.getLog(getClass)
  private val workDir = s"${config.localWorkDir}/$partitionId"
  private lazy val settings = {

    Settings
      .settingsBuilder
      .put("http.enabled", false)
      .put("node.name", s"es_node_$partitionId")
      .put("path.home", workDir)
      .put("path.data", workDir)
      .put("Dlog4j2.enable.threadlocals", false)
      .put("cluster.routing.allocation.disk.threshold_enabled", false)
      .putArray("discovery.zen.ping.unicast.hosts")
      .build()
  }
  private val clusterName = s"elasticsearch_${partitionId}"
  private val zipSource = Paths.get(config.localWorkDir, partitionId.toString, clusterName, "nodes/0/indices", config.indexName)
  //  private val zipDest = Paths.get(config.localWorkDir, "bundles", s"${config.indexName}_${partitionId}.zip")

  private lazy val node = NodeBuilder.nodeBuilder()
    .client(false)
    .local(false)
    .data(true)
    .clusterName(clusterName)
    .settings(settings)
    .build()
    .start()
  private lazy val counter = new AtomicLong(0)

  private lazy val bulkProcessor = BulkProcessor.builder(node.client(), new BulkProcessor.Listener {

    override def beforeBulk(executionId: Long, request: BulkRequest): Unit = {
      log.info(s"partition $partitionId start bulk request size : ${request.numberOfActions()}")
    }

    override def afterBulk(executionId: Long, request: BulkRequest, response: BulkResponse): Unit = {
      if (response.hasFailures()) {
        log.error(response.buildFailureMessage())
      }
      val total = counter.addAndGet(request.numberOfActions())
      log.info(s"partition $partitionId start bulk request size : ${request.numberOfActions()} and total $total")
    }

    override def afterBulk(executionId: Long, request: BulkRequest, failure: Throwable): Unit = {
      log.info("failed bulk")
    }
  }).setBulkActions(100)
    .setBulkSize(new ByteSizeValue(10, ByteSizeUnit.MB))
    .setFlushInterval(TimeValue.timeValueSeconds(5))
    .setConcurrentRequests(1)
    .setBackoffPolicy(BackoffPolicy.exponentialBackoff(TimeValue.timeValueSeconds(100), 3))
    .build()

  private lazy val fs = FileSystem.get(new Configuration())

  def put(doc: JSONObject, id: String, routing: String = ""): Unit = {
    bulkProcessor.add(new IndexRequest(config.indexName, config.typeName, id).source(doc))
  }

  def createIndex(): Unit = {
    node.client().admin().indices().prepareCreate(config.indexName)
      .setSettings(
        s"""
           |{
           |    "index": {
           |        "number_of_replicas": "0",
           |        "refresh_interval": "-1",
           |        "number_of_shards": "${config.numShards}"
           |    }
           |}
         """.stripMargin)
      .get()
  }

  private def close(): Unit = {
    bulkProcessor.flush()
    bulkProcessor.awaitClose(10, TimeUnit.MINUTES)
    node.client().admin().indices().prepareForceMerge(config.indexName).setMaxNumSegments(1).get()
    node.client().close()
    node.close()
  }

  private def uploadToHdfs(src: Path, dest: Path): Unit = {
    if (!fs.exists(new org.apache.hadoop.fs.Path(dest.getParent.toString))) {
      log.info(s"hdfs path ${dest.getParent} not exist and create it")
      fs.mkdirs(new org.apache.hadoop.fs.Path(dest.getParent.toString))
    }
    fs.copyFromLocalFile(true, true, new org.apache.hadoop.fs.Path(src.toString), new org.apache.hadoop.fs.Path(dest.toString))
  }

  private def compressIndexAndUpload(): Unit = {
    import scala.collection.JavaConversions._
    for (p <- Files.list(zipSource).iterator()) {
      val folderName = p.getFileName.toString
      val zipFileName = s"p${partitionId}_$folderName.zip"
      log.info(s"zip index partition folder from $p to ${zipSource.resolve(zipFileName)}")
      CompressionUtils.zip(p, zipSource.resolve(zipFileName), s"p_$partitionId")
      uploadToHdfs(zipSource.resolve(zipFileName), Paths.get(config.hdfsWorkDir, config.indexName, folderName, zipFileName))
    }
  }

  private def deleteWorkDir(): Unit = {
    // TODO delete failed
    log.info(s"delete work dir $workDir")
    FileUtils.deleteDirectory(Paths.get(workDir).toFile)
  }

  def cleanUp(): Unit = {
    try {
      close()
      compressIndexAndUpload()
    } finally {
      deleteWorkDir()
    }


  }

}
