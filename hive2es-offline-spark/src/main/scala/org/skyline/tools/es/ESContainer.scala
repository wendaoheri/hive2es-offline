package org.skyline.tools.es

import java.nio.channels.{FileChannel, FileLock}
import java.nio.file._
import java.util.concurrent.TimeUnit
import java.util.concurrent.atomic.AtomicLong
import java.util.stream.Collectors

import com.alibaba.fastjson.JSONObject
import org.apache.commons.io.FileUtils
import org.apache.commons.lang3.StringUtils
import org.apache.commons.logging.LogFactory
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.FileSystem
import org.elasticsearch.action.bulk.{BackoffPolicy, BulkProcessor, BulkRequest, BulkResponse}
import org.elasticsearch.action.index.IndexRequest
import org.elasticsearch.common.settings.Settings
import org.elasticsearch.common.unit.{ByteSizeUnit, ByteSizeValue, TimeValue}
import org.elasticsearch.node.NodeBuilder
import org.skyline.tools.es.ArgsParser.Config

/**
  * @author Sean Liu
  * @date 2019-09-17
  */


class ESContainer(val config: Config, val partitionId: Int) {

  @transient private lazy val log = LogFactory.getLog(getClass)
  private val dataDirs = config.localDataDir.split(",").map(dir => s"${dir}/${config.indexName}")


  private val lockFile = {
    val _tmp = Paths.get(s"/tmp/es_offline_lock/${config.indexName}.lock")
    if (!Files.exists(_tmp)) {
      val parentDir = _tmp.getParent
      if (!Files.exists(parentDir)) {
        Files.createDirectories(parentDir)
      }
      if (!Files.exists(_tmp)) {
        Files.createFile(_tmp)
      }
    }
    _tmp
  }
  private val channel = FileChannel.open(lockFile, StandardOpenOption.WRITE)
  private var lock: FileLock = null

  private val dataDir = {

    val chosenParent = try {
      lock = channel.lock()
      dataDirs.map(p => {
        val fileCount = if (!Files.exists(Paths.get(p))) {
          0
        } else {
          Files.list(Paths.get(p)).count()
        }
        (p, fileCount)
      }).minBy(x => x._2)._1
    } finally {
      if (lock != null) {
        lock.release()
      }

    }

    val chosenDir = s"$chosenParent/$partitionId"
    Files.createDirectories(Paths.get(chosenDir))
    chosenDir
  }

  log.info(s"data dir : $dataDir")

  private lazy val settings = {

    Settings
      .settingsBuilder
      .put("http.enabled", false)
      .put("node.name", s"es_node_$partitionId")
      .put("path.home", dataDir)
      .put("path.data", dataDir)
      .put("Dlog4j2.enable.threadlocals", false)
      .put("cluster.routing.allocation.disk.threshold_enabled", false)
      .put("indices.store.throttle.max_bytes_per_sec", "200mb")
      .put("indices.memory.index_buffer_size", "50%")
      .put("indices.memory.min_shard_index_buffer_size", "200m")
      .put("threadpool.bulk.size", 4)
      .put("threadpool.bulk.queue_size", 1000)
      .putArray("discovery.zen.ping.unicast.hosts")
      .build()
  }
  private val clusterName = s"elasticsearch_${partitionId}"

  private lazy val node = NodeBuilder.nodeBuilder()
    .client(false)
    .local(true)
    .data(true)
    .clusterName(clusterName)
    .settings(settings)
    .build()

  private val counter = new AtomicLong(0)

  private val bulkProcessor = BulkProcessor.builder(node.client(), new BulkProcessor.Listener {

    override def beforeBulk(executionId: Long, request: BulkRequest): Unit = {
      log.info(s"partition $partitionId start bulk request size : ${request.numberOfActions()}")
    }

    override def afterBulk(executionId: Long, request: BulkRequest, response: BulkResponse): Unit = {
      if (response.hasFailures()) {
        log.error(response.buildFailureMessage())
      }
      val total = counter.addAndGet(request.numberOfActions())
      log.info(s"partition $partitionId start bulk request size : ${request.numberOfActions()} and total $total and took time : ${response.getTookInMillis} ms.")
    }

    override def afterBulk(executionId: Long, request: BulkRequest, failure: Throwable): Unit = {
      log.info("failed bulk")
    }
  }).setBulkActions(config.bulkActions)
    .setBulkSize(new ByteSizeValue(config.bulkSize, ByteSizeUnit.MB))
    .setFlushInterval(TimeValue.timeValueSeconds(5))
    .setConcurrentRequests(1)
    .setBackoffPolicy(BackoffPolicy.exponentialBackoff(TimeValue.timeValueSeconds(100), 3))
    .build()

  private val fs = FileSystem.get(new Configuration())

  def start(): Unit = {
    node.start()
    System.gc()
  }

  def put(doc: JSONObject, id: String, routing: String = null): Unit = {
    bulkProcessor.add(new IndexRequest(config.indexName, config.typeName, id).routing(routing).create(true).source(doc))
  }

  def createIndex(): Boolean = {
    log.info("create index start")
    if (indexExists()) {
      log.info(s"index ${config.indexName} already exists, delete first")
      deleteIndex()
    }
    val resp = node.client().admin().indices().prepareCreate(config.indexName)
      .setSettings(
        s"""
           |{
           |    "index": {
           |        "number_of_replicas": "0",
           |        "refresh_interval": "-1",
           |        "number_of_shards": "${config.numShards}",
           |        "merge": {
           |          "scheduler": {
           |            "max_thread_count": "4",
           |            "auto_throttle": "false"
           |          },
           |          "policy": {
           |            "max_merged_segment": "2mb"
           |          }
           |        },
           |        "translog":{
           |          "flush_threshold_size": "10gb",
           |          "durability": "async",
           |          "sync_interval": "10m"
           |        }
           |    }
           |}
         """.stripMargin)
      .get()
    val success = resp.isAcknowledged()

    log.info(s"create index success : $success")
    success
  }

  def getNodePath(): Path = {
    import scala.collection.JavaConversions._
    val nodeDirs = Files.list(Paths.get(dataDir, clusterName, "nodes")).collect(Collectors.toList()).toSeq
    nodeDirs.maxBy(p => p.getFileName.toString.toInt).resolve("indices").resolve(config.indexName)
  }

  def indexExists(): Boolean = {
    node.client().admin().indices().prepareExists(config.indexName).get().isExists
  }

  def deleteIndex(): Boolean = {
    log.info(s"delete index ${config.indexName}")
    node.client().admin().indices().prepareDelete(config.indexName).get().isAcknowledged
  }

  def putMapping(mapping: JSONObject): Unit = {
    log.info("put mapping start")
    val root = new JSONObject()
    root.put("properties", mapping)
    disableMeta("_all", root)
    node.client.admin().indices().preparePutMapping(config.indexName).setType(config.typeName).setSource(root).execute().actionGet()
    log.info("put mapping end")
  }

  def disableMeta(meta: String, mappingRoot: JSONObject): Unit = {
    val disabled = new JSONObject()
    disabled.put("enabled", false)
    mappingRoot.put(meta, disabled)
  }

  private def close(): Unit = {
    log.info("close node start")
    log.info("flush processor")
    bulkProcessor.flush()
    log.info("close processor")
    bulkProcessor.awaitClose(10, TimeUnit.MINUTES)
    log.info("flush index")
    node.client().admin().indices().prepareFlush(config.indexName).get
    log.info("merge index")
    node.client().admin().indices().prepareForceMerge(config.indexName).setMaxNumSegments(1).get()
    node.client().close()
    log.info("close node")
    node.close()
    log.info("close node end")
  }

  private def uploadToHdfs(src: Path, dest: Path): Unit = {
    if (!fs.exists(new org.apache.hadoop.fs.Path(dest.getParent.toString))) {
      log.info(s"hdfs path ${dest.getParent} not exist and create it")
      fs.mkdirs(new org.apache.hadoop.fs.Path(dest.getParent.toString))
    }
    fs.copyFromLocalFile(true, true, new org.apache.hadoop.fs.Path(src.toString), new org.apache.hadoop.fs.Path(dest.toString))
    log.info(s"upload index folder from $src to $dest")
  }

  private def compressIndexAndUpload(): Unit = {
    val zipSource = getNodePath()
    log.info("compress index file and upload to hdfs start")
    import scala.collection.JavaConversions._
    log.info(s"zip source dirs is $zipSource")
    val zipSourceExists = Files.exists(zipSource)
    log.info(s"zip source ${zipSource} exists : $zipSourceExists")
    val shardFiles = Files.list(zipSource).collect(Collectors.toList())

    log.info(s"shard files : ${shardFiles.mkString(",")}")
    for (
      p <- shardFiles
      if p.endsWith("_state") ||
        Files.list(p.resolve("index")).iterator().filter(x => x.toString.endsWith(".fdt") || x.toString.endsWith(".cfs")).size > 0
    ) {
      val folderName = p.getFileName.toString
      val zipFileName = s"p${partitionId}_$folderName.zip"
      val zipRootDir = if (p.endsWith("_state")) "_state" else s"p_$partitionId"

      log.info(s"zip index partition folder from $p to ${zipSource.resolve(zipFileName)} and zip rootDirName is $zipRootDir")
      CompressionUtils.zip(p, zipSource.resolve(zipFileName), zipRootDir)
      uploadToHdfs(zipSource.resolve(zipFileName), Paths.get(config.hdfsWorkDir, config.indexName, folderName, zipFileName))
    }
    log.info("compress index file and upload to hdfs end")
  }

  private def deleteWorkDir(): Unit = {
    if (Files.exists(Paths.get(dataDir))) {
      log.info(s"delete data dir $dataDir")
      FileUtils.deleteDirectory(Paths.get(dataDir).toFile)
    }
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
