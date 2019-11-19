package org.skyline.tools.es

import java.nio.file.{Files, Paths}

import com.alibaba.fastjson.{JSON, JSONObject}
import org.apache.commons.logging.LogFactory
import org.apache.hadoop.hdfs.server.blockmanagement.BlockManager
import org.apache.spark.sql.SparkSession
import org.apache.spark.{Partitioner, SparkEnv, TaskContext}
import org.elasticsearch.common.math.MathUtils

/**
  * @author Sean Liu
  * @date 2019-09-17
  */
object Hive2ES {
  @transient private lazy val log = LogFactory.getLog(getClass)

  def main(args: Array[String]): Unit = {
    try {
      //      val config = parseArgs(args)

      val spark = SparkSession
        .builder()
        .appName("Hive2ES tools offline")
        .enableHiveSupport()
        .master("local[1]")
        .getOrCreate()


      val sc = spark.sparkContext

      val data = spark.read.orc("/Users/sean/data/customer/part-01008-5414b98a-fa49-457f-aa6c-b70b5e9721e5-c000.snappy.orc")
      val mapping = Files.readAllLines(Paths.get("/Users/sean/data/customer/mapping.json")).get(0)
      val config = Config(indexName = "test", typeName = "test", numShards = 6, indexMapping = mapping)
      val configB = sc.broadcast(config)
      data.rdd.coalesce(1, false).foreachPartition(docsP => {
        val partitionId = TaskContext.get.partitionId()

        TaskContext.get.addTaskCompletionListener(_ => {
          SparkEnv.get.blockManager.diskBlockManager.getAllBlocks().foreach(blockId => SparkEnv.get.blockManager.removeBlock(blockId))
        })

        val esContainer = new ESContainer(configB.value, partitionId)
        esContainer.createIndex()
        esContainer.putMapping()
        docsP.foreach(doc => esContainer.put(JSON.parseObject(doc.getAs[String]("content")),
          doc.getAs[String]("key"))
        )
      })
//      val docs = data.rdd.map(row => {
//        val jo = new JSONObject()
//        row.schema.fields.foreach(field => {
//          jo.put(field.name, row.getAs(field.name))
//        })
//        (jo.getString("code"), jo)
//      }).partitionBy(new ESHashPartitioner(6))
//
//      docs.foreachPartition(itDocs => {
//        val partitionId = TaskContext.get.partitionId
//        val esContainer = new ESContainer(configB.value, partitionId)
//        try {
//          esContainer.createIndex()
//          esContainer.putMapping()
//          itDocs.foreach(doc => {
//            esContainer.put(doc._2, doc._1)
//          })
//        } finally {
//          esContainer.cleanUp()
//        }
//
//      })


    } catch {
      case e: IllegalArgumentException =>
    }

  }

  class ESHashPartitioner(partitions: Int) extends Partitioner {
    require(partitions >= 0, s"Number of partitions ($partitions) cannot be negative.")

    def numPartitions: Int = partitions

    def getPartition(key: Any): Int = key match {
      case null => 0
      case x: String => MathUtils.mod(Murmur3HashFunction.hash(x), numPartitions)
      case _ => MathUtils.mod(key.hashCode, numPartitions)
    }

    override def equals(other: Any): Boolean = other match {
      case h: ESHashPartitioner =>
        h.numPartitions == numPartitions
      case _ =>
        false
    }

    override def hashCode: Int = numPartitions
  }

  case class Config(
                     hiveTable: String = "",
                     numShards: Int = 3,
                     numN: Int = 4,
                     jsonSource: Boolean = false,
                     indexName: String = "test2",
                     typeName: String = "test",
                     alias: String = "",
                     hdfsWorkDir: String = "/Users/sean/data/es/hdfs",
                     localWorkDir: String = "/Users/sean/data/es",
                     localDataDir: String = "/Users/sean/data/es",
                     indexSettings: String = "",
                     indexMapping: String = "",
                     indexMappingObj: JSONObject = new JSONObject()
                   )

  def parseArgs(args: Array[String]): Config = {
    val parser = new scopt.OptionParser[Config]("hive2es") {
      head("hive2es", "1.0")

      opt[String]('t', "hive-table").required().action((x, c) =>
        c.copy(hiveTable = x)).text("Source hive table")

      opt[Int]('s', "number-of-shards").required().action((x, c) =>
        c.copy(numShards = x)).text("Number of ES Index Shards")
    }

    parser.parse(args, Config()) match {
      case Some(config) =>
        config
      case None =>
        Config()
    }
  }

}
