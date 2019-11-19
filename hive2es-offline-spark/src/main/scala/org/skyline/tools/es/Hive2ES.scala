package org.skyline.tools.es

import com.alibaba.fastjson.{JSON, JSONObject}
import org.apache.spark.sql.SparkSession

import scala.util.Random

object Hive2ES {

  val argsParser = new scopt.OptionParser[Config]("hive2es offline") {
    head("hive2es offline", "1.0")

    opt[String]("hive-table").required()
      .action((x, c) => c.copy(hiveTable = x))
      .text("Source hive table")

    opt[String]("where")
      .action((x, c) => c.copy(where = x))
      .text("Hive table query where clause")

    opt[Int]("number-of-shards").required()
      .action((x, c) => c.copy(numShards = x))
      .text("Number of ES Index Shards")

    opt[Boolean]("repartition").valueName("<true, false>")
      .action((x, c) => c.copy(repartition = x))
      .text("Whether need repartition, repartition by routing column if set to true")

    opt[Int]("partition-multiples")
      .action((x, c) => c.copy(partitionMultiples = x))
      .text("Multiples of spark partitions to es shards, default is 10, only effected when repartition set to true")

    opt[Boolean]("json-source").valueName("<true, false>")
      .action((x, c) => c.copy(jsonSource = x))
      .text("Json Source")

    opt[String]("index-name").required()
      .action((x, c) => c.copy(indexName = x))
      .text("ES index name")

    opt[String]("type-name").required()
      .action((x, c) => c.copy(typeName = x))
      .text("ES type name")

    opt[String]("alias")
      .action((x, c) => c.copy(alias = x))
      .text("ES index alias")

    opt[String]("id")
      .action((x, c) => c.copy(id = x))
      .text("ES ID column")

    opt[String]("routing")
      .action((x, c) => c.copy(id = x))
      .text("ES Routing column, default same with id if id specified")

    opt[String]("hdfs-work-dir")
      .action((x, c) => c.copy(hdfsWorkDir = x))
      .text("Hdfs work dir")

    opt[String]("local-data-dir")
      .action((x, c) => c.copy(localDataDir = x))
      .text("Local data dir")

    opt[Int]("bulk-actions")
      .action((x, c) => c.copy(bulkActions = x))
      .text("Number of bulk actions, default 100")

    opt[Int]("bulk-size")
      .action((x, c) => c.copy(bulkSize = x))
      .text("Size of bulk actions, unit M, default 5M")
  }

  def main(args: Array[String]): Unit = {
    argsParser.parse(args, Config()) match {
      case Some(config) => {
        val spark = SparkSession
          .builder()
          .appName("Hive2ES tools offline")
          .enableHiveSupport()
          .getOrCreate()

        val sc = spark.sparkContext
        val data = spark.read.table(config.hiveTable).where(s"1 = 1 and ${config.where}")

        val numPartitions = config.numShards * config.partitionMultiples
        val partitionKey = Some(config.routing).orElse(Some(config.id)).get

        data.rdd.map(row => {

          val doc = if (config.jsonSource) {
            JSON.parseObject(row.getAs[String](0))
          } else {
            val jo = new JSONObject()
            row.schema.fields.foreach(field => {
              jo.put(field.name, row.getAs(field.name))
            })
            jo
          }

          val key = if (partitionKey == null) {
            Random.nextString(20)
          } else {
            doc.get(partitionKey).toString
          }

          (key, doc)

        })


      }
      case _ =>
        sys.exit(1)
    }


  }


  case class Config(
                     hiveTable: String = null,
                     where: String = null,
                     numShards: Int = 3,
                     repartition: Boolean = false,
                     partitionMultiples: Int = 10,
                     jsonSource: Boolean = false,
                     indexName: String = null,
                     typeName: String = null,
                     alias: String = null,
                     id: String = null,
                     routing: String = null,
                     hdfsWorkDir: String = "/tmp/hive2es",
                     localDataDir: String = "/tmp/hive2es",
                     bulkActions: Int = 100,
                     bulkSize: Int = 5
                   )

}

