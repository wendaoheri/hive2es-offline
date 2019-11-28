package org.skyline.tools.es

import com.alibaba.fastjson.{JSON, JSONObject}
import org.apache.commons.logging.LogFactory
import org.apache.spark.TaskContext
import org.apache.spark.sql.SparkSession
import org.json4s._
import org.json4s.jackson.Serialization.writePretty
import org.skyline.tools.es.ArgsParser.{Config, argsParser}

import scala.util.Random


object Hive2ES {
  @transient private lazy val log = LogFactory.getLog(getClass)


  def main(args: Array[String]): Unit = {

    argsParser.parse(args, Config()) match {
      case Some(config) => {
        implicit val formats = DefaultFormats
        log.info("Final application config : \n" + writePretty(config))
        run(config)
      }
      case _ => sys.exit(1)
    }


  }

  def run(config: Config): Unit = {
    val spark = SparkSession
      .builder()
      .appName("Hive2ES tools offline")
      .enableHiveSupport()
      .getOrCreate()

    val sc = spark.sparkContext
    val whereClause = Some(config.where).getOrElse("1 = 1")
    val data = spark.read.table(config.hiveTable).where(whereClause)

    val numPartitions = config.numShards * config.partitionMultiples
    val partitionKey = Option(config.routing).getOrElse(config.id)

    val docsWithKey = data.rdd.map(row => {

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
        Random.nextString(10)
      } else {
        doc.get(partitionKey).toString
      }

      (key, doc)

    })

    val docs = if (config.repartition) {
      docsWithKey.partitionBy(new ESHashPartitioner(numPartitions)).values
    } else {
      docsWithKey.values
    }

    docs.foreachPartition(docsP => {
      val partitionId = TaskContext.get().partitionId()

      val esContainer = new ESContainer(config, partitionId)

      try {
        esContainer.start()
        esContainer.createIndex()
        if (config.mapping != null) {
          esContainer.putMapping(JSON.parseObject(config.mapping))
        }
        var count = 0
        docsP.foreach(doc => {
          esContainer.put(doc, doc.getString(config.id), doc.getString(config.routing))
          count += 1
        })
        log.info(s"partition $partitionId record size : $count")
      } finally {
        esContainer.cleanUp()
      }
    })

//    ServerNotifier.notify(config)

  }

}

