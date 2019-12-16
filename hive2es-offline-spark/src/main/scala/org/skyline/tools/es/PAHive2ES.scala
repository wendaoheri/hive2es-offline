package org.skyline.tools.es

import java.nio.file.{Files, Paths}
import java.util.Date
import java.util.concurrent.atomic.AtomicLong

import com.alibaba.fastjson.{JSON, JSONObject}
import org.apache.commons.lang.time.DateFormatUtils
import org.apache.commons.lang3.StringUtils
import org.apache.commons.logging.LogFactory
import org.apache.spark.TaskContext
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types.MapType
import org.apache.spark.storage.StorageLevel
import org.json4s.DefaultFormats
import org.json4s.jackson.Serialization.writePretty
import org.skyline.tools.es.ArgsParser.{Config, argsParser}

object PAHive2ES {


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

    val dt = config.indexName.substring(config.indexName.lastIndexOf("_") + 1)
    val alias = Option(config.alias).getOrElse(config.indexName.substring(0, config.indexName.lastIndexOf("_")))

    val sc = spark.sparkContext
    val whereClause = Some(config.where).getOrElse("1 = 1")
    val input = spark.read.table(config.hiveTable).where(whereClause)

    val numPartitions = config.numShards * config.partitionMultiples
    val partitionKey = Option(config.routing).getOrElse(config.id)

    val hiveInputFields = config.hiveInputFields

    def needIndex(fieldName: String, esKey: String): Boolean = {
      if (config.indexHiveFields.contains(fieldName)) {
        return true
      }
      if (config.indexESFields.contains(esKey)) {
        return true
      }
      if (fieldName.endsWith("_il") || fieldName.endsWith("_ex")) {
        return false
      }
      true
    }


    def covertDateFormat(dataType: String, value: Object): Object = {
      if (value != null && dataType.equalsIgnoreCase("date")) {
        DateFormatUtils.format(value.asInstanceOf[Date], "yyyyMMdd")
      } else {
        value
      }
    }

    val dataTypeMapping = spark.sql(
      s"""
         |select
         |  index_name,data_type
         |from
         |  raw.I_DSPDATA_USERINDEX_INDEXFIELD
         |where dt = $dt and lower(theme) = lower("$alias")
       """.stripMargin).collect().map(r => {
      (r.getAs[String]("index_name").trim, r.getAs[String]("data_type").trim)
    }).toMap

    val data = input.rdd.map(r => {
      r.schema.fields.flatMap(f => {
        f.dataType match {
          case x: MapType => {
            val mapValue = r.getAs[Map[String, Object]](f.name)
            if (mapValue == null) {
              Seq((null, (null, false, null)))
            } else {
              mapValue.keys.map(key => {
                var esKey = if (f.name.endsWith("_il")) {
                  f.name + "-" + key
                } else {
                  "" + key
                }
                esKey = esKey.toLowerCase().replaceAll("&", "-").replaceAll("\\$", "-")
                (esKey, (x.valueType.simpleString, needIndex(f.name, esKey), covertDateFormat(x.valueType.simpleString, mapValue.get(key).get)))
              })
            }
          }
          case _ => Seq((f.name, (f.dataType.simpleString match {
            case "bigint" => "long"
            case "int" => "integer"
            case x if (x.startsWith("decimal")) => "double"
            case _ => f.dataType.simpleString
          }, true, covertDateFormat(f.dataType.simpleString, r.getAs(f.name)))))
        }
      })
    }).persist(StorageLevel.DISK_ONLY)


    val fields = data.flatMap(_.toSeq).filter(_._1 != null).reduceByKey((x, _) => x).collect

    import scala.collection.JavaConverters._


    val indexFieldCount = new AtomicLong()

    val mapping = fields.toMap.map { case (esKey, x) => {
      val index = if (!x._2) {
        "no"
      } else if (x._1.equalsIgnoreCase("string")) {
        indexFieldCount.incrementAndGet()
        "not_analyzed"
      } else {
        indexFieldCount.incrementAndGet()
        null
      }
      var result = Map("type" -> dataTypeMapping.getOrElse(esKey, x._1))
      if (index != null) {
        result += ("index" -> index)
      }
      if (x._1.equalsIgnoreCase("date")) {
        result += ("format" -> "yyyyMMdd")
      }
      (esKey, result.asJava)
    }
    }.asJava
    log.info(s"Mapping index field count / total field : [${indexFieldCount.get()} / ${mapping.size()}]")

    val mappingObj = JSON.toJSON(mapping).asInstanceOf[JSONObject]


    Files.write(Paths.get("mapping.json"), mappingObj.toString().getBytes)
    val mappingString = new String(Files.readAllBytes(Paths.get("mapping.json")))
    Files.delete(Paths.get("mapping.json"))

    def notNullValue(value: Object): Boolean = {
      if (value == null) {
        false
      } else if (value.isInstanceOf[String]) {
        val strValue = value.asInstanceOf[String]
        StringUtils.isNotEmpty(strValue) && !"null".equalsIgnoreCase(strValue)
      } else {
        true
      }
    }

    val docs = data.map(x => {
      val doc = new JSONObject()
      x.foreach(f => {
        if (f._1 != null && notNullValue(f._2._3)) doc.put(f._1, f._2._3)
      })
      (doc.getString(config.id), doc)
    })

    docs.foreachPartition(docsP => {
      val partitionId = TaskContext.get.partitionId

      val esContainer = new ESContainer(config, partitionId)
      try {
        esContainer.start()
        esContainer.createIndex()
        esContainer.putMapping(JSON.parseObject(mappingString))
        var count = 0
        docsP.foreach(doc => {
          esContainer.put(doc._2, doc._1)
          count += 1
        })
        log.info(s"partition $partitionId record size : $count")
      } finally {
        esContainer.cleanUp()
      }
    })

  }

}

