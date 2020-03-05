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


    val dataTypeMappingSql =
      s"""
         |select
         |  index_name,data_type
         |from
         |  raw.I_DSPDATA_USERINDEX_INDEXFIELD
         |where dt = $dt and lower(theme) = lower("$alias")
       """.stripMargin

    val dataTypeMapping = spark.sql(dataTypeMappingSql).collect().map(r => {
      (r.getAs[String]("index_name").trim, r.getAs[String]("data_type").trim)
    }).toMap

    log.info(dataTypeMappingSql)
    log.info(dataTypeMapping)

    def dataTypeConvert(fieldName: String, dataType: String): String = {
      dataTypeMapping.getOrElse(fieldName, dataType.toLowerCase match {
        case "bigint" => "long"
        case "int" => "integer"
        case x if (x.startsWith("decimal")) => "double"
        case _ => dataType
      })
    }

    def mapFieldName(fieldName: String, key: String): String = {
      val esKey = if (fieldName.endsWith("_il")) {
        fieldName + "-" + key
      } else {
        "" + key
      }
      esKey.toLowerCase().replaceAll("&", "-").replaceAll("\\$", "-")
    }

    val fields = input.rdd.flatMap(r => {
      r.schema.fields.flatMap(f => {
        f.dataType match {
          case x: MapType => {
            val mapValue = r.getAs[Map[String, Object]](f.name)
            if (mapValue != null) {
              mapValue.keys.map(key => {
                val esKey = mapFieldName(f.name, key)
                (esKey, (x.valueType.simpleString, false, f.name))
              })
            } else Seq()
          }
          case _ => Seq((f.name, (f.dataType.simpleString, true, f.name)))
        }
      })
    }).filter(_._1 != null).distinct().collect()

    import scala.collection.JavaConverters._


    val indexFieldCount = new AtomicLong()

    val mapping = fields.toMap.map { case (esKey, x) => {
      val indexField = x._2 || needIndex(x._3, esKey)
      val dataType = dataTypeConvert(esKey, x._1)
      val index = if (!indexField) {
        "no"
      } else if (dataType.equalsIgnoreCase("string")) {
        indexFieldCount.incrementAndGet()
        "not_analyzed"
      } else {
        indexFieldCount.incrementAndGet()
        null
      }
      var result = Map("type" -> dataType)
      if (index != null) {
        result += ("index" -> index)
      }
      if (dataType.equalsIgnoreCase("date")) {
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

    log.info(mappingString)

    val serverNotifier = new ServerNotifier(config)
//    serverNotifier.startIndex()

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

    def getFinalValue(fieldName: String, dataType: String, value: Object): Object = {
      if (dataType.contains("[")) { // Array
        JSON.parseArray(value.asInstanceOf[String])
      } else if (value != null && dataType.equalsIgnoreCase("date")) { // date
        DateFormatUtils.format(value.asInstanceOf[Date], "yyyyMMdd")
      } else if (null != value && dataTypeMapping.contains(fieldName)) { // manually assign
        val finalDataType = mappingObj.getJSONObject(fieldName).getString("type")
        finalDataType match {
          case "long" => java.lang.Long.valueOf(value.toString)
          case "integer" => if (value.isInstanceOf[java.lang.Double]) {
            value.asInstanceOf[java.lang.Double].intValue().asInstanceOf[java.lang.Integer]
          } else {
            value.asInstanceOf[java.lang.Integer]
          }
          case "double" => java.lang.Double.valueOf(value.toString)
          case "string" => value.toString
          case _ => value
        }
      } else if (null != value && dataType.startsWith("decimal")) {  // decimal
        value.asInstanceOf[java.math.BigDecimal].doubleValue().asInstanceOf[java.lang.Double]
      } else { // use default
        value
      }

    }

    // RDD[Array[(esKey,(dataType,needIndex,value))]]
    val data = input.rdd.map(r => {

      val doc = new JSONObject()
      r.schema.fields.flatMap(f => {
        f.dataType match {
          case x: MapType => {
            val mapValue = r.getAs[Map[String, Object]](f.name)
            if (mapValue == null) {
              Seq()
            } else {
              mapValue.keys.map(key => {
                val esKey = mapFieldName(f.name, key)
                (esKey, getFinalValue(esKey, x.valueType.simpleString, mapValue.get(key).get))
              })
            }
          }
          case _ => Seq((f.name, getFinalValue(f.name, f.dataType.simpleString, r.getAs(f.name))))
        }
      }).foreach(f => {
        if (f._1 != null && notNullValue(f._2)) doc.put(f._1, f._2)
      })
      (doc.getString(config.id), doc)
    })

    data.foreachPartition(docsP => {
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
//    serverNotifier.completeIndex()
  }

}

