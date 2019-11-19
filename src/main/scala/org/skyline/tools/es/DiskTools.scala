package org.skyline.tools.es

import java.nio.file.{Files, Paths}
import java.util.stream.Collectors

/**
  * @author Sean Liu
  * @date 2019-10-30
  */
object DiskTools {


  def main(args: Array[String]): Unit = {
    import scala.collection.JavaConversions._
    val nodeDirs = Files.list(Paths.get("/Users/sean/data/es/test/0/elasticsearch_0/nodes")).collect(Collectors.toList()).toSeq
    val maxPath = nodeDirs.maxBy(p => p.getFileName.toString.toInt)
    println(maxPath)

  }

}
