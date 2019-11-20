package org.skyline.tools.es

import org.skyline.tools.es.Hive2ES.Config


object PAHive2ES {


  def main(args: Array[String]): Unit = {
    val config = Config(id = "test")
    val partitionKey = Option(config.routing).getOrElse(config.id)
    print(partitionKey)
  }

}

