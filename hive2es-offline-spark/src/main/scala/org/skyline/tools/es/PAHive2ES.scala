package org.skyline.tools.es

import com.alibaba.fastjson.JSON

object PAHive2ES {


  def main(args: Array[String]): Unit = {

    val j = JSON.parseObject(
      """
        |{
        | "a" : "a"
        |}
      """.stripMargin)
    print(j.getString(null))
  }

}

