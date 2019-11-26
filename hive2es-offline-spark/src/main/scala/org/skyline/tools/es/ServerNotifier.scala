package org.skyline.tools.es

import com.alibaba.fastjson.JSON
import org.apache.curator.framework.CuratorFrameworkFactory
import org.apache.curator.retry.RetryNTimes
import org.apache.zookeeper.CreateMode
import org.skyline.tools.es.ArgsParser.Config

object ServerNotifier {

  val curatorFramework = CuratorFrameworkFactory
    .newClient("localhost:2181", 60000, 5000, new RetryNTimes(5, 5000))

  curatorFramework.start()

  def notify(config: Config): Boolean = {
    val indexName = "custom"
    val path = s"/es_offline/indices/$indexName"
    val data = JSON.parse(
      s"""
         |{
         |  "numberShards":36,
         |  "hdfsWorkDir":"/tmp/es_offline",
         |  "indexName" : "custom"
         |}
       """.stripMargin)
    curatorFramework.create()
      .creatingParentsIfNeeded()
      .withMode(CreateMode.PERSISTENT)
      .forPath(path, data.toString.getBytes())
    true
  }

  def main(args: Array[String]): Unit = {
    notify(null)
  }

}
