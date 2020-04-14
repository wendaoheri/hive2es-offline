package org.skyline.tools.es

import com.alibaba.fastjson.JSON
import com.google.common.base.Charsets
import org.apache.commons.logging.LogFactory
import org.apache.curator.framework.CuratorFrameworkFactory
import org.apache.curator.retry.RetryNTimes
import org.apache.zookeeper.CreateMode
import org.skyline.tools.es.ArgsParser.Config

class ServerNotifier(config: Config) {
  @transient private lazy val log = LogFactory.getLog(getClass)
  val client = CuratorFrameworkFactory
    .newClient(config.zookeeper, 60000, 5000, new RetryNTimes(5, 5000))
  client.start()

  val indexName = config.indexName

  val path = s"${config.chroot}/indices/$indexName"

  def startIndex(): Boolean = {
    val data = JSON.parse(
      s"""
         |{
         |  "numberShards" : ${config.numShards},
         |  "hdfsWorkDir" : "${config.hdfsWorkDir}",
         |  "indexName" : "$indexName",
         |  "typeName" : ${config.typeName},
         |  "state" : "started"
         |}
       """.stripMargin).toString
    persist(path, data)
    true
  }

  def completeIndex(): Boolean = {
    val data = JSON.parse(
      s"""
         |{
         |  "numberShards" : ${config.numShards},
         |  "hdfsWorkDir" : "${config.hdfsWorkDir}",
         |  "indexName" : "$indexName",
         |  "state" : "completed"
         |}
       """.stripMargin).toString

    persist(path, data)
    true
  }

  def isExisted(path: String): Boolean = {
    return null != client.checkExists.forPath(path)
  }


  def persist(path: String, value: String): Unit = {
    log.info("Persist zk path [{}] value [{}]", path, value)
    if (!isExisted(path)) {
      client.create.creatingParentsIfNeeded.withMode(CreateMode.PERSISTENT).forPath(path, value.getBytes(Charsets.UTF_8))
    }
    else {
      update(path, value)
    }
  }

  def update(path: String, value: String): Unit = {
    log.info("Update zk path [{}] value [{}]", path, value)
    client.transaction
      .forOperations(
        client.transactionOp.check.forPath(path),
        client.transactionOp.setData.forPath(path, value.getBytes(Charsets.UTF_8))
      )
  }
}
