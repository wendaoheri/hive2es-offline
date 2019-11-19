package org.skyline.tools.es

import org.apache.spark.Partitioner
import org.elasticsearch.common.math.MathUtils

/**
  * @author Sean Liu
  * @date 2019-11-19
  */
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