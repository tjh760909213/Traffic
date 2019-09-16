package weektest

import java.util

import org.apache.kafka.common.TopicPartition

/**
  * 获取redis内部的offset
  */
object JedisOffset {

  def apply(groupId:String):Map[TopicPartition,Long] ={
    //创建最后的返回值类型
    var formdOffset = Map[TopicPartition,Long]()

    //创建jedis连接
    val jedis = JedisConnectionPool.getConnection()
    //查询redis中所有的topic、Partition
    val topicPartitionOffset: util.Map[String, String] = jedis.hgetAll(groupId)
    //需要执行隐式转换操作
    import scala.collection.JavaConversions._
    //将map转换list进行循环处理
    val topicPartitionOffseeetList: List[(String, String)] = topicPartitionOffset.toList
    //循环处理数据
    for(topicPL<-topicPartitionOffseeetList){
      val str = topicPL._1.split("[-]")
      formdOffset +=(new TopicPartition(str(0),str(1).toInt)->topicPL._2.toLong)
    }
    formdOffset
  }
}
