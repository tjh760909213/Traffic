package com.Test

import com.util.{ConnectPoolUtils, JedisConnectionPool}
import org.apache.spark.rdd.RDD
import org.apache.spark.streaming.StreamingContext

/**
  * 指标统计
  */
object JedisAPP {

  // 指标1
  def Result01(lines:RDD[(String, List[Double])]): Unit ={
    lines.foreachPartition(f=>{
      val jedis = JedisConnectionPool.getConnection()
      f.foreach(t=>{
        // 充值订单数
        jedis.hincrBy(t._1,"count",t._2.head.toLong)
        // 充值金额
        jedis.hincrByFloat(t._1,"money",t._2(1))
        // 充值成功数
        jedis.hincrBy(t._1,"success",t._2(2).toLong)
        // 充值总时长
        jedis.hincrBy(t._1,"time",t._2(3).toLong)
      })
      jedis.close()
    })
  }
  //指标2
  def Result02(lines: RDD[(String, Double)]): Unit ={
    lines.foreachPartition(f=>{
      val jedis = JedisConnectionPool.getConnection()
      f.foreach(t=>{
       jedis.incrBy(t._1,t._2.toLong)
      })
      jedis.close()
    })
  }
  // 指标二
  def Result03(lines:RDD[((String, String), List[Double])]): Unit ={
    lines.foreachPartition(f=>{
      // 获取连接
      val conn = ConnectPoolUtils.getConnections()
      // 处理数据
      f.foreach(t=>{
        val sql = "insert into ProHour(Pro,Hour,counts) " +
          "values('"+t._1._1+"','"+t._1._2+"',"+(t._2(0)-t._2(2))+")"
        val state = conn.createStatement()
        state.executeUpdate(sql)
      })
      // 还链接
      ConnectPoolUtils.resultConn(conn)
    })
  }
  // 指标三
  def Result04(lines:RDD[(String, List[Double])],ssc: StreamingContext): Unit ={
    // 先进行排序，安装订单量进行排序
    val sortKV = lines.sortBy(_._2.head,false)
    // 进行百分比求值（成功率）
    val value = sortKV.map(t => (t._1, (t._2(2) / t._2.head).toInt,t._2(0).toInt)).take(10)
    // 创建新的RDD
    val rdd = ssc.sparkContext.makeRDD(value)
    // 循环没个分区
    rdd.foreachPartition(f=>{
      // 拿到连接
      val connection = ConnectPoolUtils.getConnections()
        // 将数据存储到Mysql
      f.foreach(t=>{
        val sql = "insert into tb_access_status(logDate,pv,uv)" +
          " values('"+t._1+"','"+t._3+"','"+t._2+"')"
        val state = connection.createStatement()
        state.executeUpdate(sql)
      })
      ConnectPoolUtils.resultConn(connection)
    })
  }
}
