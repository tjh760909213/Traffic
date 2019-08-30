package province

import java.util.Properties

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession
import org.apache.spark.streaming.{Seconds, StreamingContext}
import pool.JedisConnectionPool
import redis.clients.jedis.Jedis

object province {
  def main(args: Array[String]): Unit = {

    val conf = new SparkConf().setAppName("offset").setMaster("local[2]")



    val sc =new SparkContext(conf)


    //处理省份数据
    val pro: RDD[String] = sc.textFile("/Users/denyo/Desktop/练习/第四阶段/充值平台实时统计分析/data/city.txt")

    pro.foreachPartition(

      a => {
        val jedis =JedisConnectionPool.getConnection()

        a.foreach(x => {
          val k: Array[String] = x.split(" ")
          println ( k )

          val id = k(0)
                val proN = k ( 1 )
                //传入前面是id，后面是省的名字
                jedis.set ("provinceTmp:"+ id, proN )
                //前面是省名字，后面为数量
                jedis.set ( "province:"+proN, "0" )
        })
        jedis.close()

      }
    )
//      a=> {
//
//      val jedis =JedisConnectionPool.getConnection()
//
//      a.map((a: Array[String]) => {
//
//
//      val id = a(0)
//      val proN = a ( 1 )
//      //传入前面是id，后面是省的名字
//      jedis.set ( id, proN )
//      //前面是省名字，后面为数量
//      jedis.set ( proN, "0" )
//
//      //println ( jedis.get ( id ) )
//
//      jedis.close ()
//    })})

  }
}
