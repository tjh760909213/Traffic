package test

import java.lang
import java.sql.Statement

import Mysql.mysql
import com.alibaba.fastjson.{JSON, JSONObject}
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.common.TopicPartition
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.streaming.dstream.InputDStream
import org.apache.spark.streaming.kafka010.{ConsumerStrategies, HasOffsetRanges, KafkaUtils, LocationStrategies, OffsetRange}
import pool.JedisConnectionPool
import redis.clients.jedis.Jedis

import scala.collection.mutable

object consumer {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("offset").setMaster("local[3]")
      //设置每秒钟每个分区拉取kafka的速率
      .set("spark.streaming.kafka.maxRatePerPartition","100")
      //设置序列化机制
      .set("spark.serlizer","org.apache.spark.serializer.KryoSerializer")

    val jedis: Jedis = JedisConnectionPool.getConnection ()


    val ssc: StreamingContext = new StreamingContext(conf,Seconds(3))
    val spark = SparkSession.builder().appName("b").getOrCreate()


    import spark.implicits._



    //配置参数
    //配置基本参数
    //组名
    val groupId = "zk00"
    //topic
    val topic = "dask"
    //指定Kafka的broker地址（SparkStreaming程序消费过程中，需要和Kafka的分区对应）
    val brokerList = "hadoop01:9092,hadoop02:9092,hadoop03:9092"
    // 编写Kafka的配置参数
    val kafkas = Map[String,Object](
      "bootstrap.servers"->brokerList,
      // kafka的Key和values解码方式
      "key.deserializer"-> classOf[StringDeserializer],
      "value.deserializer"-> classOf[StringDeserializer],
      "group.id"->groupId,
      // 从头消费
      "auto.offset.reset"-> "earliest",
      // 不需要程序自动提交Offset
      "enable.auto.commit"-> (false:lang.Boolean)
    )
    // 创建topic集合，可能会消费多个Topic
    val topics = Set(topic)
    // 第一步获取Offset
    // 第二步通过Offset获取Kafka数据
    // 第三步提交更新Offset
    // 获取Offset
    var fromOffset:Map[TopicPartition,Long] = JedisOffset(groupId)
    // 判断一下有没数据
    val stream :InputDStream[ConsumerRecord[String,String]] =
      if(fromOffset.size == 0){
        KafkaUtils.createDirectStream(ssc,
          // 本地策略
          // 将数据均匀的分配到各个Executor上面
          LocationStrategies.PreferConsistent,
          // 消费者策略
          // 可以动态增加分区
          ConsumerStrategies.Subscribe[String,String](topics,kafkas)
        )
      }else{
        // 不是第一次消费
        KafkaUtils.createDirectStream(
          ssc,
          LocationStrategies.PreferConsistent,
          ConsumerStrategies.Assign[String,String](fromOffset.keys,kafkas,fromOffset)
        )
      }




    stream.foreachRDD(rdd=>{
      val offestRange: Array[OffsetRange] = rdd.asInstanceOf[HasOffsetRanges].offsetRanges
      val data: mutable.Buffer[String] = rdd.map(_.value()).collect().toBuffer


      data.foreach(a=>{


            val jsondata: JSONObject = JSON.parseObject(a)


            val Day = jsondata.getString("date").substring(0,10)
            val Month = jsondata.getString("date").substring(0,7)

            val money = jsondata.getString("money").toLong
            val phone = jsondata.getString("phoneNum")


            if(jedis.exists(Day)){
              val money1 = jedis.get("test:"+Day).toLong+money
              jedis.set("test:"+Day,money.toString)
            }else{
              jedis.set("test:"+Day,money.toString)
            }

            if(jedis.exists(Month)){
              jedis.set("test:"+Month,(jedis.get("test:"+Month).toLong+money).toString)

            }else{
              jedis.set("test:"+Month,money.toString)
            }


      })

      for (or<-offestRange){
        jedis.hset(groupId,or.topic+"-"+or.partition,or.untilOffset.toString)
      }

    })

    // 启动
    ssc.start()
    ssc.awaitTermination()
  }
}
