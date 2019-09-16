import java.lang
import java.sql.{Connection, DriverManager, ResultSet, Statement}
import java.util.Properties

import Mysql.mysql
import com.alibaba.fastjson.{JSON, JSONObject}
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.common.TopicPartition
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.streaming.dstream.{DStream, InputDStream}
import org.apache.spark.streaming.kafka010.{ConsumerStrategies, HasOffsetRanges, KafkaUtils, LocationStrategies, OffsetRange}
import org.apache.spark.streaming.{Seconds, StreamingContext}
import pool.JedisConnectionPool
import redis.clients.jedis.Jedis

import scala.collection.mutable

/**
  * Redis管理Offset
  */
object KafkaRedisOffset {
  def main(args: Array[String]): Unit = {




    val conf = new SparkConf().setAppName("offset").setMaster("local[3]")
    //设置每秒钟每个分区拉取kafka的速率
      .set("spark.streaming.kafka.maxRatePerPartition","1000")
    //设置序列化机制
      .set("spark.serlizer","org.apache.spark.serializer.KryoSerializer")

    val jedis: Jedis = JedisConnectionPool.getConnection ()
    jedis.set("orderSum","0")
    jedis.set("money","0")
    jedis.set("Vorder","0")
    jedis.set("Forder","0")


    val ssc: StreamingContext = new StreamingContext(conf,Seconds(3))
    val spark = SparkSession.builder().appName("a").getOrCreate()
    val statement: Statement = mysql.getStatement()


import spark.implicits._



    //配置参数
    //配置基本参数
    //组名
    val groupId = "zk002"
    //topic
    val topic = "hz1803b"
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
        var orderSum = jedis.get("orderSum").toLong
        var money = jedis.get("money").toLong
        var Vorder = jedis.get("Vorder").toLong
        var Forder = jedis.get("Forder").toLong


        val offestRange: Array[OffsetRange] = rdd.asInstanceOf[HasOffsetRanges].offsetRanges
        // 业务处理
        val data: mutable.Buffer[String] = rdd.map(_.value()).collect().toBuffer
        data.foreach(a=>{
          //订单数量
          orderSum+=1

          val jsondata: JSONObject = JSON.parseObject(a)
          val chargefee = jsondata.getString("chargefee").toLong
          money+=chargefee

          val bussinessRst = jsondata.getString("bussinessRst")
          if(bussinessRst=="0000"){
            Vorder+=1
          }else{
            Forder+=1
            var proid = jsondata.getString("provinceCode")
            proid = jedis.get(proid)
            //jedis.get(proid)
          }

          //分钟
          val logOutTime: String = jsondata.getString("logOutTime").substring(0,13)
          if(jedis.exists("min:"+logOutTime)){
            jedis.set("min:"+logOutTime,(jedis.get("min:"+logOutTime).toInt+1).toString)
          }else{
            jedis.set("min:"+logOutTime,"1")
          }



          //   省订单次数  存入redis
          //业务结果
          var serviceName = jsondata.getString("serviceName")
          //获取省id
          var proid = jsondata.getString("provinceCode")
          //通过省id获取省名
          var proname: String = jedis.get("provinceTmp:"+proid)
          val numpro = (jedis.get("province:"+proname).toInt +1)
          jedis.set(s"province:$proname",numpro.toString)


          //   存入各省失败的次数

          if(!bussinessRst.equals("0000") && serviceName.equals("reChargeNotifyReq")){
            if(jedis.exists(s"FP:$proname")){
              jedis.set(s"FP:$proname",(jedis.get(s"FP:$proname").toInt+1).toString)
            } else{
              jedis.set("FP:"+proname,"1")
            }
          }


          /**
            * 每省每小时失败次数
            */
          //获取小时
          var Hourtime = jsondata.getString("logOutTime").substring(0,10)


          //val proList: RDD[(String, Int)] = ssc.sparkContext.makeRDD(List((proname.toString,numpro)))


        if((!bussinessRst.equals("0000")) && serviceName.equalsIgnoreCase("reChargeNotifyReq")) {

          val str = s"select * from totalProvince where  proName='${proname}' and time ='${Hourtime}'  "
          val resultSet = statement.executeQuery ( str )

          if (!resultSet.next ()) {
            statement.execute ( s"insert into totalProvince (proName,sum,time) values ('${proname}',1,'${Hourtime}')  " )
          } else {
            statement.execute ( s"update totalProvince set sum=sum+1 where proName='${proname}' and time='${Hourtime}' " )
          }
        }

          /**
            * 每小时的充值数和钱
            */
        val hourstr = s"select * from HourSumMoney where time='${Hourtime}' "
        val hourres = statement.executeQuery(hourstr)

        if(!hourres.next()){
          statement.execute(s"insert into HourSumMoney (time,sum,money) values ('${Hourtime}',1,'${money}')")
        } else {
          statement.execute(s"update HourSumMoney set sum=sum+1, money= money+'${money}' where time='${Hourtime}' ")
        }



          /**
            * 每省的充值失败数和失败率
            *
            */

          if((!bussinessRst.equals("0000")) && serviceName.equals("reChargeNotifyReq")) {
            val fstr = s"select * from ProNumOfF where 省份='${proname}' "
            val fstrres: ResultSet = statement.executeQuery(hourstr)

            if(fstrres.next()){
              var chu = (1/(jedis.get("province:"+proname).toDouble+1)*100)
              statement.execute(s"insert into ProNumOfF (省份,失败数,失败率) values ('${proname}',1,'${chu}')  ")
            }else{
                val forder = jedis.get(s"FP:${proname}")
                val sumorder = jedis.get("province:"+proname)
                val dou = (forder.toDouble / sumorder.toDouble * 100).toString
                statement.execute ( s"update ProNumOfF set 失败数='${forder.toString}',失败率='${dou}' where 省份='${proname}'")
            }
          }


          /**
            * 可视化
            */


          val hourstr1 = s"select * from tb_access_status where logdate='${Hourtime}' "
          val hourres1 = statement.executeQuery(hourstr1)

          if(!hourres1.next()){
            statement.execute(s"insert into tb_access_status (logdate,pv,uv) values ('${Hourtime}',1,'${money}')")
          } else {
            statement.execute(s"update tb_access_status set pv=pv+1, uv= uv+'${money}' where logdate='${Hourtime}' ")
          }






            //          val proSum: DataFrame = proList.map ( a => {
//            (a._1, a._2.toString)
//          } ).toDF ( "省名", "省订单量" )
//          val protable: DataFrame = mysql.read(spark,"topUp","province")
//          protable.createTempView("pro")
//
//          spark.sql("select proname from pro").rdd.map(a=>{
//              if(a.toString()==proname){
//                val unit: Unit = spark.sql(s"select numpro from pro where proname =$proname").rdd.toString.toInt+=1
//
//              }
//          })
//
//          mysql.write(proSum,"topUp","province")


        })

        // 将偏移量进行更新
        for (or<-offestRange){
          jedis.hset(groupId,or.topic+"-"+or.partition,or.untilOffset.toString)
        }



      jedis.set("orderSum",orderSum.toString)
      jedis.set("money",money.toString)
      jedis.set("Vorder",Vorder.toString)
      jedis.set("Forder",Forder.toString)



    })



    // 启动
    ssc.start()
    ssc.awaitTermination()
  }




}
