package weektest

import java.sql.{DriverManager, Statement}

import org.apache.spark.sql.{DataFrame, SparkSession}

object mysql {


  def read(spark:SparkSession,  mysqlname:String, tablename:String): DataFrame = {
    val frame: DataFrame = spark.read.format ( "jdbc" )
      .option ( "url", s"jdbc:mysql://localhost:3306/topUp?characterEncoding=UTF-8" )
      .option ( "dbtable", s"$tablename" )
      .option ( "user", "root" )
      .option ( "password", "123456" )
      .load ()
    frame
  }


  def write(df:DataFrame,mysqlname:String,tablename:String): Unit ={
    df.write.format("jdbc")
      .option("url", s"jdbc:mysql://localhost:3306/topUp?characterEncoding=UTF-8")
      .option("dbtable", s"$tablename")
      .option("user", "root")
      .option("password", "123456")
      .save()
  }

  def getStatement(): Statement ={
    val connection = DriverManager.getConnection ( "jdbc:mysql://localhost:3306/topUp?characterEncoding=UTF-8",
      "root", "123456" )
    val statement = connection.createStatement()
    statement
  }



}
