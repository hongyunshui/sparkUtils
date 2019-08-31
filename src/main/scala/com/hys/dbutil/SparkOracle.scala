package com.hys.dbutil

//import org.apache.spark.SparkConf
//import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.SparkSession

/**
  * Created by Administrator on 2018/8/21.
  */
class SparkOracle{
  def getOracleDF:Unit = {
    Class.forName("oracle.jdbc.driver.OracleDriver")
    //创建url字符串
    val url = "jdbc:oracle:thin:test/test@//192.168.10.10:1521/orcl"
    //创建SparkSession
    val spark = SparkSession
      .builder()
      .appName("oracle_spark")
      .master("local[*]")
      .getOrCreate()

    val jdbcDF = spark.read.format("jdbc").options(Map("url" -> url,
      "user" -> "taotao",
      "password" -> "taotao",
      "dbtable" -> "TT_USER")).load()
    jdbcDF.createOrReplaceTempView("table1")
    spark.sql( " select * from table1 ").show(100)
  }
}
object SparkOracle {

  def main(args: Array[String]): Unit = {

  }
}