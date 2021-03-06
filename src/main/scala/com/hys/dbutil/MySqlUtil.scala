package com.hys.dbutil

import org.apache.spark.sql.{DataFrame, Dataset, SparkSession}



/**
  * Spark的MySql工具类
  * Auther: hys 
  * Date: 2019/9/1
  * Time: 1:05
  * Package: com.hys.dbutil
  *
  */
class MySqlUtil {
  /**
    *
    * @param sparkSession SparkSession 对象
    * @param url 数据库url
    * @param user 用户名
    * @param password 密码
    * @param tableName 表名称
    * @return 返回一个DataFrame
    */
  def getTableDF(sparkSession:SparkSession, url:String, user:String, password:String, tableName:String):DataFrame = {
    Class.forName("oracle.jdbc.driver.OracleDriver")
    val frame: DataFrame = sparkSession.read.format("jdbc")
      .option("url", url)
      .option("user", user)
      .option("password", password)
      .option("dbtable", tableName)
      .option("driver", "com.mysql.jdbc.Driver")
      .load()
    frame
  }

/*  def getTableDS(sparkSession:SparkSession, url:String, user:String, password:String, tableName:String):Dataset[Any] = {
    Class.forName("oracle.jdbc.driver.OracleDriver")
    val df: DataFrame = sparkSession.read.format("jdbc")
      .option("url", url)
      .option("user", user)
      .option("password", password)
      .option("dbtable", tableName)
      .option("driver", "com.mysql.jdbc.Driver")
      .load()
//    case class Item(id:Long, title:String, sell_point:String,price:Long, num:Long, barcode:Long, image:String, cid:Long, status:String, created:String, updated:String)
    val ds = df.as[Any]
    ds
  }*/

}

object MySqlUtil{
  def main(args: Array[String]):Unit = {
    val spark = SparkSession
      .builder()
      .appName("MySqlUtilsTest")
      .master("local")
      .getOrCreate()

    val mysqlTools = new MySqlUtil
    val tb_user = mysqlTools.getTableDF(spark,"jdbc:mysql://192.168.10.20:3306/taotao","taotao","taotao","tb_item")
    tb_user.show()
    println("*********************")

//    val tb_user_ds = mysqlTools.getTableDS(spark,"jdbc:mysql://192.168.10.20:3306/taotao","taotao","taotao","tb_user")
//    tb_user_ds.show()
    spark.close()
  }
}


