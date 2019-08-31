package com.hys.dbutil

import org.apache.spark.sql.{SparkSession, _}

/**
  * Oracle 工具类
  * Auther: hys 
  * Date: 2019/9/1
  * Time: 1:32
  * Package: com.hys.dbutil
  *
  */
class OracleUtil {
  /**
    *
    * @param sparkSession SparkSession
    * @param url 数据库Url
    * @param user 用户名
    * @param password 密码
    * @param tableName 表名称
    * @return 返回一个DataFrame
    */
  def getTableDF(sparkSession:SparkSession, url:String, user:String, password:String, tableName:String):DataFrame = {
    Class.forName("oracle.jdbc.driver.OracleDriver")

    val jdbcDF = sparkSession.read.format("jdbc").options(Map("url" -> url,
      "user" -> user,
      "password" -> password,
      "dbtable" -> tableName)).load()
    jdbcDF.createOrReplaceTempView("table1")
    val df:DataFrame = sparkSession.sql("select * from table1")
    df.show
    df
  }

}

object OracleUtil{
  def main(args:Array[String]):Unit = {
    val spark = new SparkSession
    .Builder()
      .appName("OracleUtilTest")
      .master("local")
      .getOrCreate()
    val oraclTool = new OracleUtil
    oraclTool.getTableDF(spark, "jdbc:oracle:thin:test/test@//192.168.10.10:1521/orcl", "taotao", "taotao", "item").show()
  }


}
