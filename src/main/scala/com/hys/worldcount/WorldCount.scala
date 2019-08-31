package com.hys.worldcount

import org.apache.spark.{SparkConf, SparkContext}

/**
  *
  * Auther: hys
  * Date:2019/8/31
  * Time: 18:43
  * Package: com.hys.worldcount
  *
  */
class WorldCount {

  def worldCountFun: Unit ={
    val conf = new SparkConf()
      .setAppName("WordCount")
      .setMaster("local")
    val sc = new SparkContext(conf)
    val lines = sc.textFile("E:\\files\\LearnFiles\\Spark\\filesCreateByMyself\\worldCountTest.txt")
    val words = lines.flatMap(x => x.split(""))
    val pairs = words.map { word => (word, 1) }
    val worldCounts = pairs.reduceByKey(_ + _)
    worldCounts.foreach(worldCount => println(worldCount._1 + " appeared :" + worldCount._2 + " times"))
  }

}

object WorldCount{

  def main(args: Array[String]): Unit = {
    val wc = new WorldCount
    wc.worldCountFun

  }

}
