package com.dtwave.excute

import org.apache.spark.sql.SparkSession

/**
  * Created by zcx on 2018/4/28.
  * desc:处理母婴wifi解析
  */
object dtwOdsWifiMotherBabyD {

  def main(args: Array[String]): Unit = {
    val spark = SparkSession //建立Spark session
      .builder()
      .master("yarn")
      .enableHiveSupport()
      .appName("spark-dtw-ods-wifi-monther-baby-d")
      .getOrCreate()

    //读取hdfs数据
    spark.read.json("/midware/fs/wifi/20180428/muying.json")

  }
}
