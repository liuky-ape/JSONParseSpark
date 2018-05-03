package com.dtwave.excute

import java.util.Properties

import org.apache.spark.sql.{DataFrame, Row, SparkSession}
import org.apache.spark.sql.functions._
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
    val df=spark.read.json("/midware/fs/wifi/20180428/muying.json")
  //    val df: DataFrame =spark.read.json("hdfs://localhost:8020/muying_bak.json")
   // df.printSchema()
    val outDF= df.select(df("id"),df("mmac"),df("rate"),df("time"),df("lat"),df("lon"))
    outDF.createOrReplaceTempView("outTable")
    //outDF.write.saveAsTable("out_table")
    val tempInner=df.select(explode(df("data"))).toDF("data")
    val innerDF=tempInner.select(
      "data.mac","data.rssi","data.rssi1", "data.rssi2", "data.rssi3","data.tmc","data.ts",
      "data.router", "data.range","data.tc","data.ds")
      .toDF("data_mac","data_rssi","data_rssi1", "data_rssi2", "data_rssi3","data_tmc",
        "data_ts", "data_router", "data_range","data_tc","data_ds")
    //innerDF.show()
    innerDF.createOrReplaceTempView("inTable")
    val result=innerDF.join(outDF).distinct()
//    val result=spark.sql(
//      """create table res
//        |as
//        |select
//        |outTable.id,inTable.data_mac,inTable.data_rssi,inTable.data_rssi1,inTable.data_rssi2,
//        |inTable.data_tmc,inTable.data_router,inTable.data_range,outTable.mmac,outTable.rate,
//        |outTable.time,outTable.lat,outTable.lon,inTable.data_ts,inTable.data_ds,inTable.data_rssi3,
//        |inTable.data_tc
//        |from inTable,outTable
//      """.stripMargin)
    result.createOrReplaceTempView("res")
   // spark.sql("select time from res").show()
    result.write.saveAsTable("res")
      spark.table("res").show(20)

    //读取临时表的数据，存到mysql对应表中
      val props=new Properties()
        val url = "jdbc:mysql://101.37.119.8:3306/data"
        props.put("user","data")
        props.put("pwd","data@gR8")
        result.write.jdbc(url,"wifi_data_muying",props)

  }



}
