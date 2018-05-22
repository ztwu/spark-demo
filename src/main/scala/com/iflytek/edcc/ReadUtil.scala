package com.iflytek.edcc

import org.apache.spark.SparkContext
import org.apache.spark.sql.{DataFrame, SQLContext}
/*
 * Created by ztwu2 on 2017/10/09
 */
object ReadUtil {

  def readDwsUcUserOrganization(sc:SparkContext) : DataFrame = {
    val sqlContext = new SQLContext(sc)
    import sqlContext.implicits._
    val dataFrame = sqlContext.read.parquet(Conf.inputpath1)
    dataFrame.show()
    return  dataFrame;
  }

  def readDwsLogUserActive(sc:SparkContext) : DataFrame = {
    val sqlContext = new SQLContext(sc)
    import sqlContext.implicits._
    val dataFrame = sqlContext.read.json(Conf.inputpath2)
    dataFrame.show()
    return dataFrame;
  }

  def readDwSchoolOrg(sc:SparkContext) : DataFrame = {
    val sqlContext = new SQLContext(sc)
    import sqlContext.implicits._
    val rdd = sc.textFile(Conf.inputpath3)
    val dataFrame = rdd.map(x=>{
      val temp = x.split(",")
      val schoolId = temp(0)
      val schoolName = temp(1)
      (schoolId,schoolName)
    }).toDF("school_id","school_name")
    dataFrame.show()
    return dataFrame
  }

  def readBroadcast(sc:SparkContext) : DataFrame = {
    val sqlContext = new SQLContext(sc)
    import sqlContext.implicits._
    val rdd = sc.textFile(Conf.cacheFilePath)
    val dataFrame = rdd.map(x=>{
      val temp = x.split(",")
      val districtId = temp(0).toString
      val districtName = temp(1).toString
      (districtId,districtName)
    }).toDF("district_id","district_name")
    dataFrame.show()
    return dataFrame
  }

}