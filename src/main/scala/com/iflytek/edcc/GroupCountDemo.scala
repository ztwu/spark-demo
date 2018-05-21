package com.iflytek.edcc

import org.apache.spark.{SparkConf, SparkContext}
//使用窗口函数
import org.apache.spark.sql.expressions._
//引用函数
import org.apache.spark.sql.functions._

/**
  * Created with Intellij IDEA.
  * User: ztwu2
  * Date: 2017/10/19
  * Time: 13:52
  * Description
  */

object GroupCountDemo {

  def main(args: Array[String]): Unit = {

    val conf = new SparkConf()
    //设置程序名称
    conf.setAppName(this.getClass.getName)
    //设置数据自动覆盖
    conf.set("spark.hadoop.validateOutputSpecs", "false")
    //设置主节点，local本地线程
    conf.setMaster("local")

    val sc = new SparkContext(conf)
    val dataframe1 = ReadUtil.readDwSchoolOrg(sc);
    val dataframe2 = ReadUtil.readDwsLogUserActive(sc);
    val dataframe3 = ReadUtil.readDwsUcUserOrganization(sc);

    val data = dataframe3.join(dataframe1,dataframe3("school_id")===dataframe1("school_id"),"inner")
      .join(dataframe2,dataframe3("user_id")===dataframe2("user_id"),"left")
    data.cache()

    data.show()

    data.map(x=>{
      val provinceId = x(0).toString
      val cityId = x(1).toString
      val districtId = x(2).toString
      val schoolId = x(3).toString
      val userId = x(4).toString
      val schoolName = x(6).toString
      val event = Util.to_bg1(x(7))
      ((provinceId,cityId,districtId,schoolId,schoolName),Set(userId+"#"+event))
    }).reduceByKey(
      (x,y)=>{
        x.++ (y)
      }
    ).map(x=>{
      val key = x._1
      val value = x._2.size
      Array(key,value).mkString("\t")
    }).saveAsTextFile(Conf.outputpath)

    sc.stop()

  }

}
