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
    sc.setCheckpointDir(Conf.checkpointDir)

    val dataframe1 = ReadUtil.readDwSchoolOrg(sc);
    val dataframe2 = ReadUtil.readDwsLogUserActive(sc);
    val dataframe3 = ReadUtil.readDwsUcUserOrganization(sc);
    val dataframe4 = ReadUtil.readBroadcast(sc);

    //action算子触发读取数据
    val broadcastData = sc.broadcast(dataframe4.collect())

    val data = dataframe3.join(dataframe1,dataframe3("school_id")===dataframe1("school_id"),"inner")
      .join(dataframe2,dataframe3("user_id")===dataframe2("user_id"),"left")
      //map side join
      .map(x=>{
        val districtId = x(2).toString
        var districtName = "null";
        for(value <- broadcastData.value){
          if(value(0).equals(districtId)){
            districtName = value(1).toString
          }
        }
        (x(0),x(1),x(2),x(3),x(4),x(5),x(6),x(7),x(8),districtName)
      })

    //缓存rdd数据，持久化存储到内存中
    //中间的计算结果通过cache或者persist放到内存或者磁盘中
    data.cache()

    //设置检查点
    //将DAG中比较重要的中间数据做一个检查点将结果存储到一个高可用的地方
    // (通常这个地方就是HDFS里面)
    data.checkpoint()

    data.foreach(x=>{println(x)})

    data.map(x=>{
      val provinceId = x._1.toString
      val cityId = x._2.toString
      val districtId = x._3.toString
      val districtName = x._10.toString
      val schoolId = x._4.toString
      val userId = x._5.toString
      val schoolName = x._7.toString
      val event = Util.to_bg1(x._8)
      ((provinceId,cityId,districtId,districtName,schoolId,schoolName),Set(userId+"#"+event))
    }).reduceByKey(
      (x,y)=>{
        x.++ (y)
      }
    ).map(x=>{
      val key = x._1.productIterator.toArray.mkString("\t")
      val value = x._2.size
      Array(key,value).mkString("\t")
    }).saveAsTextFile(Conf.outputpath)

    sc.stop()

  }

}
