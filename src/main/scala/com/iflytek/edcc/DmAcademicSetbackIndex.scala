package com.iflytek.edcc

import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.mutable.ArrayBuffer

//使用窗口函数
import org.apache.spark.sql.expressions._
//引用函数
import org.apache.spark.sql.functions._

/**
  * Created by admin on 2017/10/19.
  */

/**
  * Created with Intellij IDEA.
  * User: ztwu2
  * Date: 2017/10/19
  * Time: 13:52
  * Description
  */

object DmAcademicSetbackIndex {

  def main(args: Array[String]): Unit = {

    val conf = new SparkConf().setAppName(this.getClass.getName)
    //设置数据自动覆盖
    conf.set("spark.hadoop.validateOutputSpecs", "false")
    val sc = new SparkContext(conf)

    val beginDay = Util.getBeginDay(args)

    val path = Conf.dm_academic_setback_index + beginDay

    val schoolData = ReadUtil.readDwUnionSingleExamScoreDim(sc, beginDay, "5")
    val cityData = ReadUtil.readDwUnionSingleExamScoreDim(sc, beginDay, "2")
    val schoolRog = ReadUtil.readDwSchoolOrg(sc, beginDay)

    // dataframe的开窗函数
    val window = Window.partitionBy("exam_id","grade_code","subject_code").orderBy(schoolData("single_average").desc)
    val newSchoolData = schoolData.withColumn("rank",rowNumber().over(window))

    newSchoolData.join(schoolRog,newSchoolData("rd_id") === schoolRog("school_id"))
      .map(x => {
        val provinceId = x(12).toString
        val cityId= x(13).toString
        val districtId = x(14).toString
        val schoolId = x(0).toString
        val examId = x(1).toString
        val examName = x(2).toString
        val examTime = x(5).toString
        val gradeCode = x(6).toString
        val gradeName = x(7).toString
        val subjectCode = x(8).toString
        val subjectName = x(9).toString
        val rank = x(11).toString
        val tempKey = examId + "#" + examName + "#" + rank
        ((provinceId,cityId,districtId,schoolId,gradeCode,gradeName,subjectCode,subjectName),Map(tempKey->examTime))
    })
      //处理多对一，多对多
      .reduceByKey({case (a,b) => {
      //集合累加
      //x::list等价于list.::(x)),元素添加到列表
      //listA++listB等价于listA.++(listB)，2个列表相连
//      a.++(b)
      a ++ b
    }})
      //flatmap,操作一个元素返回一个可迭代类型的数据集合
    .flatMap(x => {
      val key = x._1

      val list = x._2.toList.sortBy(_._2)
      var valueTemp = ArrayBuffer[(String,String)]()
      for(i<-0 to list.length-1) {
        val temp = list(i)

        if(i != 0){
          val tempPre = list(i-1)
          val arrayPre = tempPre._1.split("#")
          val rankPre = arrayPre(2)

          val temp = list(i)
          val array = temp._1.split("#")
          val rank = array(2)
          val examId = array(0)
          val examName = array(1)
          val examTime = temp._2

          val rankIndex = rank.toInt - rankPre.toInt
          valueTemp += ((key._1+"\t"+key._2+"\t"+key._3+"\t"+key._4+"\t"+"1320"+"\t"+key._5+"\t"+key._6+"\t"+key._7+"\t"+key._8,examId + "\t" + examName + "\t" + examTime + "\t" + rankIndex))
        }else {
          val temp = list(0)
          val array = temp._1.split("#")
          val rank = array(2)
          val examId = array(0)
          val examName = array(1)
          val examTime = temp._2
          val rankIndex = 0
          valueTemp += ((key._1+"\t"+key._2+"\t"+key._3+"\t"+key._4+"\t"+"1320"+"\t"+key._5+"\t"+key._6+"\t"+key._7+"\t"+key._8,examId + "\t" + examName + "\t" + examTime + "\t" + rankIndex))
        }
      }
      valueTemp
    })
    .map(x => {
      val key = x._1
      val value = x._2
      Array(key,value).mkString("\t")
    })
      .repartition(1).saveAsTextFile(path)

    sc.stop()

  }

}
