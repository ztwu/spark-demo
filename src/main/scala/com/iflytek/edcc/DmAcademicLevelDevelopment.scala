package com.iflytek.edcc

import breeze.numerics.abs
import org.apache.spark.{SparkConf, SparkContext}

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

object DmAcademicLevelDevelopment {

  def main(args: Array[String]): Unit = {

    val conf = new SparkConf().setAppName(this.getClass.getName)
    //设置数据自动覆盖
    conf.set("spark.hadoop.validateOutputSpecs", "false")
    val sc = new SparkContext(conf)

    val beginDay = Util.getBeginDay(args)

    val path = Conf.dm_academic_level_development + beginDay

    val schoolData = ReadUtil.readDwUnionMultipleExamScoreDim(sc, beginDay, "5")
    val cityData = ReadUtil.readDwUnionMultipleExamScoreDim(sc, beginDay, "2")
    val schoolRog = ReadUtil.readDwSchoolOrg(sc, beginDay)

    val data = schoolData.join(schoolRog,schoolData("rd_id") === schoolRog("school_id"))
      .join(cityData,schoolRog("city_id") === cityData("rd_id") && cityData("exam_id") === schoolData("exam_id"))

    data.map(x => {
      val provinceId = x(9)
      val cityId = x(10)
      val districtId = x(11)
      val schoolId = x(12)
      val gradeCode = x(6)
      val gradeName = x(7)
      val examId = x(1)
      val examName = x(2)
      val examTime = x(5)
      val schoolAvg = Util.to_Default(x(8)).toDouble
      val cityAvg = Util.to_Default(x(21)).toDouble
      val excessAverageRate = abs(schoolAvg-cityAvg)/cityAvg
      Array(provinceId, cityId, districtId, schoolId, "1320", gradeCode, gradeName, examId, examName, examTime, excessAverageRate).mkString("\t")
    }).repartition(1).saveAsTextFile(path)

    sc.stop()

  }

}
