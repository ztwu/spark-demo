package com.iflytek.edcc

import org.apache.spark.SparkContext
import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.hive.HiveContext
/*
 * Created by ztwu2 on 2017/10/09
 */
object ReadUtil {

  def readDwSchoolOrg(sc:SparkContext, day:String) = {

    val sqlContext = new SQLContext(sc)
    import sqlContext.implicits._

    val path = Util.getFilePath(Conf.dw_school_org,day)
    val datardd = sc.textFile(path)

    datardd.map(s => {
      val x = s.split("\t")
      val provinceId = Util.to_bg1(x(6))
      val cityId = Util.to_bg1(x(4))
      val districtId = Util.to_bg1(x(2))
      val schoolId = Util.to_bg1(x(0))
      (provinceId, cityId, districtId, schoolId)
    })
    .toDF("province_id","city_id","district_id","school_id")

  }

  def readDwUnionMultipleExamScoreDim(sc: SparkContext, day:String, randId:String) = {
    val sqlContext = new SQLContext(sc)
    import sqlContext.implicits._

    val begin = DateUtil.getSchoolYear(day, "1")
    val end = DateUtil.getSchoolYear(day, "2")

    val path = Util.getFilePath(Conf.dw_union_multiple_exam_score_dim,day)
    val dataframe = sqlContext.read.parquet(path)
    dataframe.registerTempTable("dw_union_multiple_exam_score_dim")

    val data = sqlContext.sql("select\n" +
      "\t\trd_id,\n" +
      "\t    exam_id,\n" +
      "\t    exam_name,\n" +
      "\t    exam_type_code,\n" +
      "\t    exam_type_name,\n" +
      "\t    exam_time,\n" +
      "\t    grade_code,\n" +
      "\t    grade_name,\n" +
      "\t    average\n" +
      "\tfrom dw_union_multiple_exam_score_dim\n" +
      s"\twhere range_id = '${randId}'\n" +
      s"\tand to_date(exam_time) >= '${begin}'\n" +
      s"\tand to_date(exam_time) < '${end}'")

     data.map(x => {
        val rdId = Util.to_bg1(x(0))
        val examId = Util.to_bg1(x(1))
        val examName = Util.to_bg1(x(2))
        val examTypeCode = Util.to_bg1(x(3))
        val examTypeName = Util.to_bg1(x(4))
        val examTime = Util.to_bg1(x(5))
        val gradeCode = Util.to_bg1(x(6))
        val gradeName = Util.to_bg1(x(7))
        val average = Util.to_bg1(x(8))
        (rdId, examId, examName, examTypeCode, examTypeName, examTime, gradeCode, gradeName, average)
      })
      .toDF("rd_id","exam_id","exam_name","exam_type_code","exam_type_name","exam_time","grade_code","grade_name","average")
  }

  def readDwUnionSingleExamScoreDim(sc: SparkContext, day:String, randId:String) = {
    val hiveContext = new HiveContext(sc)
    import hiveContext.implicits._

    val begin = DateUtil.getSchoolYear(day, "1")
    val end = DateUtil.getSchoolYear(day, "2")

    val data = hiveContext.sql("select\n" +
      "\t\trd_id,\n" +
      "\t    exam_id,\n" +
      "\t    exam_name,\n" +
      "\t    exam_type_code,\n" +
      "\t    exam_type_name,\n" +
      "\t    exam_time,\n" +
      "\t    grade_code,\n" +
      "\t    grade_name,\n" +
      "\t    subject_code,\n" +
      "\t    subject_name,\n" +
      "\t    single_average\n" +
      "\tfrom edu_edcc.dw_union_single_exam_score_dim\n" +
      s"\twhere pdate = '${day}'\n" +
      s"\tand range_id = '${randId}'\n" +
      s"\tand to_date(exam_time) >= '${begin}'\n" +
      s"\tand to_date(exam_time) < '${end}'")

    data.map(x => {
      val rdId = Util.to_bg1(x(0))
      val examId = Util.to_bg1(x(1))
      val examName = Util.to_bg1(x(2))
      val examTypeCode = Util.to_bg1(x(3))
      val examTypeName = Util.to_bg1(x(4))
      val examTime = Util.to_bg1(x(5))
      val gradeCode = Util.to_bg1(x(6))
      val gradeName = Util.to_bg1(x(7))
      val subjectCode = Util.to_bg1(x(8))
      val subjectName = Util.to_bg1(x(9))
      val singleAverage = Util.to_bg1(x(10))
      (rdId, examId, examName, examTypeCode, examTypeName, examTime, gradeCode, gradeName, subjectCode, subjectName, singleAverage)
    })
      .toDF("rd_id","exam_id","exam_name","exam_type_code","exam_type_name","exam_time","grade_code","grade_name", "subject_code", "subject_name" ,"single_average")
  }

}