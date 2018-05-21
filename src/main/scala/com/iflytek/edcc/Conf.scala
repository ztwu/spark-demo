package com.iflytek.edcc

/**
  * Created by ztwu2 on 2017/10/11.
  */
object Conf {

  //输入路径
  val dw_school_org = "hdfs://ns-bj/project/zx_dev/zx_dev/db/dw_school_org/part="
  val dw_union_multiple_exam_score_dim = "hdfs://ns-bj/user/hive/warehouse/edu_edcc.db/dw_union_multiple_exam_score_dim/pdate="
  val dw_union_single_exam_score_dim = "hdfs://ns-bj/user/hive/warehouse/edu_edcc.db/dw_union_single_exam_score_dim/pdate="

  //输出主路径
  var dm_academic_level_development = "/project/edu_edcc/ztwu2/db/dm/dm_academic_level_development/pdate="
  var dm_academic_setback_index = "/project/edu_edcc/ztwu2/db/dm/dm_academic_setback_index/pdate="

}
