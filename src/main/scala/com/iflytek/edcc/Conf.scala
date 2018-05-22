package com.iflytek.edcc

/**
  * Created by ztwu2 on 2017/10/11.
  */
object Conf {

  //输入路径
  val inputpath1 = "data/project/edu_edcc/ztwu2/temp/mapreduce-data/dws_uc_user_organization";
  val inputpath2 = "data/project/edu_edcc/ztwu2/temp/mapreduce-data/dws_log_user_active";
  val inputpath3 = "data/project/edu_edcc/ztwu2/temp/mapreduce-data/dw_school_org";

  //广播文件
  val cacheFilePath = "data/project/edu_edcc/ztwu2/temp/mapreduce-data/distributecache/data.txt";

  //输出主路径
  val outputpath = "data/project/edu_edcc/ztwu2/temp/mapreduce-result";

  //检查点
  val checkpointDir = "data/project/edu_edcc/ztwu2/temp/checkpointdir";

}
