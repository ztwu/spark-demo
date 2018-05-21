package com.iflytek.edcc

/**
  * 本包中的公用方法类
 * Created by cyzhang5 on 2016-12-13.
 */
object Util {

  /**
   * 获取输出路径
    *
    * @param prefixPath 部分文件路径
   * @param subPath
   * @return 集合个数的路径用逗号分隔
   */
  def getoutPath(prefixPath: String, subPath:String) = {

    "/project/"+prefixPath+"/"+prefixPath+subPath

  }

  /**
   * 获取文件夹列表
    *
    * @param subPath 部分文件路径(未连接时间的路径)
   * @param months 月的集合()
   * @return 集合个数的路径用逗号分隔
   */
  def getFilePath(subPath: String, months: Array[String]) = {
    subPath + months.mkString("/," + subPath) + "/"
  }

  /**
    * 获取文件路径_day
    *
    * @param subpath 路径
    * @param time 即时间,格式为"yyyy-MM-dd"或"yyyy-MM"或其他part等于的值
    * @return 完整路径
    */
  def getFilePath(subpath: String, time: String): String = {
    subpath + time + "/"
  }

  /**
    * 得到数据库,默认的数据库是zhixue_dev，可以通过args[1]来取代
    *
    * @param args
    */
  def getValuePath(args: Array[String]) ={
    var db="edu_edcc"
    if (args.length > 0 && args(1)!="") {
      db = args(1)
    }
    val value_path = "/project/"+db+"/"+db+"/result/"
    value_path
  }

  /**
    * 得到数据库,默认的数据库是zhixue_dev，可以通过args[1]来取代
    *
    * @param args
    */
  def getMainPath(args: Array[String]) ={
    var db="zx_dev"
    if (args.length > 0 && args(1)!="") {
      db = args(1)
    }
    val main_path = "/project/"+db+"/"+db+"/db/"
    main_path
  }

  /**
    *截止昨日的时间筛选
    *
    * @param examTime 格式为"yyyy-MM-dd hh:mm:ss"
    * @param beginDay 计算时间
    * @return Boolan
    */
  def getBooleonByTime(examTime:String,beginDay:String):Boolean={
    val time=examTime.split(" ")(0)
    (time!="" && time.compareTo(beginDay)<=0)
  }

  /**
    *当前月时间筛选
    *
    * @param examTime 格式为"yyyy-MM-dd hh:mm:ss"
    * @param beginDay 计算时间
    * @return Boolan
    */
  def getBooleonByMonth(examTime:String,beginDay:String):Boolean={
    val time=examTime.split(" ")(0)
    val startTime=beginDay.substring(0,8)+"01"
    (time!="" && time.compareTo(beginDay)<=0&&time.compareTo(startTime)>=0)
  }

  /**
    *当前符合当前计算时间的记录
    *
    * @param activeTime 格式为"yyyy-MM-dd hh:mm:ss"
    * @param day 计算时间
    * @return Boolan
    */
  def getBooleonByTime(activeTime:Any,day:String):Boolean={
    if(activeTime!=null){
      val time=activeTime.toString.split(" ")(0)
      (activeTime!="" && time.compareTo(day)<=0)
    }else {
      false
    }
  }

  /**
    * 转换成默认值bg-1
    *
    * @param x
    */
  def to_bg1(x:Any):String ={
    if(x !=null){
      val str_x=x.toString.trim
      if("".equals(str_x) || str_x.equalsIgnoreCase("null")) "bg-1" else str_x
    }else{
      "bg-1"
    }
  }

  /**
    * 判断字符串是否是纯数字
    *
    * @param s "09,08" ","用替换成""=>0908
    * @return 是：true 否：false
    */
  def isIntByRegex(s : String) = {
    val a=s.replace(",","")
    val pattern = """^(\d+)$""".r
    a match {
      case pattern(_*) => true
      case _ => false
    }
  }

  /**
    * 获取昨日时间
    *
    * @param args 主函数接收的参数
    * @return beginDay 即昨日的时间,格式为"yyyy-MM-dd"
    */
  def getBeginDay(args: Array[String]): String = {
    var beginDay = DateUtil.getYesterday
    if (args.length > 0 && args(0).length == 10) {
      beginDay = args(0)
    }
    beginDay
  }

  /**
    * 统计值转换成默认值0
    *
    * @param x
    */
  def to_Default(x:Any):String ={
    if(x !=null){
      val str_x=x.toString.trim
      if("".equals(str_x) || str_x.equalsIgnoreCase("null")) "0" else str_x
    }else{
      "0"
    }
  }

}