package com.iflytek.edcc

/**
  * Created with Intellij IDEA.
  * User: ztwu2
  * Date: 2018/1/18
  * Time: 8:43
  * Description
  */

object ScalaTest {

  //这里可以解释为啥不同类型的字段关联join，为啥数据匹配奇怪
  def main(args: Array[String]): Unit = {
    val a1 = 49
    val a2 = "1"
    val a3 = 100000000

    println(a1.hashCode())
    println(a2.hashCode())
    println(a3.hashCode())

    // 由高位到低位
    val bb1 = (a1 >> 24) & 0xFF
    val bb2 = (a1 >> 16) & 0xFF
    val bb3 = (a1 >> 8) & 0xFF
    val bb4 = (a1) & 0xFF

    val b2 = a2.getBytes()

    println("a1字节数组:"+bb1+"_"+bb2+"_"+bb3+"_"+bb4)

    println("a2字节数组:")
    b2.foreach(println)

  }
}
