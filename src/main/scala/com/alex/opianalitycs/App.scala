package com.alex.opianalitycs

import org.apache.spark.sql.SparkSession

/**
 * @author ${user.name}
 */
object App {
  
  def foo(x : Array[String]) = x.foldLeft("")((a,b) => a+" "+b)
  
  def main(args : Array[String]) {

    val spark = SparkSession.builder
      .master("local[*]")
      .appName("Ejercicio_1")
      .config("sspark.driver.cores", "3")
      .config("spark.driver.memory","3g")
      .config("spark.executor.memory","2g")
      .getOrCreate()

    println( spark.version )
    var argss = Array[String]("Hola","Alex","Almaraz")
    println("concat arguments = " + foo(argss))
  }

}
