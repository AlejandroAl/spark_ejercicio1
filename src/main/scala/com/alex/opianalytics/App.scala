package com.alex.opianalytics

import org.apache.spark.sql.{SparkSession, DataFrame}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.expressions.Window
import org.elasticsearch.spark._
import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.SQLContext._


object app {

  def mostrarTotalLosRegistros(df : DataFrame): Unit = {
    println("*************** Total de registros: "+df.count()+"************************")
  }

  def mostrarTotalCategorias(df: DataFrame, spark: SparkSession): Unit = {
    import spark.implicits._
    val rdd_ejer1 = df.rdd
    val rdd_categoria_count = rdd_ejer1.map( x => (x.getAs[String]("categoria"),1) ).reduceByKey(_+_)
    println("*************** Total de categorias: "+rdd_categoria_count.count()+"************************")
  }

  def mostrarTotalCadenasComerciales(df: DataFrame, spark: SparkSession): Unit = {
    import spark.implicits._
    val rdd_ejer1 = df.rdd
    val rdd_cadenaComercial_count = rdd_ejer1.map( x => (x.getAs[String]("cadenaComercial"),1) ).reduceByKey(_+_)
    println("*************** Total de cadenas comerciales: "+rdd_cadenaComercial_count.count()+"************************")
  }

  def mostrarProductosXEstado(df: DataFrame, spark: SparkSession): Unit = {
    import spark.implicits._
    val df_conteo_productos_Xestado = df.select($"producto",$"estado").groupBy($"estado",$"producto")
      .agg(count("producto").alias("Total_productos"))
    val w = Window.partitionBy("estado")
    println("****************************Productos mas monitoriados por estado *********************************")
    df_conteo_productos_Xestado.withColumn("max_total", max($"Total_productos").over(w)).where($"Total_productos" === $"max_total")
      .drop("Total_productos")
      .orderBy(desc("max_total"))
      .show()
  }

  def obtenerCadenaConMayorProductosVariados(df: DataFrame, spark: SparkSession): Unit = {
    import spark.implicits._
    println("****************************Top 5 cadenas con mayor varidad de productos *********************************")
    df.groupBy($"cadenaComercial").agg(countDistinct("producto").alias("Total_productos"))
      .orderBy(desc("Total_productos"))
      .show(5)
  }


  def guardaDatosAgrupadosES(df : DataFrame, spark :SparkSession, es_nodes:String = "localhost", es_port:String = "9200", indice: String ="ejercicio1" ): Unit = {
      import spark.implicits._
      val df_grouped = df.select($"producto",$"categoria",$"cadenaComercial",$"estado",$"municipio",$"latitud",$"longitud")
        .groupBy($"estado",$"cadenaComercial",$"categoria",$"producto").agg(count($"producto").alias("total_registros"))

      df_grouped.write
        .format("org.elasticsearch.spark.sql")
        .option("es.nodes.wan.only","true")
        .option("es.port",es_port)
        .option("es.nodes", es_nodes)
        .mode("Overwrite")
        .save(indice)
  }


  def main(args : Array[String]) {

    var csvPath = args(0)
    var csvPathTest = args(1)

    val spark = SparkSession.builder
      .master("local[*]")
      .appName("Ejercicio_1")
      .config("spark.driver.memory", "4g")
      .config("spark.executor.memory", "1g")
      .config("spark.executor.cores", "8")
      .config("spark.executor.memoryOverhead","0.10")
      .config("es.index.auto.create", "true")
      .getOrCreate()

    var df_ejer1 = spark.read.option("header", "true").option("inferSchema", "false").csv(csvPath)

//    mostrarTotalLosRegistros(df_ejer1)
//    guardaDatosAgrupadosES(df_ejer1,spark)
//    mostrarTotalCategorias(df_ejer1,spark)
//    mostrarTotalCadenasComerciales(df_ejer1,spark)
//    mostrarProductosXEstado(df_ejer1,spark)
    obtenerCadenaConMayorProductosVariados(df_ejer1,spark)





  }


}