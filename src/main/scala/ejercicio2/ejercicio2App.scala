package ejercicio2

import org.apache.spark.sql.{SparkSession, SaveMode, DataFrame}
import org.apache.spark.sql.functions._

object app {

  def generaDiccionario(df: DataFrame, spark: SparkSession, pathDiccionario: String): Unit = {
    import spark.implicits._
    val df_ejercicio_grouped = df.groupBy($"country").agg(countDistinct("country")).where($"country".isNotNull )

    def getID = (country: String) => {
      val r = scala.util.Random
      val cleancountry = country.replaceAll("[0-9()]", "x").trim
      var splitName = cleancountry.split(" ")
      val len = splitName.length



      if ( len > 1) {
        try {
          (splitName.map(_.charAt(0)).foldLeft("")(_+_).toArray.mkString("") + (r.nextFloat*1000).toString.substring(0,3)).toUpperCase
        } catch { case x: Exception => splitName(0) }

      } else if (len == 1 ){
        try {
          (splitName(0).charAt(0).toString + splitName(0).charAt(1).toString).toUpperCase + (r.nextFloat*1000).toString.substring(0,3)
        } catch { case x: Exception => splitName(0).charAt(0).toString.toUpperCase + (r.nextFloat*1000).toString.substring(0,3) }

      } else {
        (country.charAt(0).toString + country.charAt(1).toString).toUpperCase + (r.nextFloat*1000).toString.substring(0,3)
      }

    }

    val get_id_country = spark.udf.register("get_id_country",getID)

    val df_diccionario_country = df_ejercicio_grouped.select("country").withColumn("country_id",get_id_country(col("country")))

    df_diccionario_country.show(750)

    df_diccionario_country.groupBy($"country_id").agg(count("country_id").alias("count")).where($"count" > 1).show(750)

    df_diccionario_country.coalesce(1).write.mode(SaveMode.Overwrite).option("header","true").csv(pathDiccionario)
  }

  def main(args : Array[String]) {

    var csvPath = args(0)
    var pathDiccionario = args(1)

    val spark = SparkSession.builder
      .master("local[*]")
      .appName("Ejercicio_1")
      .config("spark.driver.memory", "4g")
      .config("spark.executor.memory", "1g")
      .config("spark.executor.cores", "8")
      .config("spark.executor.memoryOverhead","0.10")
      .config("es.index.auto.create", "true")
      .getOrCreate()
    import spark.implicits._

    def trimColumn = (columnatext: String) => {
      try {
        columnatext.trim
      } catch { case x: Exception => columnatext}

    }

    val trimColumna = spark.udf.register("trimColumna",trimColumn)

    val df_ejercicio2 = spark.read.option("header","true").option("inferSchema","true").csv(csvPath)

    val df_trim_ejercicio2 = df_ejercicio2.withColumn("country", trimColumna($"country"))

    println("*********************************Total de registros actuales: "+df_ejercicio2.count())

    df_trim_ejercicio2.show()

//    generaDiccionario(df_ejercicio2,spark)

    val df_diccionario = spark.read.option("header","true").option("inferSchema","true").csv(pathDiccionario)

    println("*********************************Total de registros diccionario: "+df_diccionario.count())

    val df_joined = df_trim_ejercicio2.join(df_diccionario,df_trim_ejercicio2.col("country") === df_diccionario.col("country"),"left")

    println("*********************************Total de registros joined: "+df_joined.count())

    println("*********************************Total de registros joined: "+df_joined.count())

    println("*********************************Total de registros country null: "+df_joined.where($"country_id".isNull).count())

    df_joined.where($"country_id".isNull).show()

  }


}