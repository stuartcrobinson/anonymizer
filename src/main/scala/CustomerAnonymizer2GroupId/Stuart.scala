package CustomerAnonymizer2GroupId


import java.io.File
import java.nio.file.{Files, Paths}

import com.databricks.spark.avro._
import org.apache.commons.io.FileUtils
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{DataFrame, SparkSession}

import scala.collection.JavaConversions._
//
//
//import java.io.File
//import java.nio.file.{Files, Paths}
//import scala.collection.JavaConversions._
//
//import com.databricks.spark.avro._
//import org.apache.commons.io.FileUtils
//import org.apache.spark.sql.functions._
//import org.apache.spark.sql.{DataFrame, SparkSession}

/*

NOTES - parquet anonymizing tool complete! per input file.  clean up a bit to make more usable.  turn into function with inputs: input and output file names

TODO - do same for avro
 */

/**
  * Application entry point object.
  */
object Stuart {

  final val SPARK_APP_NAME = "StuartBrowseOrderAnonymizer"

  //how can i combine these?
  def anonymizeLong: (Long => Int) = _.toString.hashCode

  def anonymizeString: (String => Int) = _.toString.hashCode

  //  def anonymize: (Long => Int) = _.toString.hashCode


  //  def anonymize(value: Object): Int ={
  //    value.toString.hashCode
  //  }

  def displayParquetFiles(spark: SparkSession) = {

    val parquetFiles = new java.io.File("resources/flatfiles").listFiles.filter(_.getName.endsWith(".parquet"))

    //    parquetFiles.foreach {
    //      println
    //    }

    println("")

    for (file <- parquetFiles) {
      println(file)
      val table = spark.read.load(file.toString)
      table.printSchema()

      val updatedDf = table.withColumn("contact_id", udf(anonymizeLong).apply(col("contact_id")))

      table.show()
      updatedDf.show()
      updatedDf.printSchema()
    }

  }

  def displayAvroFiles(spark: SparkSession) = {

    println("\n ---------- AVRO --------------\n")

    val avroFiles = new java.io.File("resources/flatfiles").listFiles.filter(_.getName.endsWith(".avro"))

    //    avroFiles.foreach {
    //      println
    //    }

    println("")

    for (file <- avroFiles) {
      println(file)
      val table = spark.read.avro(file.toString)
      table.printSchema()

      val updatedDf = table.withColumn("contact_id", udf(anonymizeLong).apply(col("contact_id")))

      table.show()
      //      updatedDf.show()
      //      updatedDf.printSchema()
    }

  }

  def anonymizeCols(frame: DataFrame, colNames: String*): DataFrame = {
    var frameVar = frame //what is the right way to do this in scala? a more functional way?
    frameVar.show()

    for (colName <- colNames) {

      if (colName.equals("contact_id")) {
        frameVar = frameVar.withColumn(colName, udf(anonymizeLong).apply(col(colName)))
      }
      else if (colName.equals("customer_id")) {
        frameVar = frameVar.withColumn(colName, udf(anonymizeString).apply(col(colName)))
      }
      else {
        throw new RuntimeException("unrecognized column name.  only contact_id and customer_id accepted cos i can't figure out how to have a udf accept generic Object-type parameter")
      }

      println(colName)
      frameVar.show()

    }
    frameVar
  }


  def getDf(spark: SparkSession, file: File) = {

    if (file.getName.endsWith(".avro")) {
      spark.read.avro(file.toString)
    }
    else if (file.getName.endsWith(".parquet")) {
      spark.read.load(file.toString)
    }
    //    else if (file.getName.endsWith(".csv")) {
    ////      spark.sql("SELECT * FROM csv.`" + file.toString + "`")
    //
    //      spark.read.option("header", true).csv(file.toString)
    //
    ////      spark.read
    ////        .format("csv")
    ////        .option("header", "true") //reading the headers
    ////        .option("mode", "DROPMALFORMED")
    ////        .load(file.toString)
    //
    //    }
    else {
      throw new RuntimeException("wrong file type.  only parquet and avro and csv allowed for reading into DataFrame.")
    }
  }

  def anonymizeCustomerIdentifiers(spark: SparkSession, file: File): DataFrame = {
    //    var output :DataFrame = null  //what's a better way to do this

    val df = getDf(spark, file)

    if (file.getName.endsWith(".avro")) {
      anonymizeCols(df, "contact_id", "customer_id")
    }
    else if (file.getName.endsWith(".parquet")) {
      anonymizeCols(df, "contact_id")
    }
    else {
      throw new RuntimeException("wrong file type.  only parquet and avro allowed.")
    }
  }

  def writeAnonymizedCopyOfFile(spark: SparkSession, inputFilePath: String, outputDir: String) = {
    val outputFileName = inputFilePath.replaceAll("/", "_")
    val outputFilePath = Paths.get(outputDir).resolve(outputFileName)


    if (inputFilePath.endsWith(".csv")) {
      //no customer data in csv files - these are products snapshots
      Files.copy(Paths.get(inputFilePath), outputFilePath)
    }
    else {
      val anonymizedDf = anonymizeCustomerIdentifiers(spark, new File(inputFilePath))

      if (inputFilePath.endsWith(".parquet")) {
        anonymizedDf.write.parquet(outputFilePath.toString)
      }
      else if (inputFilePath.endsWith(".avro")) {
        println("HEREROIEWORIWJEOJEWFKJWELF " + inputFilePath.toString)
        anonymizedDf.show()
        anonymizedDf.write.avro(outputFilePath.toString)
      }
      else {
        throw new RuntimeException("invalid file type while writing anonymized copies")
      }
    }
  }

  /** Main entry point  */
  def main(args: Array[String]) {
    org.apache.log4j.Logger.getLogger("org").setLevel(org.apache.log4j.Level.OFF)

    println("hello")
    val spark = SparkSession.builder.master("local[2]").appName("SUnderstandingSparkSession").getOrCreate()
    //    spark.read.json("resources/people.json").show()


    val outputDir = new File("output")
    outputDir.mkdirs()


    // configuration to use deflate compression
    spark.conf.set("spark.sql.avro.compression.codec", "deflate")
    spark.conf.set("spark.sql.avro.deflate.level", "5")

    spark.read.avro("resources/flatfiles/mesosmaster-stg-003_browse-data_processed_2017-09-27_site_id=253445_part-r-00000-9f3f8178-2fb8-4cba-ae34-3ea21725506e.avro")
      .show()
    spark.read.avro("output/_Users_stuart.robinson_repos_ml_CustomerAnonymizer2_resources_flatfiles_mesosmaster-stg-003_browse-data_processed_2017-09-27_site_id=253445_part-r-00000-9f3f8178-2fb8-4cba-ae34-3ea21725506e.avro/part-00000-3bfd36ab-e4f2-4037-b951-fa932b5faafb.avro")
      .show()


        val d4 = spark.read.avro("resources/flatfiles/mesosmaster-stg-003_browse-data_processed_2017-09-27_site_id=253482_part-r-00000-9f3f8178-2fb8-4cba-ae34-3ea21725506e.avro")
        d4.show()


        val d5 = spark.read.avro("output/_Users_stuart.robinson_repos_ml_CustomerAnonymizer2_resources_flatfiles_mesosmaster-stg-003_browse-data_processed_2017-09-27_site_id=253482_part-r-00000-9f3f8178-2fb8-4cba-ae34-3ea21725506e.avro/part-00000-4920a084-059a-4310-a191-710a0587c8f7.avro")
        d5.show()
//
    System.exit(0)
    //    val d3 = spark.read.parquet("output/asdfasdf.parquet/part-00000-3eec51b7-83bf-415f-a95f-4d63fe9eae53-c000.snappy.parquet")
    //    d3.show()
    //
    //    val df2 = spark.read.parquet("resources/flatfiles/hbase-stg-005_product_reconciled-orders-export_site_id=253445_part-r-00000-2324f0be-884b-4db2-b2a4-8f1425c59929.snappy.parquet")
    //    df2.show()
    //    df2.write.parquet("output/awegargaefawe.parquet")
    //
    //
    //    val df = spark.read.avro("resources/flatfiles/mesosmaster-stg-003_browse-data_processed_2017-09-27_site_id=253445_part-r-00000-9f3f8178-2fb8-4cba-ae34-3ea21725506e.avro")
    //    df.show()
    //    df.printSchema()
    //    df.write.parquet("output/asdfasdf.parquet")
    //    df.write.avro("output/asdfasdf.avro")


    // writes out compressed Avro records

    //
    //
    //
    //
    for (file <- new File("resources/flatfiles").listFiles) {
      println("processing " + file.toString)
      writeAnonymizedCopyOfFile(spark, file.getAbsolutePath, outputDir.getAbsolutePath)
    }

//    FileUtils.listFiles(outputDir, null, true).foreach(file => {
//      println(file)
//    })
//    println("end of all files")

    //
    FileUtils.listFiles(outputDir, Array("parquet", "avro"), true).foreach(file => {
      println(file)
      getDf(spark, file).show()
    })
    FileUtils.listFiles(outputDir, Array("csv"), true).foreach(file => {
      println(file)
      Files.readAllLines(file.toPath).subList(0, 10).foreach(println)
      println()
    })

    //    for (file <- files2) {
    //      println(file)
    //    }
    spark.stop()
  }
}


//    Files.f

//    java.nio.file.Files.walk(outputDir.toPath).iterator().asScala.filter(Files.isRegularFile(_)).foreach(println)

//    //filter(_.getName.endsWith(".parquet"))
//    Files.walk(outputDir.toPath)
//      .filter(_.getName.endsWith(".parquet"))
//      .forEach(System.out::println);


//        displayParquetFiles(spark)
//    displayAvroFiles(spark)

//    val anAvroFile = new File("resources/flatfiles/mesosmaster-stg-003_browse-data_processed_2017-09-27_site_id=253445_part-r-00000-9f3f8178-2fb8-4cba-ae34-3ea21725506e.avro")
//    val aParquetFile = new File("resources/flatfiles/hbase-stg-005_product_reconciled-orders-export_site_id=253445_part-r-00001-2324f0be-884b-4db2-b2a4-8f1425c59929.snappy.parquet")
//
//    val df1 = anonymizeCustomerIdentifiers(spark, anAvroFile)
//    //    df1.show()
//
//    val outputFileName1 = anAvroFile.getAbsolutePath.replaceAll("/", "_")
//    println(outputFileName1)
//
//    val df2 = anonymizeCustomerIdentifiers(spark, aParquetFile)
//    //    df2.show()
//
//    val inputFilePath = "asdf"
//    val outputDir = "asdf"
//
//    writeAnonymizedCopyOfFile(spark, inputFilePath, outputDir)
//
//    val outputFileName2 = aParquetFile.getAbsolutePath.replaceAll("/", "_")
//    println(outputFileName2)