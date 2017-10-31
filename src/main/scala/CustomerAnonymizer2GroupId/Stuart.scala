package CustomerAnonymizer2GroupId


import java.io.File
import java.nio.file.{Files, Paths}

import com.databricks.spark.avro._
import org.apache.commons.io.FileUtils
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{DataFrame, SparkSession}

import scala.collection.JavaConversions._

/**
  * customer_id is browser session stuff.  may or may not have contact id per browse data.
  *
  * question - what is best way to get image urls from orders tables in mysql?  --- eh just get from bing
  * pql:
  * service='order' and role='read'> select * from line_items limit 30;
  * *
  *
  * convert timestamp to yyyy-mm-dd-1,yyyy-mm-dd-2... based on order of event during that day..... :(  any other way?
  *
  * examples:
  * resources/flatfiles/hbase-stg-005_product_reconciled-orders-export_site_id=253482_part-r-00001-2324f0be-884b-4db2-b2a4-8f1425c59929.snappy.parquet
  * resources/flatfiles/mesosmaster-stg-003_browse-data_processed_2017-09-27_site_id=253482_part-r-00000-9f3f8178-2fb8-4cba-ae34-3ea21725506e.avro
  * resources/flatfiles/hbase-stg-005_product_snapshots_253482_11329.csv
  *
  */

/** see def main */
object Stuart {

  final val SPARK_APP_NAME = "StuartBrowseOrderAnonymizer"

  //TODO how can i combine these?
  def anonymizeLong: (Long => Int) = _.toString.hashCode

  //  def anonymizeLong: (Any => Int) = _.toString.hashCode

  def anonymizeString: (String => Int) = _.toString.hashCode

  //  def anonymize: (Long => Int) = _.toString.hashCode


  //  def anonymize(value: Object): Int ={
  //    value.toString.hashCode
  //  }

  def displayParquetFiles(spark: SparkSession) = {

    val parquetFiles = new java.io.File("resources/flatfiles").listFiles.filter(_.getName.endsWith(".parquet"))

    println("")

    for (file <- parquetFiles) {
      println(file)
      val table = spark.read.load(file.toString)
      table.printSchema()

      val updatedDf = table.withColumn("contact_id", hash(col("contact_id")))


      table.show()
      updatedDf.show()
      updatedDf.printSchema()
    }

  }

  def displayAvroFiles(spark: SparkSession) = {

    println("\n ---------- AVRO --------------\n")

    val avroFiles = new java.io.File("resources/flatfiles").listFiles.filter(_.getName.endsWith(".avro"))

    println("")

    for (file <- avroFiles) {
      println(file)
      val table = spark.read.avro(file.toString)
      table.printSchema()

      val updatedDf = table.withColumn("contact_id", udf(anonymizeLong).apply(col("contact_id")))

      table.show()
    }

  }

  def anonymizeCols(frame: DataFrame, colNames: String*): DataFrame = {
    var frameVar = frame //TODO what is the right way to do this in scala? a more functional way?
    //    frameVar.show()

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

      //      println(colName)
      //      frameVar.show()

    }
    frameVar
  }


  def getDf(spark: SparkSession, file: File) = {
    if (file.getName.endsWith(".avro")) {
      spark.read.avro(file.toString)
    }
    else if (file.getName.endsWith(".csv")) {
      spark.read
        .format("csv")
        .option("header", "true") //reading the headers
        .load(file.toString)
    }
    else if (file.getName.endsWith(".parquet")) {
      spark.read.load(file.toString)
    }
    else {
      throw new RuntimeException("wrong file type.  only parquet and avro and csv allowed for reading into DataFrame.")
    }
  }

  def anonymizeCustomerIdentifiers(spark: SparkSession, file: File): DataFrame = {
    //    var output :DataFrame = null  //TODO what's a better way to do this?

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


  def writeAnonymizedCopyOfFilePrecise(spark: SparkSession, inputFilePath: String, outputDir: String) = {
    val outputFileName = inputFilePath.replaceAll("/", "_")
    val outputFilePath = Paths.get(outputDir).resolve(outputFileName)

    if (inputFilePath.endsWith(".parquet") || inputFilePath.endsWith(".avro") || inputFilePath.endsWith(".csv")) {

      var df = getDf(spark, new File(inputFilePath))

      if (doPrintlns) {
        println(inputFilePath)
        df.show(10)
      }

      if (inputFilePath.contains("product_snapshots") && inputFilePath.endsWith(".csv")) {

        df.columns
          .filterNot(Seq("product_id", "title", "description").contains)
          .foreach(c => df = df.drop(c))

        df.write.csv(outputFilePath.toString)
      }
      else if (inputFilePath.contains("product_reconciled-orders") && inputFilePath.endsWith(".parquet")) {
        df = df
          .withColumn("contact_id", hash(col("contact_id")))
          .withColumn("line_item", explode(col("line_items")))
          .withColumn("product_id", col("line_item.sku"))
          .select("contact_id", "event_date", "product_id")

        df.write.parquet(outputFilePath.toString)
      }
      else if (inputFilePath.contains("browse-data_processed") && inputFilePath.endsWith(".avro")) {
        df = df
          .withColumn("contact_id", hash(col("contact_id")))
          .withColumn("customer_id", hash(col("customer_id")))
          .withColumn("product_id", col("value"))
          .select("customer_id", "contact_id", "product_id", "event_date", "url")

        df.write.avro(outputFilePath.toString)
      }
      else {
        throw new RuntimeException("invalid file type while writing anonymized & trimmed copies")
      }
      if (doPrintlns) {
        df.show(10, false)
      }
    }
  }

  def containsAll(fullPathStr: String, strs: Array[String]): Boolean = {

    for (str <- strs) {
      if (!fullPathStr.contains(str))
        return false
    }
    true
  }

  def recursivelyWriteAnonymized(spark: SparkSession, inputParentDir: File, outputDir: File, fullPathMandatoryStrings: Array[String], fileTypes: Array[String]) = {

    for (file <- inputParentDir.listFiles.filter(file => containsAll(file.getAbsolutePath(), fullPathMandatoryStrings))) {

      println("processing " + file.toString)
      writeAnonymizedCopyOfFile(spark, file.getAbsolutePath, outputDir.getAbsolutePath)
    }
  }

  /** anonymize different files into different outputs.  filter by filename. */
  def recursivelyWriteAnonymizedPrecise(spark: SparkSession, inputParentDir: File, outputDir: File, fullPathMandatoryStrings: Array[String]) = {

    inputParentDir.listFiles
      .filter(file => containsAll(file.getAbsolutePath(), fullPathMandatoryStrings))
      .foreach(inputFile => writeAnonymizedCopyOfFilePrecise(spark, inputFile.getAbsolutePath, outputDir.getAbsolutePath))
  }

  def displayActualDataFilesOld(spark: SparkSession, outputDir: File) = {
    FileUtils.listFiles(outputDir, Array("avro"), true).foreach(println)
    FileUtils.listFiles(outputDir, Array("parquet"), true).foreach(file => {
      println(file)


      val df = getDf(spark, file)

      df.show()

      //      val updatedDf = table.withColumn("contact_id", udf(anonymizeLong).apply(col("contact_id")))

      println("exploded:\n")
      //      df


      val df2 = df.withColumn("line_item", explode(col("line_items")))
      //      val df2 = df //.withColumn("line_item", explode(col("line_items")))


      //      val df3 = df.withColumn("line_item", explode(col("line_items")))

      df2.show()
      df2.printSchema()

      val df3 = df2
        .withColumn("quantity", col("line_item.quantity"))
        .withColumn("product_id", col("line_item.sku"))
        .withColumn("total_price", col("line_item.total_price"))
        .drop("line_item")
        .drop("line_items")

      println("explodedexploded:\n")


      df3.show()
      df3.printSchema()

    })
    FileUtils.listFiles(outputDir, Array("csv"), true).foreach(file => {
      println(file)
      Files.readAllLines(file.toPath).subList(0, 10).foreach(println)
      println()
    })
  }

  def displayActualDataFiles(spark: SparkSession, outputDir: File) = {
    FileUtils.listFiles(outputDir, Array("avro", "parquet", "csv"), true)
      .foreach(f => {
        println(f)
        getDf(spark, f).show(10)
      })
  }

  val doPrintlns = true

  def main(args: Array[String]) {
    org.apache.log4j.Logger.getLogger("org").setLevel(org.apache.log4j.Level.OFF)
    println("o hi")

    val spark = SparkSession.builder.master("local[2]").appName("SUnderstandingSparkSession").getOrCreate()

    val inputDir = new File("resources/flatfiles")
    val outputDir = new File("output")
    FileUtils.deleteDirectory(outputDir)
    outputDir.mkdirs()

    //    displayActualDataFiles(spark, inputDir)

    recursivelyWriteAnonymizedPrecise(spark, inputDir, outputDir, Array("resources", "flatfiles"))

    spark.stop()
  }
}