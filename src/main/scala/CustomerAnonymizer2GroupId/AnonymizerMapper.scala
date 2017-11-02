package CustomerAnonymizer2GroupId

import java.io.File
import java.nio.file.Paths

import com.databricks.spark.avro._
import org.apache.commons.io.FileUtils
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

import scala.collection.JavaConversions._


/** see def main */
object AnonymizerMapper {

  final val SPARK_APP_NAME = "StuartBrowseOrderAnonymizer"

  //TODO how can i combine these?
  def anonymizeLong: (Long => Int) = _.toString.hashCode

  //  def anonymizeLong: (Any => Int) = _.toString.hashCode

  def anonymizeString: (String => Int) = _.toString.hashCode

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


        //per df, put all useIDs in a set.  then put in list.  get random number between 0 and last index.
        // remove that and put in new list.  repeat.  new list index is new element's new userID.  put new list in map



        df = df
          .withColumn("contact_id", udf(anonymizeLong).apply(col("contact_id")))
          .withColumn("line_item", explode(col("line_items")))
          .withColumn("product_id", col("line_item.sku"))
          .select("contact_id", "event_date", "product_id")

        df.write.parquet(outputFilePath.toString)
      }
      else if (inputFilePath.contains("browse-data_processed") && inputFilePath.endsWith(".avro")) {
        df = df
          .withColumn("contact_id", udf(anonymizeLong).apply(col("contact_id")))
          .withColumn("customer_id", udf(anonymizeString).apply(col("customer_id")))
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

    if (strs == null || strs.isEmpty)
      return true

    for (str <- strs) {
      if (!fullPathStr.contains(str))
        return false
    }
    true
  }


  /** anonymize different files into different outputs.  filter by filename. */
  def recursivelyWriteAnonymizedPrecise(spark: SparkSession, inputParentDir: File, outputDir: File, fullPathMandatoryStrings: Array[String]) = {

    inputParentDir.listFiles
      .filter(file => containsAll(file.getAbsolutePath(), fullPathMandatoryStrings))
      .foreach(inputFile => writeAnonymizedCopyOfFilePrecise(spark, inputFile.getAbsolutePath, outputDir.getAbsolutePath))
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

    //TODO talk to john melton about this - done
    //TODO - don't hash.  use random mapping.  how ... ? - per df, put all useIDs in a set.  then put in list.  get random number between 0 and last index.  remove that and put in new list.  repeat.  new list index is new element's new userID.  put new list in map

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