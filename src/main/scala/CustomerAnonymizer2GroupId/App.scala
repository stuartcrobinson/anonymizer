package CustomerAnonymizer2GroupId

import org.apache.spark.sql.functions._
import org.apache.spark.sql.{DataFrame, SparkSession}

import java.nio.file.Paths

import com.databricks.spark.avro._
import org.apache.commons.io.FileUtils
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

import scala.collection.JavaConversions._


import scala.collection.mutable

object App {

  val r = new scala.util.Random

  def addAnonymizedColumn(spark: SparkSession, df: DataFrame, colName: String): DataFrame = {

    import spark.implicits._

//    val df = Seq(
//      ("first", 2.0),
//      ("awef", 1.5),
//      ("awef", 1.5),
//      ("first", 1.5),
//      ("23r", 1.5),
//      ("aw", 1.5),
//      ("nrgn", 1.5),
//      ("j65", 1.5),
//      ("j65", 1.5),
//      ("j65", 8.0)
//    ).toDF("id", "val")


    df.show()
    val userIdsSet = df.select(colName).map(r => r.get(0).toString).collect.toSet
    val userIdsList = userIdsSet.toList
    val userIdReplacements = r.shuffle(0 to userIdsList.length).toList

    val myMap = scala.collection.mutable.Map[String, String]()

    userIdsList.zipWithIndex.foreach {
      case (id, i) => myMap += (id -> userIdReplacements(i).toString)
    }

    def replacer(mymap: mutable.Map[String, String]) = udf { x: String => mymap(x) } //mymap.get(x.toString)

    df.withColumn(colName + "_anon", replacer(myMap)(col(colName)))
  }


  def main(args: Array[String]) = {
    org.apache.log4j.Logger.getLogger("org").setLevel(org.apache.log4j.Level.OFF)
    println("oh hi")
    val spark = SparkSession.builder.master("local[2]").appName("SUnderstandingSparkSession").getOrCreate()
    import spark.implicits._


        val df2 = Seq(
          (1, 2.0),
          (2, 1.5),
          (2, 1.5),
          (1, 1.5),
          (3, 1.5),
          (4, 1.5),
          (5, 1.5),
          (6, 1.5),
          (6, 1.5),
          (6, 8.0)
        ).toDF("id", "val")

//    val df2 = Seq(
//      ("first", 2.0),
//      ("awef", 1.5),
//      ("awef", 1.5),
//      ("first", 1.5),
//      ("23r", 1.5),
//      ("aw", 1.5),
//      ("nrgn", 1.5),
//      ("j65", 1.5),
//      ("j65", 1.5),
//      ("j65", 8.0)
//    ).toDF("id", "val")

    df2.show()

    addAnonymizedColumn(spark, df2, "id").show()

//
//    val userIdsSet = df2.select("id").map(r => r.get(0).toString).collect.toSet
//    val userIdsList = userIdsSet.toList
//    val userIdReplacements = r.shuffle(0 to userIdsList.length).toList
//
//    val mymap = scala.collection.mutable.Map[String, String]()
//
//    userIdsList.zipWithIndex.foreach {
//      case (id, i) => mymap += (id -> userIdReplacements(i).toString)
//    }
//
//    println(mymap)
//
//    def replacer(mymap: mutable.Map[String, String]) = udf { x: String => mymap(x) } //mymap.get(x.toString)
//
//
//    val df3 = df2
//      .withColumn("hashed_id", hash(col("id")))
//      .withColumn("mapped_id", replacer(mymap)(col("id")))
//
//    df3.show()
//
//
//    System.exit(0)
//    val df = spark.read.json("resources/people.json")
//    df.show()
  }
}