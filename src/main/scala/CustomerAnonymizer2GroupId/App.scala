package CustomerAnonymizer2GroupId

import org.apache.spark.sql.SparkSession

object App {
  def main(args: Array[String]) = {
    println("oh hi")

    val spark = SparkSession.builder.master("local[2]").appName("SUnderstandingSparkSession").getOrCreate()

    val df = spark.read.json("resources/people.json")
    df.show()


    org.apache.log4j.Logger.getLogger("org").setLevel(org.apache.log4j.Level.OFF)
  }
}