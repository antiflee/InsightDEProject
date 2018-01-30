package com.yufeil.dotainsight.spark

/*
  Calculating Monthly-Active-Users
 */

import org.apache.spark.{SparkConf, SparkContext}
import com.datastax.spark.connector._
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.approx_count_distinct

object MAU {

  val HOST_IP = "ec2-34-213-32-67.us-west-2.compute.amazonaws.com"
  val SPARK_ADDRESS = "spark://ip-10-0-0-5.us-west-2.compute.internal:7077"

  def main(args: Array[String]): Unit = {

    if (args.length == 0 || args(0).length != 7) {
      println("Please indicate the date to calculate yyyy-MM")
      return
    }

    // Setup Spark
    val sparkConf = new SparkConf()
      .setAppName("MAU")
      .setMaster(SPARK_ADDRESS)
      .set("spark.cassandra.connection.host", HOST_IP)
    val sparkContext = new SparkContext(sparkConf)

    // The date that Spark looks for in player_match table for processing
    var date = args(0)

    val dateArray = date.split("-")
    val year = dateArray(0)
    val month = dateArray(1)

    val queryCondition = "year = " + year + " and month = " + month

    // Read data from Cassandra
    val count = sparkContext.cassandraTable("ks", "daily_player")
      // Only needs duration and win for each row
      .select("year", "month", "account_id")
      // Select the matches during that day
      .where(queryCondition)
      // To-do: Use HyperLogLog to approximate the DAU
      .distinct()
      .count()

    val rdd = sparkContext.parallelize(Seq((year, month, count)))
      .saveToCassandra("ks", "monthly_active_users",
        SomeColumns("year", "month", "num"))


    println("******************************************")
    println(count)
    println("******************************************")
  }
}
