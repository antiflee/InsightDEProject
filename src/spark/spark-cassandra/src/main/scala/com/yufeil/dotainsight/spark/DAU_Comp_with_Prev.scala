package com.yufeil.dotainsight.spark

import org.apache.spark.{SparkConf, SparkContext}
import com.datastax.spark.connector._
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.approx_count_distinct

/*
  Calculate the daily active users (DAU) from Cassandra data tabe ks.'daily_player',
  and write the result into ks.'daily_active_user'.

  Method:
    Comparing each account_id with the previous one. If different, count += 1
 */

object DAU_Comp_with_Prev {

  val HOST_IP = "ec2-34-213-32-67.us-west-2.compute.amazonaws.com"
  val SPARK_ADDRESS = "spark://ip-10-0-0-5.us-west-2.compute.internal:7077"

  def main(args: Array[String]): Unit = {

    if (args.length == 0 || args(0).length != 10) {
      println("Please indicate the date to calculate yyyy-MM-dd")
      return
    }

    var prev_acc_id = -1L    // Records the previous account_id
    var count = 0L

    // Setup Spark
    val sparkConf = new SparkConf()
      .setAppName("DAU")
      .setMaster(SPARK_ADDRESS)
      .set("spark.cassandra.connection.host",HOST_IP)
    val sparkContext = new SparkContext(sparkConf)

    // The date that Spark looks for in player_match table for processing
    var date = args(0)

    val dateArray = date.split("-")
    val year = dateArray(0)
    val month = dateArray(1)
    val day = dateArray(2)

    val queryCondition = "year = " + year + " and month = " + month + " and day = " + day

    // Read data from Cassandra
    val casRdd: RDD[Unit] = sparkContext.cassandraTable("ks", "daily_player")
      // Only needs duration and win for each row
      .select("year", "month", "day", "account_id")
      // Select the matches during that day
      .where(queryCondition)
      // To-do: Use HyperLogLog to approximate the DAU
      .map { row =>
        val new_acc_id = row.getLong("account_id")
        if (new_acc_id != prev_acc_id) {
          prev_acc_id = new_acc_id
          count += 1
        }
      }

    val rdd = sparkContext.parallelize(Seq((year, month, day, count)))
      .saveToCassandra("ks", "daily_active_users",
        SomeColumns("year", "month", "day", "num"))


    println("******************************************")
    println(count)
    println("******************************************")
  }
}
