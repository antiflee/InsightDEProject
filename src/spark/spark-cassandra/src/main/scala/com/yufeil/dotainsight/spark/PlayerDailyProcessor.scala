package com.yufeil.dotainsight.spark

/*
  For each date, Spark looks for the behaviour of each player in 'player_match'
  table in Cassandra, aggregating all matches played by that player, and write
  the result into 'player_daily' data table.

  Schema of "player_match":
    account_id bigint,
    year int,
    month int,
    day int,
    start_time timestamp,
    duration int,
    hero_id text,
    win boolean,
    PRIMARY KEY (account_id, start_time)

  Schema of "player_daily":
    account_id bigint,
    year int,
    month int,
    day int,
    matches_played int,
    matches_won int,
    time_played_in_min int,
    PRIMARY KEY (account_id, date)

 */

import com.datastax.spark.connector._
import org.apache.spark.sql.cassandra._
import org.apache.spark.sql.SparkSession
import org.apache.spark.{SparkConf, SparkContext}

object PlayerDailyProcessor {

  val HOST_IP = "ec2-34-213-32-67.us-west-2.compute.amazonaws.com"
  val SPARK_ADDRESS = "spark://ip-10-0-0-5.us-west-2.compute.internal:7077"

  def main(args: Array[String]): Unit = {

    if (args.length == 0 || args(0).length != 10) {
      println("Please indicate the date to calculate yyyy-MM-dd")
      return
    }

    // Setup Spark
    val sparkConf = new SparkConf()
      .setAppName("Player_Daily")
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
    val rdd = sparkContext.cassandraTable("ks", "player_match")
      // Only needs duration and win for each row
      .select("account_id","year","month","day","duration","win")
      // Select the matches during that day
      .where(queryCondition)
      // Group by (account_id,year,month,day)
      .keyBy(row => (row.getLong("account_id"),row.getInt("year"),row.getInt("month"),row.getInt("day")))
      // Map. Value becomes (duration, win, 1)
      .map{case (k,v) => (k, (v.getInt("duration"),bool2int(v.getBoolean("win")),1))}
      // Reduce
      .reduceByKey((a,b) => (a._1+b._1,a._2+b._2,a._3+b._3))
      // Flatten the result to save to Cassandra
      .map{case (
            (account_id,year,month,day),(time_played_in_sec,matches_won,matches_played)) =>
            (account_id,year,month,day,matches_played,matches_won,time_played_in_sec)}
      // Save to Cassandra
      .saveToCassandra("ks", "player_daily",
            SomeColumns("account_id", "year", "month", "day",
                        "matches_played","matches_won","time_played_in_sec"))


    println("******************************************")
    println("******************************************")
  }

  implicit def bool2int(b:Boolean) = if (b) 1 else 0
}
