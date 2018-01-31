package com.yufeil.dotainsight.spark

import java.nio.ByteBuffer

import org.apache.spark.{SparkConf, SparkContext}
import com.datastax.spark.connector._
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.approx_count_distinct
import com.twitter.algebird.{Approximate, HLL, HyperLogLogMonoid}
import com.twitter.algebird.HyperLogLog._
import org.apache.spark.rdd.RDD

/*
  Calculate the daily active users (DAU) from Cassandra data tabe ks.'daily_player',
  and write the result into ks.'daily_active_user'.

  Using HyperLogLog (HLL) to achieve the space and time efficiency. The trade-off is
  accuracy.

  One example can be found:
    https://github.com/eBay/Spark/blob/master/examples/src/main/scala/org/apache/spark/examples/streaming/TwitterAlgebirdHLL.scala

  About how spark implements HLL:
    https://databricks.com/blog/2016/05/19/approximate-algorithms-in-apache-spark-hyperloglog-and-quantiles.html
 */

object DAU_HLL {

  val HOST_IP = "ec2-34-213-32-67.us-west-2.compute.amazonaws.com"
  val SPARK_ADDRESS = "spark://ip-10-0-0-5.us-west-2.compute.internal:7077"

  def longToBytes(x: Long): Array[Byte] = {
    var l = x
    val result = new Array[Byte](8)
    var i = 7
    while ({i >= 0}) {
      result(i) = (l & 0xFF).toByte
      l = l >> 8

      {i -= 1; i + 1}
    }
    result
  }

  def main(args: Array[String]): Unit = {


    if (args.length == 0 || args(0).length != 10) {
      println("Please indicate the date to calculate yyyy-MM-dd")
      return
    }

    // Setup HLL
    /** Bit size parameter for HyperLogLog, trades off accuracy vs size */
    val BIT_SIZE = 12

    val hll = new HyperLogLogMonoid(BIT_SIZE)

    // Setup Spark
    val sparkConf = new SparkConf()
      .setAppName("DAU_HLL")
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
    val hlls: HLL = sparkContext.cassandraTable("ks", "daily_player")
      // Select the records during that day
      .where(queryCondition)
      .select("account_id")
      .map(row => row.getLong("account_id"))
      .map{ id =>
      val bytes: Array[Byte] = longToBytes(id)
      hll.create(bytes)
    }
//      .map( id => id.toString())
//      .map { str =>
//        val bytes: Array[Byte] = str.getBytes("utf-8")
//        hll.create(bytes)
//      }
      .reduce(_+_)

    val count = hll.sizeOf(hlls).estimate

    val rdd = sparkContext.parallelize(Seq((year, month, day, count)))
      .saveToCassandra("ks", "daily_active_users",
        SomeColumns("year", "month", "day", "num"))


    println("******************************************")
    println(count)
    println("******************************************")
  }

}
