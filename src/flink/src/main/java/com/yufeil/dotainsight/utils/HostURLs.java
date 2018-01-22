package com.yufeil.dotainsight.utils;
/*
    Saves the com.yufeil.dotainsight.utils.HostURLs of Kafka, Flink, Zookeeper, Cassandra, Redis
 */

public class HostURLs {
    public String ZOOKEEPER_URL     = System.getenv("ZOOKEEPER_URL");
    public String KAFKA_URL         = System.getenv("KAFKA_URL");
    public String FLINK_URL         = System.getenv("FLINK_URL");
//    public String CASSANDRA_URL     = System.getenv("CASSANDRA_URL");
    public String CASSANDRA_URL     = "ec2-34-213-32-67.us-west-2.compute.amazonaws.com";
//    public String REDIS_URL         = System.getenv("REIDS_URL");
    public String REDIS_URL         = "ec2-34-213-4-249.us-west-2.compute.amazonaws.com";

}
