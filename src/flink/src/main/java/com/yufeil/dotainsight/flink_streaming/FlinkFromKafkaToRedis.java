package com.yufeil.dotainsight.flink_streaming;

/*
    Serves as the connector between Kafka and Redis.
    Read from topics "hero-win-rate", "hero-pair-win-rate", "hero-counter-pair-win-rate"
 */

import com.yufeil.dotainsight.utils.HostURLs;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer010;
import redis.clients.jedis.Jedis;

import java.util.Properties;

public class FlinkFromKafkaToRedis {

    // Single node mode
    static private Jedis jedis;

    public static void main(String[] args) throws Exception {
        // parse input arguments
        final ParameterTool parameterTool = ParameterTool.fromArgs(args);

        HostURLs urls = new HostURLs();

        // Set up properties for Kafka and Redis
        Properties properties = new Properties();
        properties.put("bootstrap.servers",
                parameterTool.get(
                        "bootstrap.servers",
                        urls.KAFKA_URL+":9092"));
        properties.put("zookeeper.connect",
                parameterTool.get(
                        "zookeeper.connect",
                        urls.KAFKA_URL+":2181"));
        properties.put("group.id", parameterTool.get("group.id","flink-streaming-kafka-to-redis"));

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.getConfig().disableSysoutLogging();
        env.getConfig().setRestartStrategy(RestartStrategies.fixedDelayRestart(4, 10000));
        env.enableCheckpointing(5000); // create a checkpoint every 5 seconds
        env.getConfig().setGlobalJobParameters(parameterTool); // make parameters available in the web interface

        // make parameters available in the web interface
        env.getConfig().setGlobalJobParameters(parameterTool);

        // 1. hero-win-rate
        DataStream<String> heroWinRate = env
                .addSource(new FlinkKafkaConsumer010<>(
                        "hero-win-rate",
                        new SimpleStringSchema(),
                        properties))
                .map(new RedisSender());

        // Write the data stream to redis
//        heroWinRateStream.addSink(new RedisSink<Tuple2<String, String>>(conf, new RedisExampleMapper()));

        env.execute("Send win rate of each hero to Redis");
    }

    private static class RedisSender implements MapFunction<String, String> {

        // A mapper that format the raw heroWinRateStream.
        // To-do: use Avro for Schema Registry.

        @Override
        public String map(String value) throws Exception {

            String[] tmp = value.split(",");
            Tuple2<String, String> res = new Tuple2<>(tmp[0], tmp[1]);

            // Send data to Redis

            // Single node
            jedis.set(res.f0, res.f1);

            return value;
        }
    }
}
