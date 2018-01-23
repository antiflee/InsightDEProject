//package com.yufeil.dotainsight.flink_streaming;
//
//import com.yufeil.dotainsight.utils.HostURLs;
//import com.yufeil.dotainsight.utils.KafkaTopicNames;
//import org.apache.flink.api.common.restartstrategy.RestartStrategies;
//import org.apache.flink.api.common.serialization.DeserializationSchema;
//import org.apache.flink.api.common.serialization.SerializationSchema;
//import org.apache.flink.api.common.serialization.SimpleStringSchema;
//import org.apache.flink.api.common.serialization.TypeInformationSerializationSchema;
//import org.apache.flink.api.common.typeinfo.TypeInformation;
//import org.apache.flink.api.java.tuple.Tuple2;
//import org.apache.flink.api.java.tuple.Tuple3;
//import org.apache.flink.api.java.typeutils.TypeInfoParser;
//import org.apache.flink.api.java.utils.ParameterTool;
//import org.apache.flink.streaming.api.TimeCharacteristic;
//import org.apache.flink.streaming.api.datastream.DataStream;
//import org.apache.flink.streaming.api.datastream.DataStreamSource;
//import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
//import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer010;
//
//import java.util.Properties;
////import redis.clients.jedis.Jedis;
//
//public class Test {
//    public static void main(String[] args) throws Exception {
//
//        final ParameterTool parameterTool = ParameterTool.fromArgs(args);
//
//        // Publics DNS names of the tools used.
//        HostURLs urls = new HostURLs();
//        // Names of the topics in Kafka.
//        KafkaTopicNames topics = new KafkaTopicNames();
//
//        // Set up the properties of the Kafka server.
//        Properties properties = new Properties();
//        properties.put("bootstrap.servers", urls.KAFKA_URL+":9092");
//        properties.put("zookeeper.connect", urls.ZOOKEEPER_URL+":2181");
//        properties.put("group.id", "flink-streaming-json-parser");
//
//        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
//
//        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
//        env.getConfig().disableSysoutLogging();
//        env.getConfig().setRestartStrategy(RestartStrategies.fixedDelayRestart(4, 10000));
//        env.enableCheckpointing(5000); // create a checkpoint every 5 seconds
//        env.getConfig().setGlobalJobParameters(parameterTool); // make parameters available in the web interface
//
//        // make parameters available in the web interface
//        env.getConfig().setGlobalJobParameters(parameterTool);
//
//        // 2. regionInfo: Tuple3<Integer, Long, Long>
//        TypeInformation<Tuple3<Integer, Long, Long>> intLongLong = TypeInfoParser.parse("Tuple3<Integer, Long, Long>");
//        TypeInformationSerializationSchema<Tuple3<Integer, Long, Long>> regionInfoSerSchema =
//                new TypeInformationSerializationSchema<>(intLongLong, env.getConfig());
//
//
//        DataStreamSource<Tuple3<Integer, Long, Long>> test = env
//                .addSource(new FlinkKafkaConsumer010<>(
//                        topics.REGION_INFO,
//                        regionInfoSerSchema,
//                        properties));
//
//        test.print();
//
//        env.execute("test");

//        String rawJSONString = "{pre_game_duration: 90, flags: 1, match_id: 2041712029, match_seq_num: 1798354947, radiant_win: t, start_time: 1451490407, duration: 2022, tower_status_radiant: 1982, tower_status_dire: 0, barracks_status_radiant: 63, barracks_status_dire: 0, cluster: 204, first_blood_time: 12, lobby_type: 7, human_players: 10, leagueid: 0, positive_votes: 0, negative_votes: 0, game_mode: 22, engine: 1, players: [{account_id:4294967295,hero_id:6,player_slot:0},{account_id:4294967295,hero_id:44,player_slot:1},{account_id:127247352,hero_id:104,player_slot:2},{account_id:154109636,hero_id:14,player_slot:3},{account_id:101364475,hero_id:21,player_slot:4},{account_id:4294967295,hero_id:90,player_slot:128},{account_id:233321909,hero_id:1,player_slot:129},{account_id:151447131,hero_id:113,player_slot:130},{account_id:175992604,hero_id:74,player_slot:131},{account_id:120626522,hero_id:42,player_slot:132}]}";
//        SingleMatch match = new Gson().fromJson(rawJSONString, SingleMatch.class);
//        System.out.println(match.getStart_time());
//    }
//}