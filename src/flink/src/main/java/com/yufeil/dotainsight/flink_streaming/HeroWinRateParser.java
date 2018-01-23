package com.yufeil.dotainsight.flink_streaming;

import com.yufeil.dotainsight.utils.HostURLs;
import com.yufeil.dotainsight.utils.KafkaTopicNames;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.common.serialization.TypeInformationSerializationSchema;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.api.java.typeutils.TypeInfoParser;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSink;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.AssignerWithPeriodicWatermarks;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.streaming.api.windowing.assigners.SlidingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.connectors.cassandra.CassandraSink;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer010;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer010;

import java.sql.Timestamp;
import java.util.Properties;

/*
    Reads from Kafka topics: (1) "hero-result", (2) "hero-pair-result", and (3) "hero-counter-pair-result".
    Aggregate over a fixed time window and calculate for the corresponding win rate.

    Note that those three topics were also written by Flink, and used the TypeInformationSerializationSchema,
    So we could easily deserialize it.

    Output:
        Send the three win-rate data streams to Kafka, as Strings.
            Format: <"hero_id(1,hero_id2),win_rate"> : <String>
        Send the hero-win-rate with timestamp to Cassandra.
            Format: <hero_id:String,win_rate:Float,time_:Timestamp>
 */

public class HeroWinRateParser {

    private static final int WINDOWLENGTH = 600000;   // in milliseconds.
    private static final int SLIDELENGTH = 30000;   // in milliseconds.
    private static final String CASSANDRA_KEYSPACE = "ks";

    public static void main(String[] args) throws Exception {

        final ParameterTool parameterTool = ParameterTool.fromArgs(args);

        // Publics DNS names of the tools used.
        HostURLs urls = new HostURLs();
        // Names of the topics in Kafka.
        KafkaTopicNames topics = new KafkaTopicNames();

        // Set up the properties of the Kafka server.
        Properties properties = new Properties();
        properties.put("bootstrap.servers", urls.KAFKA_URL+":9092");
        properties.put("zookeeper.connect", urls.ZOOKEEPER_URL+":2181");
        properties.put("group.id", "flink-streaming-hero-win-rate-parser");

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
        env.getConfig().disableSysoutLogging();
        env.getConfig().setRestartStrategy(RestartStrategies.fixedDelayRestart(4, 10000));
        env.enableCheckpointing(5000); // create a checkpoint every 5 seconds
        env.getConfig().setGlobalJobParameters(parameterTool); // make parameters available in the web interface


        // Schema of the data stream
        TypeInformation<Tuple3<String, Integer, Long>> stringIntLong = TypeInfoParser.parse("Tuple3<String, Integer, Long>");
        TypeInformationSerializationSchema<Tuple3<String, Integer, Long>> heroResultSerSchema =
                new TypeInformationSerializationSchema<>(stringIntLong, env.getConfig());

          ///////////////
         // Read data //
        ///////////////

        // 1. hero-result
        DataStreamSource<Tuple3<String, Integer, Long>> heroResult = env
                .addSource(new FlinkKafkaConsumer010<>(
                        topics.HERO_RESULT,
                        heroResultSerSchema,
                        properties));

        // 2. hero-pair-result
        DataStreamSource<Tuple3<String, Integer, Long>> heroPairResult = env
                .addSource(new FlinkKafkaConsumer010<>(
                        topics.HERO_PAIR_RESULT,
                        heroResultSerSchema,
                        properties));

        // 3. hero-counter-pair-result
        DataStreamSource<Tuple3<String, Integer, Long>> heroCounterPairResult = env
                .addSource(new FlinkKafkaConsumer010<>(
                        topics.HERO_COUNTER_PAIR_RESULT,
                        heroResultSerSchema,
                        properties));

          ////////////////////////////
         // Calculate the win rate //
        ////////////////////////////

        // 1. Calculate the win rate for each hero.
        //    Format: <hero_id:String, win rate:Float, time_stamp:Long>
        DataStream<Tuple3<String,Float,Timestamp>> heroWinRate = heroResult
                .assignTimestampsAndWatermarks(new HeroResultTimeStampGenerator())
                .keyBy(0)
                .window(
                        SlidingEventTimeWindows.of(
                                Time.milliseconds(WINDOWLENGTH),
                                Time.milliseconds(SLIDELENGTH)))
                .aggregate(new WinRateAggregator())
                // Convert the time from long to Timestamp, so that
                // Cassandra can read it.
                .map(new HeroWinRateTimeStampLongToTimestamp());

        // Convert from Tuple3 to String, then send to Kafka, for Redis to use.
        DataStream<String> heroWinRateString = heroWinRate
                .map(new HeroWinRateString());

        // 2. Calculate the hero pair win rate for each hero pair.
        //     For simplicity, we combine hero_id1 and hero_id2 as one string.
        //     So the format is: <"hero_id1,hero_id2":String, win rate:Float, time_stamp:Long>
        //     So that we can reuse the WinRateAggregator() method.
        DataStream<Tuple3<String,Float,Long>> heroPairWinRate= heroPairResult
                .assignTimestampsAndWatermarks(new HeroResultTimeStampGenerator())
                .keyBy(0)
                .window(
                        SlidingEventTimeWindows.of(
                                Time.milliseconds(WINDOWLENGTH),
                                Time.milliseconds(SLIDELENGTH)))
                .aggregate(new WinRateAggregator());

        // Convert from Tuple3 to String, then send to Kafka, for Redis to use.
        DataStream<String> heroPairWinRateString = heroPairWinRate
                .map(new HeroPairWinRateString());


        // 3. Calculate the hero counter pair win rate for each hero pair.
        //     For simplicity, we combine hero_id1 and hero_id2 as one string.
        //     So the format is: <"hero_id1,hero_id2":String, win rate:Float, time_stamp:Long>
        //     So that we can reuse the WinRateAggregator() method.
        DataStream<Tuple3<String,Float,Long>> heroCounterPairWinRate = heroPairResult
                .assignTimestampsAndWatermarks(new HeroResultTimeStampGenerator())
                .keyBy(0)
                .window(
                        SlidingEventTimeWindows.of(
                                Time.milliseconds(WINDOWLENGTH),
                                Time.milliseconds(SLIDELENGTH)))
                .aggregate(new WinRateAggregator());

        // Convert from Tuple3 to String, then send to Kafka, for Redis to use.
        DataStream<String> heroCounterPairWinRateString = heroCounterPairWinRate
                .map(new HeroPairWinRateString());


           /////////////////////////////////////////////////
          // Send the win rate result to Kafka as String //
         //  For Redis to use                           //
        /////////////////////////////////////////////////

        // 1. "hero-win-rate"
        DataStreamSink<String> heroWinRateSink = heroWinRateString.addSink(
                new FlinkKafkaProducer010<>(
                        "hero-win-rate",
                        new SimpleStringSchema(),
                        properties));
        heroWinRateSink.name("hero-win-rate-to-kafka");

        // 2. "hero-pair-win-rate"
        DataStreamSink<String> heroPairWinRateSink = heroPairWinRateString.addSink(
                new FlinkKafkaProducer010<>(
                        "hero-pair-win-rate",
                        new SimpleStringSchema(),
                        properties));
        heroPairWinRateSink.name("hero-pair-win-rate-to-kafka");

        // 3. "hero-counter-pair-win-rate"
        DataStreamSink<String> heroCounterPairWinRateSink = heroCounterPairWinRateString.addSink(
                new FlinkKafkaProducer010<>(
                        "hero-counter-pair-win-rate",
                        new SimpleStringSchema(),
                        properties));
        heroCounterPairWinRateSink.name("hero-counter-pair-win-rate-to-kafka");

          ////////////////////////////////////////////////////
         // Send hero win rate with timestamp to Cassandra //
        ////////////////////////////////////////////////////

        CassandraSink.addSink(heroWinRate)
                .setQuery(
                        "INSERT INTO "+CASSANDRA_KEYSPACE+".hero_win_rate" +
                                "(hero_id, win_rate, time_) values (?, ?, ?);")
                .setHost(urls.CASSANDRA_URL)
                .build();

        /////////////
        // Execute //
        /////////////

        env.execute("Hero Win Rate Parser");

    }
    public static class HeroResultTimeStampGenerator implements AssignerWithPeriodicWatermarks<Tuple3<String,Integer,Long>> {

        /*
            Assign timestamp to each record of hero-result
         */

        private final long maxOutOfOrderness = 90*60; // 90 min out of order allowed.

        private long currentMaxTimestamp;

        @Override
        public long extractTimestamp(Tuple3<String,Integer,Long> heroResult, long previousElementTimestamp) {
            long timestamp = heroResult.f2;
            currentMaxTimestamp = Math.max(timestamp, currentMaxTimestamp);
            return timestamp;
        }

        @Override
        public Watermark getCurrentWatermark() {
            // return the watermark as current highest timestamp minus the out-of-orderness bound
            return new Watermark(currentMaxTimestamp - maxOutOfOrderness);
        }
    }

    private static class WinRateAggregator
            implements AggregateFunction<Tuple3<String, Integer, Long>, Tuple4<String, Long, Long, Long>, Tuple3<String, Float, Long>> {

        // Calculate the win rate of a specific hero
        // See documentation
        //    https://ci.apache.org/projects/flink/flink-docs-master/dev/stream/operators/windows.html#aggregatefunction
        // Input/value: <hero_id:String,win/lose:Integer,start_time:Long>
        // Accumulator: <hero_id:String,total_win:Long,total_played:Long,latest_start_time:Long>
        // Result: <hero_id:String,win_rate:Float,latest_start_time:Long>

        @Override
        public Tuple4<String, Long, Long, Long> createAccumulator() {
            return new Tuple4<>("",0L, 0L, 0L);
        }

        @Override
        public Tuple4<String, Long, Long, Long> add(Tuple3<String, Integer, Long> value, Tuple4<String, Long, Long, Long> accumulator) {
            // Update accumulator with the new value. Here we update the start_time to be the latest
            return new Tuple4<>(value.f0,
                    accumulator.f1 + value.f1,
                    accumulator.f2+ 1L,
                    Math.max(value.f2,accumulator.f3));
        }

        @Override
        public Tuple3<String, Float, Long> getResult(Tuple4<String, Long, Long, Long> accumulator) {
            return new Tuple3<>(accumulator.f0, Float.valueOf(accumulator.f1) / accumulator.f2, accumulator.f3);
        }

        @Override
        public Tuple4<String, Long, Long, Long> merge(Tuple4<String, Long, Long, Long> a, Tuple4<String, Long, Long, Long> b) {
            return new Tuple4<>(a.f0, a.f1 + b.f1, a.f2 + b.f2, Math.max(a.f3,b.f3));
        }
    }

    private static class HeroWinRateTimeStampLongToTimestamp
            implements MapFunction<Tuple3<String,Float,Long>, Tuple3<String,Float,Timestamp>> {
        /*
            Convert the timestamp in heroResult DataStream from Long to Timestamp
            to save into Cassandra
         */
        @Override
        public Tuple3<String,Float,Timestamp> map(Tuple3<String,Float,Long>v) throws Exception {
            return new Tuple3<>(v.f0,v.f1,new Timestamp(v.f2));
        }
    }

    private static class HeroWinRateString
            implements MapFunction<Tuple3<String,Float,Timestamp>, String> {
        /*
            Convert heroWinRate from Tuple3 to String
         */
        @Override
        public String map(Tuple3<String,Float,Timestamp> v) throws Exception {
            return v.f0+","+String.format("%.3f", v.f1);
        }
    }

    private static class HeroPairWinRateString
            implements MapFunction<Tuple3<String,Float,Long>, String> {
        /*
            Convert heroPairWinRate from Tuple3 to String
         */
        @Override
        public String map(Tuple3<String,Float,Long> v) throws Exception {
            return v.f0+","+String.format("%.3f", v.f1);
        }
    }

}
