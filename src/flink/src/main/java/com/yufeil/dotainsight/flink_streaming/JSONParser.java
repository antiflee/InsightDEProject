package com.yufeil.dotainsight.flink_streaming;

/*
    Read from topic "match-raw-json" from Kafka, parse it and generate the following data streams (note
    there are some intermediate streams that are not included in this table):

   |----------------------------------------------------------------------------------------------------------------|
   |   |        NAME          |            FORMAT            |    DESTINATION     |              NOTE               |
   |----------------------------------------------------------------------------------------------------------------|
   |(0)|       matches        |     <match:SingleMatch>      |       Flink        | Generated from json             |
   |   |                      |                              |                    | using Gson                      |
   |----------------------------------------------------------------------------------------------------------------|
   |   |                      |                              |                    | Not used. Will be saved         |
   |(1)|     matchSimple      |        <String>              |       -----        | to a SQL DB later (for          |
   |   |                      |                              |                    | a ML project).                  |
   |----------------------------------------------------------------------------------------------------------------|
   |(2)|     heroResult       |   <hero_id,win,start_time>   |       Flink        | For downstream processing in    |
   |   |                      |                              |                    | Flink (win-rate calculation).   |
   |----------------------------------------------------------------------------------------------------------------|
   |(3)|    heroPairResult    |   <"id1,id2",win,time_>      |       Flink        | Here two hero ids are combined  |
   |   |                      |                              |                    | as one string.                  |
   |----------------------------------------------------------------------------------------------------------------|
   |(4)| heroCounterPairResult|     <"id1,id2",win,time_>    |       Flink        | Same as above.                  |
   |----------------------------------------------------------------------------------------------------------------|
   |   |                      |                              |                    | For the calculation of num_of_  |
   |(5)|     regionInfo       |  <cluster_id,start_time,10L> |       Flink        | players in each region. The     |
   |   |                      |                              |                    | number 10L here is used for     |
   |   |                      |                              |                    | aggregation function later.     |
   |----------------------------------------------------------------------------------------------------------------|
   |(6)|   playerMatchInfo    | <acc_id,start_time,duration, |       Flink        | Will be used later by Flink, to |
   |   |                      |                              |                    | save to Cassandra.              |
   |----------------------------------------------------------------------------------------------------------------|
   |   |                      |                              |       Kafka        | This data is for Redis to use.  |
   |(7)|    heroWinRate       |   <hero_id,win_rate,time_>   |  topic: "hero-win  |                                 |
   |   |                      |                              |  -rate"            |                                 |
   |----------------------------------------------------------------------------------------------------------------|
   |   |                      |                              |       Kafka        | Here two hero ids are combined. |
   |(8)|    heroPairWinRate   |  <"id1,id2",win_rate,time_>  |  topic: "hero-pair |                                 |
   |   |                      |                              |  -win"             |                                 |
   |----------------------------------------------------------------------------------------------------------------|
   |   |                      |                              |       Kafka        | See above.                      |
   |(9)|heroCounterPairWinRate|  <"id1,id2",win_rate,time_>  |  topic: "hero-     |                                 |
   |   |                      |                              |  counter-pair-win" |                                 |
   |----------------------------------------------------------------------------------------------------------------|
   |    |                     |                              |      Cassandra     | Split date into year,month,day  |
   |(10)|  heroWinRateDaily   |<hero_id,date,win_rate,count> |  table: "hero_win  | before saving to Cassandra.     |
   |    |                     |                              |  _rate"            |                                 |
   |----------------------------------------------------------------------------------------------------------------|
   |    |                     |     <cluster_id,date,        |      Cassandra     | Same as above.                  |
   |(11)| regionNumOfPlayers  |     num_of_players>          |  table: "region_   |                                 |
   |    |                     |                              |  num_of_players"   |                                 |
   |----------------------------------------------------------------------------------------------------------------|
   |    |                     |                              |      Cassandra     | Note here each record in the data|
   |    |                     | <acc_id,date,time,duration,  |  table#1: "player_ | stream is saved to 2 Cassandra  |
   |(12)|playerMatchInfoToCas |      hero_id,win/lose>       |  match"; #2: "daily| tables. The only difference b/w |
   |    |                     |                              |  _player.          | the 2 tables are the choice of  |
   |    |                     |                              |                    | partition key. The 2 tables are |
   |    |                     |                              |                    | for two different use cases.    |
   |----------------------------------------------------------------------------------------------------------------|


    To-dos:
    (1) Use Avro for Schema Registry
 */


import com.google.gson.Gson;
import com.yufeil.dotainsight.utils.HostURLs;
import com.yufeil.dotainsight.utils.KafkaTopicNames;
import com.yufeil.dotainsight.utils.SingleMatch;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.api.common.serialization.SerializationSchema;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.common.serialization.TypeInformationSerializationSchema;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.tuple.*;
import org.apache.flink.api.java.typeutils.TypeInfoParser;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSink;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.AssignerWithPeriodicWatermarks;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.streaming.api.windowing.assigners.SlidingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.assigners.SlidingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.connectors.cassandra.CassandraSink;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer010;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer010;
import org.apache.flink.util.Collector;

import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Properties;

public class JSONParser {

    private static final int WINDOWLENGTH = 60*60*1000;   // in milliseconds.
    private static final int SLIDELENGTH = 15*1000;   // in milliseconds.
    private static long INVALID_MATCHES = 0;        // Count invalid matches.

    private static final String CASSANDRA_KEYSPACE = "ks";

    public static void main(String[] args) throws Exception {

        ///////////////
        // Env setup //
        ///////////////

        // parse input arguments
        final ParameterTool parameterTool = ParameterTool.fromArgs(args);

        // Publics DNS names of the tools used.
        HostURLs urls = new HostURLs();
        // Names of the topics in Kafka.
        KafkaTopicNames topics = new KafkaTopicNames();

        // Set up the properties of the Kafka server.
        Properties properties = new Properties();
        properties.put("bootstrap.servers", urls.KAFKA_URL+":9092");
        properties.put("zookeeper.connect", urls.ZOOKEEPER_URL+":2181");
        properties.put("group.id", "flink-streaming-json-parser");

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
        env.getConfig().disableSysoutLogging();
        env.getConfig().setRestartStrategy(RestartStrategies.fixedDelayRestart(4, 10000));
        env.enableCheckpointing(5000); // create a checkpoint every 5 seconds
        env.getConfig().setGlobalJobParameters(parameterTool); // make parameters available in the web interface

        // make parameters available in the web interface
        env.getConfig().setGlobalJobParameters(parameterTool);



        ///////////////////////////////
        // Read JSON data from Kafka //
        ///////////////////////////////

        FlinkKafkaConsumer010<String> kafkaConsumer = new FlinkKafkaConsumer010<>(
                topics.MATCH_RAW_JSON,
                new SimpleStringSchema(),
                properties);
        kafkaConsumer.setStartFromLatest();

        DataStream<String> rawJSON = env
                .addSource(kafkaConsumer);

        ///////////////////////////////////////////////
        // Convert the json to SingleMatch instances //
        ///////////////////////////////////////////////

        DataStream<SingleMatch> matches = rawJSON
                .map(json -> parseMatch(json))
                // Filter the matches.
                .filter(match -> match != null && match.isValidMatch())
                // Assign event time using the start_time of the match
                .assignTimestampsAndWatermarks(new BoundedOutOfOrdernessGenerator());

        ////////////////////////////////////////
        // DataStreams based on the "matches" //
        ////////////////////////////////////////

        // 1. Generate a simplified match string in the format of
        //    "hero_id1,hero_id2,...,hero_id10,radiantWin,startTime"
        DataStream<String> matchSimple = matches
                .map(match -> match.simplifyMatch());

        // 2. Extract hero_result data from the matches data stream.
        //    Format: Tuple2<String, Integer, Long>
        //              --> <hero_id, win/lose, start_time>
        //    0: lose, 1: win
        DataStream<Tuple3<String, Integer, Long>> heroResult = matches
                .flatMap(new HeroResultExtractor());

        // 3. Extract hero-pair-result from the matches DataStream
        //    Format: Tuple3<String, Integer, Long>
        //              --> <"hero_id1,hero_id2", win/lose, start_time>
        DataStream<Tuple3<String, Integer, Long>> heroPairResult = matches
                .flatMap(new HeroPairResultExtractor())
                // First combine the two ids, so that we can use the same
                // serDe schema as the heroResult
                .map(new TwoHeroIdsCombiner());

        // 4. Extract hero-counter-pair-result from the matches DataStream
        //    Format: Tuple3<String, Integer, Long>
        //              --> <"hero_id1,hero_id2", win/lose, start_time>
        DataStream<Tuple3<String, Integer, Long>> heroCounterPairResult = matches
                .flatMap(new HeroCounterPairResultExtractor())
                // First combine the two ids, so that we can use the same
                // serDe schema as the heroResult
                .map(new TwoHeroIdsCombiner());

        // 5. Extract region information of the match
        //    Format: Tuple3<Integer, Long, Long>
        //              --> <cluster_id, start_time, 10>
        DataStream<Tuple3<Integer, Long, Long>> regionInfo = matches
                .flatMap(new RegionInfoFlatMapper());


        // 6. Extract each player's information from the match
        //    Format: Tuple5<Long, Timestamp, Integer, String, Integer>
        //       --> <account_id, start_time, duration, hero_id, win/lose>
        DataStream<Tuple5<Long,Long,Integer,String,Boolean>> playerMatchInfo = matches
                .flatMap(new PlayerMatchInfoExtractor());
                // Convert the type of timestamp from "long" to "timestamp", for Cassandra
//                .map(new PlayerMatchInfoLongToTimestamp());

          /////////////////////////////////////////////
         // Define schema for the above DataStreams //
        /////////////////////////////////////////////

        // Here we use TypeInformationSerializationSchema(). This is easy to implement, and
        // is used for data that are both read and written by Flink.
        // See: https://ci.apache.org/projects/flink/flink-docs-release-1.4/dev/connectors/kafka.html#the-deserializationschema

        // This part is not used, but just left here for the record.

        // 1. heroResult,heroPairResult,heroCounterPairResult: Tuple3<String, Integer, Long>
        TypeInformation<Tuple3<String, Integer, Long>> stringIntLong = TypeInfoParser.parse("Tuple3<String, Integer, Long>");
        TypeInformationSerializationSchema<Tuple3<String, Integer, Long>> heroResultSerSchema =
                new TypeInformationSerializationSchema<>(stringIntLong, env.getConfig());

        // 2. regionInfo: Tuple3<Integer, Long, Long>
        TypeInformation<Tuple3<Integer, Long, Long>> intLongLong = TypeInfoParser.parse("Tuple3<Integer, Long, Long>");
        TypeInformationSerializationSchema<Tuple3<Integer, Long, Long>> regionInfoSerSchema =
                new TypeInformationSerializationSchema<>(intLongLong, env.getConfig());

        // 3. playerMatchInfo: Tuple5<Long,Long,Integer,String,Boolean>
        TypeInformation<Tuple5<Long,Long,Integer,String,Boolean>> longLongIntStringBool =
                TypeInfoParser.parse("Tuple5<Long,Long,Integer,String,Boolean>");
        TypeInformationSerializationSchema<Tuple5<Long,Long,Integer,String,Boolean>> playerMatchInfoSerSchema =
                new TypeInformationSerializationSchema<>(longLongIntStringBool, env.getConfig());


           /////////////////////////////////////////////////////////////
          // Write some of the DataStreams generated above to Kafka, //
         // for the downstream Flink processing, e.g. hero-result   //
        /////////////////////////////////////////////////////////////

        // "match-simple"
        DataStreamSink<String> matchSimpleSink = matchSimple.addSink(
                        new FlinkKafkaProducer010<>(
                        "match-simple",
                        new SimpleStringSchema(),
                        properties));
        matchSimpleSink.name("match-simple-to-kafka");

        ////////////////////////////
        // Calculate the win rate //
        ////////////////////////////

        // 1. Calculate the win rate for each hero.
        //    Format: <hero_id:String, win rate:Float, time_stamp:Long>
        DataStream<Tuple3<String, Integer, Long>> heroResultTimed = heroResult
                .assignTimestampsAndWatermarks(new HeroResultTimeStampGenerator());

        DataStream<Tuple3<String,Float,Long>> heroWinRate = heroResultTimed
                .keyBy(0)
                .countWindow(100)
                .aggregate(new WinRateAggregator());

        heroResultTimed.print();

        // 2. Calculate the win rate for each hero-pair.
        //    Format: <hero_id1,hero_id2:String, win rate:Float, time_stamp:Long>
        DataStream<Tuple3<String, Integer, Long>> heroPairResultTimed = heroPairResult
                .assignTimestampsAndWatermarks(new HeroResultTimeStampGenerator());

        DataStream<Tuple3<String,Float,Long>> heroPairWinRate = heroPairResultTimed
                .keyBy(0)
                .countWindow(25)
//                .window(TumblingEventTimeWindows.of(Time.minutes(30)))
//                .window(TumblingProcessingTimeWindows.of(Time.minutes(1)))
//                .window(TumblingEventTimeWindows.of(Time.hours(12)))
                .aggregate(new WinRateAggregator());

        // 3. Calculate the win rate for each hero-counter-pair.
        //    Format: <hero_id1,hero_id2:String, win rate:Float, time_stamp:Long>
        DataStream<Tuple3<String, Integer, Long>> heroCounterPairResultTimed = heroCounterPairResult
                .assignTimestampsAndWatermarks(new HeroResultTimeStampGenerator());

        DataStream<Tuple3<String,Float,Long>> heroCounterPairWinRate = heroCounterPairResultTimed
                .keyBy(0)
                .countWindow(25)
//                .window(TumblingEventTimeWindows.of(Time.minutes(30)))
//                .window(TumblingEventTimeWindows.of(Time.hours(12)))
                .aggregate(new WinRateAggregator());


        /////////////////////////////////////////////////
        // Send the win rate result to Kafka as String //
        //  For Redis to use                           //
        /////////////////////////////////////////////////

        // Convert from Tuple3 to String, then send to Kafka, for Redis to use.
        DataStream<String> heroWinRateString = heroWinRate
                .map(new HeroWinRateString());

        DataStreamSink<String> heroWinRateSink = heroWinRateString.addSink(
                new FlinkKafkaProducer010<>(
                        "hero-win-rate",
                        new SimpleStringSchema(),
                        properties));
        heroWinRateSink.name("hero-win-rate-to-kafka");

        DataStream<String> heroPairWinRateString = heroPairWinRate
                .map(new HeroWinRateString());

        DataStreamSink<String> heroPairWinRateSink = heroPairWinRateString.addSink(
                new FlinkKafkaProducer010<>(
                        "hero-pair-win-rate",
                        new SimpleStringSchema(),
                        properties));
        heroPairWinRateSink.name("hero-pair-win-rate-to-kafka");

        DataStream<String> heroCounterPairWinRateString = heroCounterPairWinRate
                .map(new HeroCounterPairWinRateString());

        DataStreamSink<String> heroCounterPairWinRateSink = heroCounterPairWinRateString.addSink(
                new FlinkKafkaProducer010<>(
                        "hero-counter-pair-win-rate",
                        new SimpleStringSchema(),
                        properties));
        heroCounterPairWinRateSink.name("hero-counter-pair-win-rate-to-kafka");


        /////////////////////////////////////////////////////////
        // Send hero win rate and count with date to Cassandra //
        /////////////////////////////////////////////////////////

        DataStream<Tuple4<String,Float,Long,Long>> heroWinRateDaily = heroResultTimed
                .keyBy(0)
                .window(TumblingEventTimeWindows.of(Time.days(1)))
                .aggregate(new WinRateAggregatorWithCount());

        // Generate year, month and day from timestamp.
        // <hero_id,year,month,day,win_rate,count>
        DataStream<Tuple6<String,Integer,Integer,Integer,Float,Long>> heroWinRateToCas =
                heroWinRateDaily.map(new HeroWinRateGenerateYearMonthDayWithCount());

        CassandraSink
                .addSink(heroWinRateToCas)
                .setQuery(
                        "INSERT INTO "+CASSANDRA_KEYSPACE+".hero_win_rate" +
                                "(hero_id, year, month, day, win_rate, count) values (?, ?, ?, ?, ?, ?);")
                .setHost(urls.CASSANDRA_URL)
                .build();



          /////////////////////////////////////////////////////////////
         // Process and save the region-num-of-players to Cassandra //
        /////////////////////////////////////////////////////////////

        //     Calculate the number of players currently in-game in each region/cluster
        //     Since the regionInfo DataStream is in the format of <cluster_id, time, +10/-10>,
        //     we can simply use an aggregation function over time.

        DataStream<Tuple3<Integer,Long,Long>> regionNumOfPlayers = regionInfo
                .assignTimestampsAndWatermarks(new RegionInfoTimeStampGenerator())
                .keyBy(0)
                .window(TumblingProcessingTimeWindows.of(Time.minutes(5)))
//                .window(TumblingEventTimeWindows.of(Time.days(1)))
                .reduce((a,b) -> new Tuple3<>(a.f0,Math.max(a.f1,b.f1), a.f2+b.f2));

        // Rewrite regionNumOfPlayers into the format that the table
        // in Cassandra has:
        //      <cluster_id:Integer,year:Integer,month:Integer,day:Integer,num_of_players:Long>
        DataStream<Tuple5<Integer,Integer,Integer,Integer,Long>> regionNumOfPlayersToCas =
                regionNumOfPlayers.map(new RegionNumOfPlayersGenerateYearMonthDay());

        // Write the result to Cassandra
        CassandraSink.addSink(regionNumOfPlayersToCas)
                .setQuery(
                        "INSERT INTO "+CASSANDRA_KEYSPACE+".region_num_of_players" +
                                "(cluster_id, year, month, day, num_of_players) values (?, ?, ?, ?, ?);")
                .setHost(urls.CASSANDRA_URL)
                .build();

          /////////////////////////////////////////////
         // Save the player-match-info to Cassandra //
        /////////////////////////////////////////////

        // Generate year, month, day from playerMatchInfo
        // <account_id:Long,year:Integer,month:Integer,day:Integer,start_time:Timestamp,
        //  duration:Integer,hero_id:text,win:Boolean>

        DataStream<Tuple8<Long,Integer,Integer,Integer,Long,Integer,String,Boolean>> playerMatchInfoToCas =
                playerMatchInfo.map(new PlayerMatchInfoGenerateYearMonthDay());

        CassandraSink.addSink(playerMatchInfoToCas)
                .setQuery(
                        "INSERT INTO "+CASSANDRA_KEYSPACE+".player_match" +
                                "(account_id, year, month, day, start_time, duration, hero_id, win)" +
                                " values (?, ?, ?, ?, ?, ?, ?, ?);")
                .setHost(urls.CASSANDRA_URL)
                .build();

        ////////////////////////////////////////////////////
        // Generate and save dailyPlayerInfo to Cassandra //
        ////////////////////////////////////////////////////

        // Generate (date:String, account_id:Long, time_:Long, hero_id:String, duration:Integer) for each player,
        // to speed up the calculation of DAU and MAU in Spark

        // This part could be optimized in the way that instead of transforming from playerMatchInfo,
        // just directly use playerMatchInfoToCas, with the date and account_id swapped.
        DataStream<Tuple7<Integer,Integer,Integer,Long,Long,String,Integer> > dailyPlayerInfo =
                playerMatchInfo.map(new dailyPlayerInfoMapper());

        // Save to Cassandra

        CassandraSink.addSink(dailyPlayerInfo)
                .setQuery(
                        "INSERT INTO "+CASSANDRA_KEYSPACE+".daily_player" +
                                "(year, month, day, account_id, time_, hero_id, duration)" +
                                " values (?, ?, ?, ?, ?, ?, ?);")
                .setHost(urls.CASSANDRA_URL)
                .build();

          /////////////
         // Execute //
        /////////////

        env.execute("JSON Parser");

    }

    public static class BoundedOutOfOrdernessGenerator implements AssignerWithPeriodicWatermarks<SingleMatch> {

        private final long maxOutOfOrderness = 90*60; // 90 min out of order allowed.

        private long currentMaxTimestamp;

        @Override
        public long extractTimestamp(SingleMatch match, long previousElementTimestamp) {
            long timestamp = match.getStart_time();
            currentMaxTimestamp = Math.max(timestamp, currentMaxTimestamp);
            return timestamp;
        }

        @Override
        public Watermark getCurrentWatermark() {
            // return the watermark as current highest timestamp minus the out-of-orderness bound
            return new Watermark(currentMaxTimestamp - maxOutOfOrderness);
        }
    }

    public static class RegionInfoTimeStampGenerator implements AssignerWithPeriodicWatermarks<Tuple3<Integer,Long,Long>> {

        private final long maxOutOfOrderness = 600; // 10 min out of order allowed.

        private long currentMaxTimestamp;

        @Override
        public long extractTimestamp(Tuple3<Integer,Long,Long> regionInfo, long previousElementTimestamp) {
            long timestamp = regionInfo.f1;
            currentMaxTimestamp = Math.max(timestamp, currentMaxTimestamp);
            return timestamp;
        }

        @Override
        public Watermark getCurrentWatermark() {
            // return the watermark as current highest timestamp minus the out-of-orderness bound
            return new Watermark(currentMaxTimestamp - maxOutOfOrderness);
        }
    }

    private static SingleMatch parseMatch(String rawJSONString) {
        /*
            A simple wrapper of the fromJson() method in the Gson package.
            Input: a game match in the form of Json string.
            Output: the structured SingleMatch instance.
         */
        SingleMatch match;
        try {
            match = new Gson().fromJson(rawJSONString, SingleMatch.class);
        }
        catch(Exception e) {
            match = null;
        }
        if (match == null) {
            JSONParser.INVALID_MATCHES += 1;
//            if (JSONParser.INVALID_MATCHES % 100 == 0)
//                System.out.println("Number of invalid matches got: " + String.valueOf(JSONParser.INVALID_MATCHES));
        };
        return match;
    }

    public static class HeroResultExtractor implements FlatMapFunction<SingleMatch, Tuple3<String, Integer, Long>> {
        /*
            Given a match, extract the ten <hero_id, win/lose, start_time> tuples.
         */
        @Override
        public void flatMap(SingleMatch match, Collector<Tuple3<String, Integer, Long>> out) throws Exception {
            ArrayList<SingleMatch.PlayerInMatch> players = match.getPlayers();
            Boolean radianWin = match.isRadiant_win();
            Long startTime = match.getStart_time();

            for (int i=0; i<players.size(); i++) {
                SingleMatch.PlayerInMatch player = players.get(i);
                String hero_id = String.valueOf(player.getHero_id());
                Integer win = ((radianWin && i < 5) || (!radianWin && i >= 5))? 1 : 0;
                out.collect(new Tuple3<>(hero_id, win, startTime));
            }
        }
    }

    public static class HeroPairResultExtractor implements FlatMapFunction<SingleMatch, Tuple4<String, String, Integer, Long>> {
        /*
            Given a match, extract the <hero_id1, hero_id2, win/lose, start_time> tuples.
            Here hero_id1 and hero_id2 are on the same side.
         */
        @Override
        public void flatMap(SingleMatch match, Collector<Tuple4<String, String, Integer, Long>> out) throws Exception {
            ArrayList<SingleMatch.PlayerInMatch> players = match.getPlayers();
            Boolean radianWin = match.isRadiant_win();
            Long timestamp = match.getStart_time();

            if (players.size() != 10) {
                // Although we have implemented filter() to guarantee the match validity
                // Here we still validate it once more.
                out.collect(new Tuple4<String, String, Integer, Long>("0","0",-1, (long) -1));
                return;
            }

            // Radiant team.
            for (int i=0; i<5; i++) {
                for (int j=0; j<5; j++) {
                    if (i != j) {
                        String hero_id1 = String.valueOf(players.get(i).getHero_id());
                        String hero_id2 = String.valueOf(players.get(j).getHero_id());
                        Integer win = radianWin? 1 : 0;
                        out.collect(new Tuple4<>(hero_id1,hero_id2,win,timestamp));
                    }
                }
            }

            // Dire team.
            for (int i=5; i<players.size(); i++) {
                for (int j=5; j<players.size(); j++) {
                    if (i != j) {
                        String hero_id1 = String.valueOf(players.get(i).getHero_id());
                        String hero_id2 = String.valueOf(players.get(j).getHero_id());
                        Integer win = radianWin? 0 : 1;
                        out.collect(new Tuple4<>(hero_id1,hero_id2,win,timestamp));
                    }
                }
            }
        }
    }


    public static class HeroCounterPairResultExtractor implements FlatMapFunction<SingleMatch, Tuple4<String, String, Integer, Long>> {
        /*
            Given a match, extract the <hero_id1, hero_id2, win/lose, start_time> tuples.
            Here hero_id1 and hero_id2 are on the opposite side.
         */
        @Override
        public void flatMap(SingleMatch match, Collector<Tuple4<String, String, Integer, Long>> out) throws Exception {
            ArrayList<SingleMatch.PlayerInMatch> players = match.getPlayers();
            Boolean radianWin = match.isRadiant_win();
            Long timestamp = match.getStart_time();

            if (players.size() != 10) {
                // Although we have implemented filter() to guarantee the match validity
                // Here we still validate it once more.
                out.collect(new Tuple4<String, String, Integer, Long>("0","0",-1, (long) -1));
                return;
            }

            // Generate hero counter pairs.
            for (int i=0; i<5; i++) {
                for (int j=5; j<players.size(); j++)  {
                        String hero_id1 = String.valueOf(players.get(i).getHero_id());
                        String hero_id2 = String.valueOf(players.get(j).getHero_id());
                        Integer rWin = radianWin? 1 : 0;
                        out.collect(new Tuple4<>(hero_id1,hero_id2,rWin,timestamp));
                        out.collect(new Tuple4<>(hero_id2,hero_id1,1-rWin,timestamp));
                }
            }
        }
    }

    private static class RegionInfoFlatMapper implements FlatMapFunction<SingleMatch, Tuple3<Integer, Long, Long>>  {
        /*
            Extract the region/cluster information from the match:
            <cluster_id, start_time, 1>
            See the comments above "regionInfo" data stream in the main function for the details.
         */

        @Override
        public void flatMap(SingleMatch match, Collector<Tuple3<Integer, Long, Long>> out) throws Exception {
            Integer cluster = match.getCluster();
            out.collect(new Tuple3<>(cluster, match.getStart_time(),10L));
//            out.collect(new Tuple3<>(cluster, end_time,-10L));
        }
    }


    public static class PlayerMatchInfoExtractor implements FlatMapFunction<SingleMatch, Tuple5<Long,Long,Integer,String,Boolean>> {
        /*
            Given a match, extract the ten player's data: <account_id,start_time,duration,hero_id,win/lose>
            Note that account_id=4294967295 actually are the players that set their account as "private",
            so we exclude them here.
         */
        @Override
        public void flatMap(SingleMatch match, Collector<Tuple5<Long,Long,Integer,String,Boolean>> out) throws Exception {
            ArrayList<SingleMatch.PlayerInMatch> players = match.getPlayers();

            Boolean radianWin = match.isRadiant_win();
//            Timestamp startTime = match.getStartTimeInTimestamp();
            Long startTime = match.getStart_time();
            Integer duration = match.getDuration();

            for (int i=0; i<players.size(); i++) {

                SingleMatch.PlayerInMatch player = players.get(i);
                Long accountId = player.getAccount_id();
                String heroId = String.valueOf(player.getHero_id());
                Boolean win = (i < 5)? radianWin : !radianWin;

                // Exclude accounts that are set to be private
                if (accountId != 4294967295L)
                    out.collect(new Tuple5<>(accountId,startTime,duration,heroId,win));
            }
        }
    }

    private static class TwoHeroIdsCombiner
            implements MapFunction<Tuple4<String, String, Integer, Long>, Tuple3<String, Integer, Long>> {

        // <hero_id1,hero_id2,win/lose,start_time> ===> <"hero_id1,hero_id2",win/lose,start_time>
        @Override
        public Tuple3<String, Integer, Long> map(Tuple4<String, String, Integer, Long> v) throws Exception {
            return new Tuple3<>(v.f0+","+v.f1,v.f2,v.f3);
        }
    }
    private static class RegionPlayerAggregator
            implements AggregateFunction<Tuple3<Integer,Long,Integer>, Tuple3<Integer,Long,Long>, Tuple3<Integer,Long,Long>> {

        // Calculate the current in-game players in a specific region/cluster
        // Input/value: <cluster_id:Integer,time:Long,+10/-10:Integer>
        // Accumulator: <cluster_id:Integer,latest_time:Long,current_players:Long>
        // Result: <cluster_id:Integer,time:Long,current_players:Long>

        @Override
        public  Tuple3<Integer,Long,Long> createAccumulator() {
            return new Tuple3<>(0,0L, 0L);
        }

        @Override
        public  Tuple3<Integer,Long,Long> add( Tuple3<Integer,Long,Integer> value,  Tuple3<Integer,Long,Long> accumulator) {
            // Update accumulator with the new value. Here we update the start_time to be the latest
            return new Tuple3<>(value.f0,
                    Math.max(value.f1,accumulator.f1),
                    accumulator.f2 + value.f2);
        }

        @Override
        public Tuple3<Integer,Long,Long> getResult( Tuple3<Integer,Long,Long> accumulator) {
            return accumulator;
        }

        @Override
        public Tuple3<Integer,Long,Long> merge( Tuple3<Integer,Long,Long> a,  Tuple3<Integer,Long,Long> b) {
            return new Tuple3<>(a.f0, Math.max(a.f1,b.f1), a.f2 + b.f2);
        }
    }

    private static class PlayerMatchInfoLongToTimestamp
            implements MapFunction<Tuple5<Long,Long,Integer,String,Boolean>, Tuple5<Long,Timestamp,Integer,String,Boolean>> {
        /*
            Convert the timestamp in playerMatchInfo DataStream from Long to Timestamp
            to save into Cassandra
         */
        @Override
        public Tuple5<Long,Timestamp,Integer,String,Boolean> map(Tuple5<Long,Long,Integer,String,Boolean> v) throws Exception {
            return new Tuple5<>(v.f0,new Timestamp(v.f1),v.f2,v.f3,v.f4);
        }
    }

    private static class RegionNumOfPlayersLongToTimestamp
            implements MapFunction<Tuple3<Integer,Long,Long>, Tuple3<Integer,Timestamp,Long>> {
        /*
            Convert the timestamp in region-info DataStream from Long to Timestamp
            to save into Cassandra
         */
        @Override
        public Tuple3<Integer,Timestamp,Long> map(Tuple3<Integer,Long,Long>v) throws Exception {
            return new Tuple3<>(v.f0,new Timestamp(v.f1),v.f2);
        }
    }

    ///////////////////////////
    // Hero win rate related //
    ///////////////////////////
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

    private static class WinRateAggregatorWithCount
            implements AggregateFunction<Tuple3<String, Integer, Long>, Tuple4<String, Long, Long, Long>, Tuple4<String, Float, Long, Long>> {

        // Similar to WinRateAggregator, but also count the number of occurences.
        // Input: <hero_id:String,win/lose:Integer,start_time:Long>
        // Accumulator: <hero_id:String,total_win:Long,total_played:Long,latest_start_time:Long>
        // Output: <hero_id:String,win_rate:Float,count:Long,latest_start_time:Long>

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
        public Tuple4<String, Float, Long, Long> getResult(Tuple4<String, Long, Long, Long> accumulator) {
            return new Tuple4<>(accumulator.f0, Float.valueOf(accumulator.f1) / accumulator.f2, accumulator.f2, accumulator.f3);
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
            implements MapFunction<Tuple3<String,Float,Long>, String> {
        /*
            Convert heroWinRate from Tuple3 to String
         */
        @Override
        public String map(Tuple3<String,Float,Long> v) throws Exception {
            return v.f0+","+String.format("%.3f", v.f1);
        }
    }

    private static class HeroCounterPairWinRateString
            implements MapFunction<Tuple3<String,Float,Long>, String> {
        /*
            Convert heroCounterPairWinRate from Tuple3 to String
         */
        @Override
        public String map(Tuple3<String,Float,Long> v) throws Exception {
            return "counter,"+v.f0+","+String.format("%.3f", v.f1);
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

    private static class HeroWinRateGenerateYearMonthDayWithCount
            implements MapFunction<Tuple4<String,Float,Long,Long>, Tuple6<String,Integer,Integer,Integer,Float,Long>> {
        /*
            Generate year, month and day from the timestamp (long). For Cassandra.
         */
        @Override
        public Tuple6<String,Integer,Integer,Integer,Float,Long> map(Tuple4<String,Float,Long,Long> v) throws Exception {
            Calendar cal = Calendar.getInstance();
            cal.setTimeInMillis(v.f3);
            return new  Tuple6<>(
                    v.f0,cal.get(Calendar.YEAR),cal.get(Calendar.MONTH),cal.get(Calendar.DAY_OF_MONTH),
                    v.f1,v.f2
            );
        }
    }

    private static class RegionNumOfPlayersGenerateYearMonthDay
            implements MapFunction<Tuple3<Integer,Long,Long>, Tuple5<Integer,Integer,Integer,Integer,Long>> {
        /*
            Generate year, month and day from the timestamp (long). For Cassandra.
         */
        @Override
        public Tuple5<Integer,Integer,Integer,Integer,Long> map(Tuple3<Integer,Long,Long> v) throws Exception {
            Calendar cal = Calendar.getInstance();
            cal.setTimeInMillis(v.f1);
            return new  Tuple5<>(
                    v.f0,cal.get(Calendar.YEAR),cal.get(Calendar.MONTH),cal.get(Calendar.DAY_OF_MONTH),v.f2
            );
        }
    }

    private static class PlayerMatchInfoGenerateYearMonthDay
            implements MapFunction<Tuple5<Long,Long,Integer,String,Boolean>,
            Tuple8<Long,Integer,Integer,Integer,Long,Integer,String,Boolean>> {
        /*
            Generate year, month and day from the timestamp (long). For Cassandra.
         */
        @Override
        public Tuple8<Long,Integer,Integer,Integer,Long,Integer,String,Boolean>
        map(Tuple5<Long,Long,Integer,String,Boolean> v) throws Exception {
            Calendar cal = Calendar.getInstance();
            cal.setTimeInMillis(v.f1);
            return new  Tuple8<>(
                    v.f0,cal.get(Calendar.YEAR),cal.get(Calendar.MONTH),cal.get(Calendar.DAY_OF_MONTH),
                    v.f1, v.f2, v.f3, v.f4
            );
        }
    }

    private static class dailyPlayerInfoMapper
            implements MapFunction<Tuple5<Long,Long,Integer,String,Boolean>,
            Tuple7<Integer,Integer,Integer,Long,Long,String,Integer> > {
        /*
            Mapper for players' data. From <account_id, start_time, duration, hero_id, win/lose>
            to (date:String, account_id:Long, timestamp:Long, hero_id:String, duration:Integer)
         */

        @Override
        public Tuple7<Integer,Integer,Integer,Long,Long,String,Integer> map(Tuple5<Long,Long,Integer,String,Boolean> v) throws Exception {
            Calendar cal = Calendar.getInstance();
            cal.setTimeInMillis(v.f1);
            return new  Tuple7<>(
                    cal.get(Calendar.YEAR),cal.get(Calendar.MONTH),cal.get(Calendar.DAY_OF_MONTH),
                    v.f0, v.f1, v.f3, v.f2
            );
        }
    }

}
