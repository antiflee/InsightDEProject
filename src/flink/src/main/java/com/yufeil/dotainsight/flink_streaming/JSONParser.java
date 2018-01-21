package flink_streaming;

/*
    Read from topic "match-raw-json" from Kafka,
    Parse it and send corresponding data to the following topics in Kafka:

    (1) "match-simple":
            "hero_id1,hero_id2,...hero_id10,radiant win,timestamp"
    (2) "hero-result":
            "hero_id,win/lose,timestamp"
    (3) "hero-pair-result":
            "hero_id1,hero_id2,win/lose"
    (4) "hero-counter-pair-result":
            "hero_id1,hero_id2,hero1 win/lose"
    (5) "region-info":
            "region,timestamp,duration"
    (6) "player-match-info":
            "account_id,timestamp,duration,hero_id,win/lose"
 */

import org.apache.flink.api.java.utils.ParameterTool;
import HostURLs;

import java.util.Properties;

public class JSONParser {

    final static private int WINDOWLENGTH = 5000;   // in milliseconds.
    final static private int SLIDELENGTH = 2000;   // in milliseconds.

    public static void main(String[] args) {

        HostURLs

        final ParameterTool parameterTool = ParameterTool.fromArgs(args);

        Properties properties = new Properties();
        properties.put("bootstrap.servers", ":9092"));
        properties.put("zookeeper.connect", parameterTool.get("zookeeper.connect",ZOOKEEPER_URL+":2181"));
        properties.put("group.id", parameterTool.get("group.id","flink-streaming-hero"));
        properties.put("input-topic", parameterTool.get("input-topic", "hero-result"));
        properties.put("output-topic", parameterTool.get("output-topic", "hero-win-rate"));
    }
}
