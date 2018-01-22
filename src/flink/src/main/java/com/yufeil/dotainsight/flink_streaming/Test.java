package com.yufeil.dotainsight.flink_streaming;

import com.yufeil.dotainsight.utils.HostURLs;
import redis.clients.jedis.Jedis;

public class Test {
    public static void main(String[] args) {
        HostURLs urls = new HostURLs();
        Jedis jedis = new Jedis(urls.REDIS_URL);

        jedis.set("1","0.5");
    }
}
