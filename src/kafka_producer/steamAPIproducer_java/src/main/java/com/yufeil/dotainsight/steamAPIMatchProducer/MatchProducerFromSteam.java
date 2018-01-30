package com.yufeil.dotainsight.steamAPIMatchProducer;

/*
    This producer reads from Steam Web API, and send matches to Kafka in the format of JSON
    Example of the produced data:
        https://api.steampowered.com/IDOTA2Match_570/GetMatchHistoryBySequenceNum/V001/?key=<APIKEY>&start_at_match_seq_num=2670080000&matches_requested=50
*/


public class MatchProducerFromSteam {

    private static final int TIME_TO_SLEEP      = 2000; // Time to wait between two URL queries, in milliseconds .
    private static final String TOPIC_NAME_1    = "match-detailed";
    private static final String TOPIC_NAME_2    = "match-simple";
    private static final long STARTING_SEQ_NUM  = 3075387892L;
    private static final String D2APIKEY1       =

    public static void main(String[] args) {

    }
}
