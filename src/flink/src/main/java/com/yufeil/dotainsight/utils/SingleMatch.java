package com.yufeil.dotainsight.utils;

import org.apache.flink.api.java.tuple.Tuple2;

import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;

public class SingleMatch {
    // The data structure of a single DOTA2 match returned by GetMatchesBySeqNum
    // The example can be found in the 'matches' list in this link:
    //    https://api.steampowered.com/IDOTA2Match_570/GetMatchHistoryBySequenceNum/V001/?skill=3&key=<APIKEY>&start_at_match_seq_num=2670080000&matches_requested=50
    // An example:
    /*
        players: ArrayList<PlayerInMatch>
        radiant_win: true,
        duration: 2468,
        pre_game_duration: 90,
        start_time: 1489637693,
        match_id: 3057715953,
        match_seq_num: 2670080000,
        tower_status_radiant: 2038,
        tower_status_dire: 0,
        barracks_status_radiant: 63,
        barracks_status_dire: 0,
        cluster: 223,
        first_blood_time: 49,
        lobby_type: 7,
        human_players: 10,
        leagueid: 0,
        positive_votes: 0,
        negative_votes: 0,
        game_mode: 3,
        flags: 1,
        engine: 1
     */

    private ArrayList<PlayerInMatch> players;
    private boolean radiant_win;
    private int duration;
    private int pre_game_duration;
    private long start_time;
    private long match_id;
    private long match_seq_num;
    private int tower_status_radiant;
    private int tower_status_dire;
    private int barracks_status_radiant;
    private int barracks_status_dire;
    private int cluster;
    private int first_blood_time;
    private int lobby_type;
    private int human_players;
    private int leagueid;
    private int positive_votes;
    private int negative_votes;
    private int game_mode;
    private int flags;
    private int engine;

    // Some constants for the validation of the DOTA2 match.
    // See isValidMatch() method.
    private static final Set<Integer> VALID_GAMEMODES = new HashSet<> (Arrays.asList(1,2,3,5,16,22));
    private static final int VALID_HUMAN_PLAYERS = 10;
    private static final int MIN_VALID_DURATION = 1000; // in seconds.


    public SingleMatch(boolean radiant_win, int duration, long start_time, long match_id, long match_seq_num) {
        this.radiant_win = radiant_win;
        this.duration = duration;
        this.start_time = start_time;
        this.match_id = match_id;
        this.match_seq_num = match_seq_num;
    }

    public ArrayList<PlayerInMatch> getPlayers() {
        return players;
    }

    public void setPlayers(ArrayList<PlayerInMatch> players) {
        this.players = players;
    }

    public boolean isRadiant_win() {
        return radiant_win;
    }

    public void setRadiant_win(boolean radiant_win) {
        this.radiant_win = radiant_win;
    }

    public int getDuration() {
        return duration;
    }

    public void setDuration(int duration) {
        this.duration = duration;
    }

    public int getPre_game_duration() {
        return pre_game_duration;
    }

    public void setPre_game_duration(int pre_game_duration) {
        this.pre_game_duration = pre_game_duration;
    }

    public long getStart_time() {
        // Note here we convert the timestamp unit from second to millisecond.
        // This is because Flink uses millisecond unit for the timestamp watermark.
        return start_time * 1000;
    }

    public void setStart_time(long start_time) {
        this.start_time = start_time;
    }

    public long getMatch_id() {
        return match_id;
    }

    public void setMatch_id(long match_id) {
        this.match_id = match_id;
    }

    public long getMatch_seq_num() {
        return match_seq_num;
    }

    public void setMatch_seq_num(long match_seq_num) {
        this.match_seq_num = match_seq_num;
    }

    public int getTower_status_radiant() {
        return tower_status_radiant;
    }

    public void setTower_status_radiant(int tower_status_radiant) {
        this.tower_status_radiant = tower_status_radiant;
    }

    public int getTower_status_dire() {
        return tower_status_dire;
    }

    public void setTower_status_dire(int tower_status_dire) {
        this.tower_status_dire = tower_status_dire;
    }

    public int getBarracks_status_radiant() {
        return barracks_status_radiant;
    }

    public void setBarracks_status_radiant(int barracks_status_radiant) {
        this.barracks_status_radiant = barracks_status_radiant;
    }

    public int getBarracks_status_dire() {
        return barracks_status_dire;
    }

    public void setBarracks_status_dire(int barracks_status_dire) {
        this.barracks_status_dire = barracks_status_dire;
    }

    public int getCluster() {
        return cluster;
    }

    public void setCluster(int cluster) {
        this.cluster = cluster;
    }

    public int getFirst_blood_time() {
        return first_blood_time;
    }

    public void setFirst_blood_time(int first_blood_time) {
        this.first_blood_time = first_blood_time;
    }

    public int getLobby_type() {
        return lobby_type;
    }

    public void setLobby_type(int lobby_type) {
        this.lobby_type = lobby_type;
    }

    public int getHuman_players() {
        return human_players;
    }

    public void setHuman_players(int human_players) {
        this.human_players = human_players;
    }

    public int getLeagueid() {
        return leagueid;
    }

    public void setLeagueid(int leagueid) {
        this.leagueid = leagueid;
    }

    public int getPositive_votes() {
        return positive_votes;
    }

    public void setPositive_votes(int positive_votes) {
        this.positive_votes = positive_votes;
    }

    public int getNegative_votes() {
        return negative_votes;
    }

    public void setNegative_votes(int negative_votes) {
        this.negative_votes = negative_votes;
    }

    public int getGame_mode() {
        return game_mode;
    }

    public void setGame_mode(int game_mode) {
        this.game_mode = game_mode;
    }

    public int getFlags() {
        return flags;
    }

    public void setFlags(int flags) {
        this.flags = flags;
    }

    public int getEngine() {
        return engine;
    }

    public void setEngine(int engine) {
        this.engine = engine;
    }

    public Timestamp getStartTimeInTimestamp() {
        // Convert the start_time from long to Timestamp, in seconds
        return new Timestamp(this.start_time);
    }

    public boolean isValidMatch() {
        /*
            Validate if the match is valid. Valid means:
            (1) human_players == 10;
            (2) duration >= 1000;
            (3) game_mode in (1,2,3,5,16,22)
         */
        if (this.human_players== SingleMatch.VALID_HUMAN_PLAYERS
                && this.duration >= SingleMatch.MIN_VALID_DURATION
                && SingleMatch.VALID_GAMEMODES.contains(this.game_mode)) {
            return true;
        }
        return false;
    }

    public String simplifyMatch() {
        /*
            Returns a string formatted as:
                hero_id * 10 + radiant_win (true:1, false:0) + timestamp, delimiter = " "
         */
        if (!this.isValidMatch()) {
            return "";
        }

        String result = "";

        int count = 0;

        for (PlayerInMatch player: this.players) {
            result += String.valueOf(player.hero_id) + " ";
            count += 1;
        }

        result += this.radiant_win? "1" : "0";

        if (count != 10) {
            // Although we have validated the match already,
            // in case the number of players not equal to 10, discard the result
            result = "";
        }

        result += this.getStart_time();

        return result;
    }

    public ArrayList<Tuple2<String, Integer>> extractHeroResult() {
        /*
            Given a match, extract the ten <hero_id, win/lose> tuples.
         */
        ArrayList<Tuple2<String, Integer>> result = null;

        for (int i=0; i<this.players.size(); i++) {
            PlayerInMatch player = players.get(i);
            String hero_id = String.valueOf(player.hero_id);
            Integer win = ((this.radiant_win && i < 5) || (!this.radiant_win && i >= 5))? 1 : 0;
            result.add(new Tuple2<>(hero_id, win));
        }

        return result;
    }

    // Details of each hero in the match
    public class PlayerInMatch {
        /*
            An example:
                account_id: 201080081,
                player_slot: 0,
                hero_id: 87
         */

        private long account_id;
        private int player_slot;
        private int hero_id;

        public PlayerInMatch(long account_id, int player_slot, int hero_id) {
            this.account_id = account_id;
            this.player_slot = player_slot;
            this.hero_id = hero_id;
        }

        public long getAccount_id() {
            return account_id;
        }

        public void setAccount_id(long account_id) {
            this.account_id = account_id;
        }

        public int getPlayer_slot() {
            return player_slot;
        }

        public void setPlayer_slot(int player_slot) {
            this.player_slot = player_slot;
        }

        public int getHero_id() {
            return hero_id;
        }

        public void setHero_id(int hero_id) {
            this.hero_id = hero_id;
        }

    }

}
