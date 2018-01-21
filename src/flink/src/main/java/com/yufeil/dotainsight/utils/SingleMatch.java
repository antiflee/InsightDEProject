package com.yufeil.dotainsight.flink_streaming;

import com.yufeil.dotainsight.utils.HostURLs;import java.util.ArrayList;
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
        engine: 1,
        radiant_score: 60,
        dire_score: 38
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
    private int radiant_score;
    private int dire_score;

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
        return start_time;
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

    public int getRadiant_score() {
        return radiant_score;
    }

    public void setRadiant_score(int radiant_score) {
        this.radiant_score = radiant_score;
    }

    public int getDire_score() {
        return dire_score;
    }

    public void setDire_score(int dire_score) {
        this.dire_score = dire_score;
    }

    public boolean isValidMatch() {
        /*
            Validate if the match is valid. Valid means:
            (1) human_players == 10;
            (2) duration >= 1000;
            (3) game_mode in (1,2,3,5,16,22)
         */
        if (this.human_players== HostURLs.SingleMatch.VALID_HUMAN_PLAYERS
                && this.duration >= HostURLs.SingleMatch.MIN_VALID_DURATION
                && HostURLs.SingleMatch.VALID_GAMEMODES.contains(this.game_mode)) {
            return true;
        }
        return false;
    }

    public String simplifyMatch() {
        /*
            Returns a string formatted as:
                hero_id * 10 + radiant_win (true:1, false:0), delimiter = " "
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

        return result;
    }

    public String simplyMatchWithStartTime() {
        /*
            Returns the simplified version of the match with its start_time
         */
        return this.simplifyMatch() + " " + String.valueOf(this.getStart_time());
    }


    // Details of each hero in the match
    private class PlayerInMatch {
        /*
            An example:
                account_id: 201080081,
                player_slot: 0,
                hero_id: 87,
                item_0: 36,
                item_1: 102,
                item_2: 37,
                item_3: 100,
                item_4: 180,
                item_5: 232,
                backpack_0: 43,
                backpack_1: 46,
                backpack_2: 0,
                kills: 12,
                deaths: 10,
                assists: 25,
                leaver_status: 0,
                last_hits: 47,
                denies: 9,
                gold_per_min: 389,
                xp_per_min: 560,
                level: 23,
                hero_damage: 20666,
                tower_damage: 781,
                hero_healing: 0,
                gold: 2293,
                gold_spent: 12900,
                scaled_hero_damage: 15025,
                scaled_tower_damage: 514,
                scaled_hero_healing: 0,
                ability_upgrades: ArrayList<AbilityUpgrade>
         */

        private long account_id;
        private int player_slot;
        private int hero_id;
        private int item_0;
        private int item_1;
        private int item_2;
        private int item_3;
        private int item_4;
        private int item_5;
        private int backpack_0;
        private int backpack_1;
        private int backpack_2;
        private int kills;
        private int deaths;
        private int assists;
        private int leaver_status;
        private int last_hits;
        private int denies;
        private int gold_per_min;
        private int xp_per_min;
        private int level;
        private int hero_damage;
        private int tower_damage;
        private int hero_healing;
        private int gold;
        private int gold_spent;
        private int scaled_hero_damage;
        private int scaled_tower_damage;
        private int scaled_hero_healing;
        private ArrayList<AbilityUpgrade> ability_upgrades;

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

        public int getItem_0() {
            return item_0;
        }

        public void setItem_0(int item_0) {
            this.item_0 = item_0;
        }

        public int getItem_1() {
            return item_1;
        }

        public void setItem_1(int item_1) {
            this.item_1 = item_1;
        }

        public int getItem_2() {
            return item_2;
        }

        public void setItem_2(int item_2) {
            this.item_2 = item_2;
        }

        public int getItem_3() {
            return item_3;
        }

        public void setItem_3(int item_3) {
            this.item_3 = item_3;
        }

        public int getItem_4() {
            return item_4;
        }

        public void setItem_4(int item_4) {
            this.item_4 = item_4;
        }

        public int getItem_5() {
            return item_5;
        }

        public void setItem_5(int item_5) {
            this.item_5 = item_5;
        }

        public int getBackpack_0() {
            return backpack_0;
        }

        public void setBackpack_0(int backpack_0) {
            this.backpack_0 = backpack_0;
        }

        public int getBackpack_1() {
            return backpack_1;
        }

        public void setBackpack_1(int backpack_1) {
            this.backpack_1 = backpack_1;
        }

        public int getBackpack_2() {
            return backpack_2;
        }

        public void setBackpack_2(int backpack_2) {
            this.backpack_2 = backpack_2;
        }

        public int getKills() {
            return kills;
        }

        public void setKills(int kills) {
            this.kills = kills;
        }

        public int getDeaths() {
            return deaths;
        }

        public void setDeaths(int deaths) {
            this.deaths = deaths;
        }

        public int getAssists() {
            return assists;
        }

        public void setAssists(int assists) {
            this.assists = assists;
        }

        public int getLeaver_status() {
            return leaver_status;
        }

        public void setLeaver_status(int leaver_status) {
            this.leaver_status = leaver_status;
        }

        public int getLast_hits() {
            return last_hits;
        }

        public void setLast_hits(int last_hits) {
            this.last_hits = last_hits;
        }

        public int getDenies() {
            return denies;
        }

        public void setDenies(int denies) {
            this.denies = denies;
        }

        public int getGold_per_min() {
            return gold_per_min;
        }

        public void setGold_per_min(int gold_per_min) {
            this.gold_per_min = gold_per_min;
        }

        public int getXp_per_min() {
            return xp_per_min;
        }

        public void setXp_per_min(int xp_per_min) {
            this.xp_per_min = xp_per_min;
        }

        public int getLevel() {
            return level;
        }

        public void setLevel(int level) {
            this.level = level;
        }

        public int getHero_damage() {
            return hero_damage;
        }

        public void setHero_damage(int hero_damage) {
            this.hero_damage = hero_damage;
        }

        public int getTower_damage() {
            return tower_damage;
        }

        public void setTower_damage(int tower_damage) {
            this.tower_damage = tower_damage;
        }

        public int getHero_healing() {
            return hero_healing;
        }

        public void setHero_healing(int hero_healing) {
            this.hero_healing = hero_healing;
        }

        public int getGold() {
            return gold;
        }

        public void setGold(int gold) {
            this.gold = gold;
        }

        public int getGold_spent() {
            return gold_spent;
        }

        public void setGold_spent(int gold_spent) {
            this.gold_spent = gold_spent;
        }

        public int getScaled_hero_damage() {
            return scaled_hero_damage;
        }

        public void setScaled_hero_damage(int scaled_hero_damage) {
            this.scaled_hero_damage = scaled_hero_damage;
        }

        public int getScaled_tower_damage() {
            return scaled_tower_damage;
        }

        public void setScaled_tower_damage(int scaled_tower_damage) {
            this.scaled_tower_damage = scaled_tower_damage;
        }

        public int getScaled_hero_healing() {
            return scaled_hero_healing;
        }

        public void setScaled_hero_healing(int scaled_hero_healing) {
            this.scaled_hero_healing = scaled_hero_healing;
        }

        public ArrayList<AbilityUpgrade> getAbility_upgrades() {
            return ability_upgrades;
        }

        public void setAbility_upgrades(ArrayList<AbilityUpgrade> ability_upgrades) {
            this.ability_upgrades = ability_upgrades;
        }
    }

    private class AbilityUpgrade {
        /*
            Example:
            ability: 5458,
            time: 244,
            level: 1
         */

        private int ability;
        private int time;
        private int level;

        public AbilityUpgrade(int ability, int time, int level) {
            this.ability = ability;
            this.time = time;
            this.level = level;
        }

        public int getAbility() {
            return ability;
        }

        public void setAbility(int ability) {
            this.ability = ability;
        }

        public int getTime() {
            return time;
        }

        public void setTime(int time) {
            this.time = time;
        }

        public int getLevel() {
            return level;
        }

        public void setLevel(int level) {
            this.level = level;
        }
    }



}
