/*
    Create the following tables in the Cassandra cluster:

        Table_name              Schema
    (1) hero_win_rate           (hero_id, time_), win_rate
    (2) region_num_of_players   (cluster_id, time_), num_of_players
    (3) player_match            (account_id, start_time), duration, hero_id, won
    (4) player_daily            (account_id, date), matches_played, matches_won, time_played

 */

import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.Session;

public class CreateCassandraTable {

    private static final String CASSANDRA_URL = System.getenv("CASSANDRA_URL");
    private static final String KEYSPACE = "ks";

    public static void main(String args[]){

        //Query

        String create_hero_win_rate = "CREATE TABLE IF NOT EXISTS hero_win_rate("
                + "hero_id varchar, "
                + "time_ timestamp, "
                + "win_rate float, "
                + "PRIMARY KEY (hero_id, time_));";

        String create_region_num_of_players = "CREATE TABLE IF NOT EXISTS region_num_of_players("
                + "cluster_id int, "
                + "time_ timestamp, "
                + "num_of_players int, "
                + "PRIMARY KEY (cluster_id, time_));";

        String create_player_match= "CREATE TABLE IF NOT EXISTS player_match("
                + "account_id varchar, "
                + "start_time timestamp, "
                + "duration int, "
                + "hero_id varchar, "
                + "win boolean, "
                + "PRIMARY KEY (account_id, start_time));";

        String create_player_daily = "CREATE TABLE IF NOT EXISTS player_daily("
                + "account_id varchar, "
                + "date timestamp, "
                + "matches_played int, "
                + "matches_won int, "
                + "time_played_in_min int, "
                + "PRIMARY KEY (account_id, date));";

        //Creating Cluster object
        Cluster cluster = Cluster.builder().addContactPoint("ec2-34-213-32-67.us-west-2.compute.amazonaws.com").build();

        //Creating Session object
        Session session = cluster.connect("ks");

        //Executing the query
        session.execute(create_hero_win_rate);
        session.execute(create_region_num_of_players);
        session.execute(create_player_match);
        session.execute(create_player_daily);

        System.out.println("Table created");

        cluster.close();
    }
}
