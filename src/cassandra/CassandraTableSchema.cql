DROP KEYSPACE IF EXISTS ks;

CREATE KEYSPACE ks
  WITH REPLICATION = { 'class' : 'SimpleStrategy', 'replication_factor' : 2 };


CREATE TABLE IF NOT EXISTS ks.hero_win_rate(
          hero_id         text,
          year            int,
          month           int,
          day             int,
          win_rate        float,
          count           bigint,
          PRIMARY KEY ((hero_id), year, month, day)
          );

CREATE TABLE IF NOT EXISTS ks.region_num_of_players(
          cluster_id      int,
          year            int,
          month           int,
          day             int,
          num_of_players  bigint,
          PRIMARY KEY     ((year, month, day), cluster_id)
          );

CREATE TABLE IF NOT EXISTS ks.player_match(
          account_id      bigint,
          year            int,
          month           int,
          day             int,
          start_time      bigint,
          duration        int,
          hero_id         text,
          win             boolean,
          PRIMARY KEY     ((account_id), year, month, day, start_time)
          );

CREATE TABLE IF NOT EXISTS ks.player_daily(
          account_id      bigint,
          year            int,
          month           int,
          day             int,
          matches_played  int,
          matches_won     int,
          time_played_in_sec int,
          PRIMARY KEY     ((account_id), year, month, day)
          );

CREATE TABLE IF NOT EXISTS ks.daily_player(
          year            int,
          month           int,
          day             int,
          account_id      bigint,
          time_           bigint,
          hero_id         text,
          duration        int,
          PRIMARY KEY     ((year, month), day, account_id, time_)
);


CREATE TABLE IF NOT EXISTS ks.daily_active_users(
          year            int,
          month           int,
          day             int,
          num             bigint,
          PRIMARY KEY     ((year, month), day)
);

CREATE TABLE IF NOT EXISTS ks.monthly_active_users(
          year            int,
          month           int,
          num             bigint,
          PRIMARY KEY     (year, month)
);
