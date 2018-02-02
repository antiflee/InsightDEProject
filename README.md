# Dota Insight

Insight DE 18A

# Table of Contents

1. [About](README.md#about)
2. [Use Cases](README.md#use-cases)
3. [Pipeline](README.md#pipeline)

# About

* <a href="https://docs.google.com/presentation/d/1YrbU6vBK0_jchzj-x7RPRLyQJft8U83qO6RziF-5Ijw/edit#slide=id.g3285dc8d63_0_0">
  Slides </a>


This project is to build a pipeline for the analysis on a popular online video game <a href="https://www.dota2.com">DOTA2</a>.

*About DOTA2 (From <a href="https://en.wikipedia.org/wiki/Dota_2">Wikipedia</a>)*:

Dota 2 is played in matches between two teams of five players, with each team occupying and defending their own separate base on the map. Each of the ten players independently controls a powerful character, known as a "hero", who all have unique abilities and differing styles of play.

*About the data*:

<a href="https://www.opendota.com/">OpenDOTA</a> provides a data dump of over 1 billion matches (see <a href="https://blog.opendota.com/2017/03/24/datadump2/">blog</a>). For each match it contains the information of start time, cluster, and the data for each player in a match, such as kills, deaths, and gold per minute.

The official <a href="https://wiki.teamfortress.com/wiki/WebAPI#Dota_2">Steam API</a> can also be used for streaming. But due to the query limits (~100,000 calls per day), the data dump by OpenDOTA is used in this project. To simulate real-time data feeding, the raw data of the data dump, which is in csv format, is converted to JSON format before sent to Kafka for ingestion (See <a href="https://github.com/antiflee/InsightDEProject/blob/master/src/kafka_producer/JSONGeneratorFromCSV_matches.py">producer_script</a> for more details).

**One thing to note** is that, the matches in the raw data is out of order (e.g., a match in 2012 comes after a match in 2016, then followed by a match in 2014). For simplicity, while feeding the data to Kafka, the *start_time* of each match is simulated in this way: it is approximately in ascending order, but with some extent of *out of order* is included. The default *disorderness* is set to be one hour, i.e., a match can be sent to Kafka at any time in a range of +/- 1 hr, based on its actual start time. See <a href="https://github.com/antiflee/InsightDEProject/blob/master/src/kafka_producer/JSONGeneratorFromCSV_matches.py">producer_script</a> for more details. A more realistic way to simulate would be a latency of 10 - 90 mins after the start time (which is the event time).

<!---
<img src="https://s3-us-west-2.amazonaws.com/yfsmiscfilesbucket/Screen+Shot+2018-01-11+at+9.08.24+PM.png" alt="hero-avatars" style="width:50%">
-->

# Use Cases

The app built in this project can be beneficial for both players and the game company. It has two main tabs, heroes and players, which show both the real-time and historical data in a dashboard.

### Heroes Tab:

This project can help players to have a better understanding of the game "meta", e.g., what heroes combinations are the most powerful, how to *counter pick* heroes.

(1) The dashboard displays the real-time win rate for each hero, based on the last 100 matches played.

(2) For each hero, the web app gives you the win rate and popularity (how many times this hero is picked every day) over time. Also, it shows the best teammates and best opponents of this hero.

### Players Tab:

The project can also help the game company (<a href="http://www.valvesoftware.com/">Valve Corporation</a>) to *monitor* users' activity/behavior, and optimize the in-game <a href="https://dota2.gamepedia.com/Matchmaking">matchmaking system</a>.

Basically, users of this app can query with a specific date, or with an account id.

(1) *query with a date*: this will give you the daily active users around that day. Also, it displays the distribution of players in different regions/countries over the world.

(2) *query with an account id*: this will show the activity of the player with that account id. For each day in the past, it gives: number of matches played and won, and number of minutes played on that day.

# Pipeline

<img src="https://s3-us-west-2.amazonaws.com/yfsmiscfilesbucket/pipeline.png">
<br>
*Click any component below to see the source code!*



&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;<a href="https://github.com/antiflee/InsightDEProject/blob/master/src/python-kafka-to-redis/kafka_win_rate_consumers.py">[Consumer]</a>-----[Redis]-----------
&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;|&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;|
[S3]-----<a href="https://github.com/antiflee/InsightDEProject/blob/master/src/kafka_producer/JSONGeneratorFromCSV_matches.py">[Producer]</a>-----[Kafka]-----<a href="https://github.com/antiflee/InsightDEProject/blob/master/src/flink/src/main/java/com/yufeil/dotainsight/flink_streaming/JSONParser.java">[Flink]</a>&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;<a href="https://github.com/antiflee/dota-insight-django">[Django]</a>
&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;|&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;|
&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;<a href="https://github.com/antiflee/InsightDEProject/tree/master/src/spark">[Spark]</a>-----<a href="https://github.com/antiflee/InsightDEProject/blob/master/misc/CassandraTableSchema.cql">[Cassandra]</a>----------
