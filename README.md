# InsightDEProject

Insight DE 18A

# Table of Contents

1. [About](README.md#about)
2. [Use Cases](README.md#use-cases)
3. [Pipeline](README.md#pipeline)

# About

This project is about building a pipeline for the analysis on a popular online video game <a href="https://www.dota2.com">DOTA2</a>.

*About DOTA2 (From <a href="https://en.wikipedia.org/wiki/Dota_2">Wikipedia</a>)*:

Dota 2 is played in matches between two teams of five players, with each team occupying and defending their own separate base on the map. Each of the ten players independently controls a powerful character, known as a "hero", who all have unique abilities and differing styles of play.

*About the data*:

<a href="https://www.opendota.com/">OpenDOTA</a> provides a data dump of over 1 billion matches (see <a href="https://blog.opendota.com/2017/03/24/datadump2/">blog</a>). For each match it contains the information of start time, cluster, and the data for each player in a match, such as kills, deaths, and gold per minute.

The official <a href="https://wiki.teamfortress.com/wiki/WebAPI#Dota_2">Steam API</a> can also be used for streaming. But due to the query limits (~100,000 calls per day), the data dump by OpenDOTA is used in this project.

<!---
<img src="https://s3-us-west-2.amazonaws.com/yfsmiscfilesbucket/Screen+Shot+2018-01-11+at+9.08.24+PM.png" alt="hero-avatars" style="width:50%">
-->

# Use Cases

The app built in this project can be helpful for both players and the game company.

### For players:

This project can help players to have a better understanding of the game "meta", e.g., what heroes combinations are the most powerful, how to *counter pick* heroes.

(1) The app provides a dashboard that shows the statistics of all the 115 heroes over time, including hero popularity, win rate, best teammates and opponents, what in-game items to buy ("hero builds") for that hero.

(2) *Match of the hour*: the best match that was played in the last hour, based on the total number of hero kills, and how close two teams are.

(3) A predictive model that predicts which team will win based on the 10 heroes picked. This model is based on all game matches played in the past week, and updated everyday.

### For the game company:

The project can also help the game company (<a href="http://www.valvesoftware.com/">Valve Corporation</a>) to *monitor* users' activity/behavior, and optimize the in-game <a href="https://dota2.gamepedia.com/Matchmaking">matchmaking system</a>.

(1) Number of currently in-game players in different countries (on different servers).

(2) Player churn report: a list of players that stopped playing since one month ago. Saves the play_id and the match_id of the last 10 game matches the played played. This report could be later used for churn prediction.

(3) Matchmaking system optimization... (group players into opposing teams based on the last 5 matches they played?)

(4) "Toxic" players detection: toxic players are players that ruin others' game experience (for example, abandon the game, or intentionally "feed" the opponents). The algorithm to define the toxicity is not easy. Here we simply provide a list of players that are potentially toxic based on the last few matches they played.

# Pipeline

<img src="https://s3-us-west-2.amazonaws.com/yfsmiscfilesbucket/pipeline.png">
