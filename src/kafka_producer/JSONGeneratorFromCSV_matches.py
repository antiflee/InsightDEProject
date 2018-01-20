"""
    The data dump provided by OpenDOTA is in csv format,
    while the official web API is in JSON.
    This python script converts the historical file from csv to JSON,
    and send it to Kafka for ingestion.
    
    Problem description:
        
      A key difference between the data dump and the web API is that the raw 
    data file processed here doesn't contain the detailed info of players,
    such as kills, deaths, gold earned, etc. This part of information was
    saved in another file of the data dump.
    Also, we'll omit some fields from the raw csv file, as it is not included
    in the JSON returned by the web API.
    
    Input: csv file from AWS S3 (fields with star symbol will be omitted):
        Match:
            match_id,
            match_seq_num,
            radiant_win,
            start_time,
            duration,
            tower_status_radiant,
            tower_status_dire,
            barracks_status_radiant,
            barracks_status_dire,
            cluster,
            first_blood_time,
            lobby_type,
            human_players,
            leagueid,
            positive_votes,
            negative_votes,
            game_mode,
            engine,
            *picks_bans,
            *parse_status,
            *chat,
            *objectives,
            *radiant_gold_adv,
            *radiant_xp_adv,
            *teamfights,
            *version,
            pgroup (name will be changed to players)
        Player:
            {""0"":{""account_id"":4294967295,""hero_id"":93,""player_slot"":0},
            ""1"":{""account_id"":4294967295,""hero_id"":75,""player_slot"":1},
            ""2"":{""account_id"":4294967295,""hero_id"":19,""player_slot"":2},
            ""3"":{""account_id"":4294967295,""hero_id"":44,""player_slot"":3},
            ""4"":{""account_id"":4294967295,""hero_id"":7,""player_slot"":4},
            ""128"":{""account_id"":4294967295,""hero_id"":46,""player_slot"":128},
            ""129"":{""account_id"":45475622,""hero_id"":38,""player_slot"":129},
            ""130"":{""account_id"":4294967295,""hero_id"":52,""player_slot"":130},
            ""131"":{""account_id"":4294967295,""hero_id"":43,""player_slot"":131},
            ""132"":{""account_id"":4294967295,""hero_id"":60,""player_slot"":132}}
        
    Output: the format of JSON:
        Match:
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
        
        PlayerInMatch:
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
        
        AbilityUpgrade:
            ability: 5458,
            time: 244,
            level: 1
"""

import os
import smart_open
import re
import time
import json

from kafka import KafkaProducer

#############
# Constants #
#############

# The raw data to read from
S3_FILE_URL = "s3://yf-insight-de-18a-data/sample-data/matches_small.csv"

# The Kafka cluster that the JSON file would be sent to
KAFKA_URL = os.getenv("KAFKA_URL")

# The name of the topic it will be sent to
TOPIC_NAME = "match-raw-json"

# The path and filename of the log
LOG_FILENAME = "log_csv_to_json_producer.txt"

# Columns/Fields in the raw data file (csv formatted)
COLUMNS =   ['match_id',
             'match_seq_num',
             'radiant_win',
             'start_time',
             'duration',
             'tower_status_radiant',
             'tower_status_dire',
             'barracks_status_radiant',
             'barracks_status_dire',
             'cluster',
             'first_blood_time',
             'lobby_type',
             'human_players',
             'leagueid',
             'positive_votes',
             'negative_votes',
             'game_mode',
             'engine']

NUM_OF_OMITTED_FIELDS = 8       # Some fields don't need to be converted.
                                # See the stared columns in the comments
                                # at the top of this script.

REGEX_PATTERN = "{[\",:\w]+\}"  # For extracting players info from the data
                                # See function processLine() below

###########
# Methods #
###########

def startProcessing():
    """
        Process raw matches data (in csv format) from AWS S3 line by line.
    """

    start_time_10000 = time.time()
    
    # Start the producer
    producer = KafkaProducer(bootstrap_servers=KAFKA_URL+":9092")
    
    print("---------------------------------------------------------------")
    print("Start processing... ")
    print("---------------------------------------------------------------")
    
    # Header of the log file
    with open(LOG_FILENAME, "a") as f:
        f.write("\nStart logging... Time: " + time.ctime(int(time.time())) + "\n")
        f.write("------------------------------------------------------------------------------------\n")
        f.write("TimeStamp\t\t|\tMatch_Seq_Num\t|Time_consumed_for_10k_matches\t|Total_matches_elapsed\n")
        f.write("------------------------------------------------------------------------------------\n")
    
    n, effective_matches = 0, 0
    for line in smart_open.smart_open(S3_FILE_URL):
        if n == 0:
            # Skipe the header
            n = 1
            continue
        
        line = str(line,'utf-8')
        
        jsonFile, match_seq_num = processLine(line)
        
        # Convert from string to bytes, as Kafka accepts bytes format.
        jsonFile = jsonFile.encode()
        
        # Send it to Kafka
        producer.send(TOPIC_NAME, jsonFile)
        
        n += 1
        
        if jsonFile:
            # If the match is valid (10 human players), increase the count
            effective_matches += 1
        
        if effective_matches % 10000 == 0:
            # Record the time consumed for every 10k matchces processed
            # Log it and the last match_seq_num
            end_time = time.time()
            
            t = "{}\t|\t{}\t|\t\t{:.2f}\t\t|\t{}\n".format(
                                        end_time, match_seq_num, 
                                        end_time - start_time_10000,
                                        n)
            
            with open(LOG_FILENAME, "a") as f:
                f.write(t)
            
            print("{0:.2f} seconds for processing 10000 valid matches.".format(end_time - start_time_10000,) + 
                  " Total matches processed: " + str(n) + ".")
            
            start_time_10000 = end_time
        
        if effective_matches % 100000 == 0:
            # Record current time for every 100k matches.
            t = "---------------------------------------------------------------\n" + \
                "Total " + str(effective_matches) + " matches processed. " + \
                "Current time is " + time.ctime(int(time.time())) + \
                "\n---------------------------------------------------------------\n"
            print(t)
            with open(LOG_FILENAME, "a") as f:
                f.write(t)
            
            
def processLine(line):
    """
        Input:  a line of raw data in csv format
        Output: returns the data in JSON format (as in the official API)
                and the match_seq_num of the match.
    """
    
    data = line.split(',', len(COLUMNS) + NUM_OF_OMITTED_FIELDS)  
                            # Note here the last item contains the information of 
                            # all players, which is a long string, and will be
                            # handled later in this function.
    
    # Validate data:
    if len(data) != len(COLUMNS) + NUM_OF_OMITTED_FIELDS + 1:
        # the "+1" above is the info of players
        return "", "0"
    
    # Note there are two fields: pre_game_duration, and flags, that are
    # not included in the raw csv file, but in the JSON file. They are 
    # trivial parameters so we simply set them to default.
    result = {'pre_game_duration':90,'flags':1}
    
    # Pass the values of each column from csv to the dictionary "result",
    # except for information of all players
    for i, col in enumerate(COLUMNS):
        result[col] = data[i]
    
    
    # Now deal with the players. We will not parse it, but just "massage" it
    # in the format of the official API JSON file, and treat it as a string.
    players_info = data[-1].lstrip("\"{").rstrip("\n")
    
    players_list = re.findall(REGEX_PATTERN, players_info)
    
    # Validate if there are 10 players.
    if len(players_list) != 10:
        return "", "0"
    
    players_info = ",".join(players_list)
    players_info = "[" + players_info + "]"
    
    # Put it to "result"
    result["players"] = players_info
    
    return json.dumps(result), result.get("match_seq_num", "0")


if __name__ == "__main__":
#    import sys
#    if len(sys.argv) > 1:
    startProcessing()







    