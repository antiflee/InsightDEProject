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
    
    Here we use multithreading to create multiple producers, where each producer
    will read different lines from the AWS CSV file.

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
            engine: 1

        PlayerInMatch:
            account_id: 201080081,
            player_slot: 0,
            hero_id: 87
"""

import os
import smart_open
import re
import time
import datetime
import json
import threading

from kafka import KafkaProducer
from random import randint, uniform

#############
# Constants #
#############

# The raw data to read from
#S3_FILE_URL = "s3://yf-insight-de-18a-data/sample-data/matches_small.csv"
S3_FILE_URL = "s3://yf-insight-de-18a-data/matches_full.csv"
#S3_FILE_URL = "s3://yfsmiscfilesbucket/steamAPIJSON.csv"
# The Kafka cluster that the JSON file would be sent to
#KAFKA_URL = os.getenv("KAFKA_URL")
KAFKA_URL = "ec2-52-41-196-130.us-west-2.compute.amazonaws.com"

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

TIME_TO_SLEEP = 0.0003

START_TIMESTAMP = [1378573135]    # Simulated start_time, in seconds

TIMESTAMP_LOG_FILENAME = "timestamp_log.txt"

# We simulate the timestamp of each match, this parameter gives 
# how long you want to simulate the matches for one day, assuming
# the through is 1000 per second.
TIME_PER_DAY = 300              # in seconds.

# The simulated average time elapsed between two matches can by calculated.
# So the value of the timestamp will have a 1/AVG_TIME_INTERVAL to increase
# by one second.
AVG_TIME_INTERVAL = 60*60*24/(1000*TIME_PER_DAY)

###########
# Methods #
###########

# To speed up reading the S3 file, we create multiple producers.
# For N producers, each producer will read one line then skip N-1 lines.
class ProducerFromS3 (threading.Thread):

    def __init__(self, threadID, seq_num, step=3, topic_name=TOPIC_NAME):
        threading.Thread.__init__(self)
        self.threadID = threadID
        self.seq_num = seq_num
        self.topic_name = topic_name
        self.step = step
  
    def run(self):
        ProducerFromS3.startProcessing(self.seq_num, self.step)
        
    @staticmethod
    def startProcessing(start_num=1,step=1):
        """
            Process raw matches data (in csv format) from AWS S3 line by line.
        """
    
        start_time_10000 = time.time()
    
        # Start the producer
        producer = KafkaProducer(bootstrap_servers=KAFKA_URL+":9092")
    
        print("---------------------------------------------------------------")
        print("\nStart processing thread #" + str(start_num))
        print("---------------------------------------------------------------")
    
        # Header of the log file
        with open(LOG_FILENAME, "w") as f:
            f.write("\nStart logging... Time: " + str(time.ctime(int(time.time()))) + "\n")
            f.write("------------------------------------------------------------------------------------\n")
            f.write("TimeStamp\t\t|\tMatch_Seq_Num\t|Time_consumed_for_10k_matches\t|Total_matches_elapsed\n")
            f.write("------------------------------------------------------------------------------------\n")
    
        n, effective_matches = 0, 0
        for line in smart_open.smart_open(S3_FILE_URL):
            
            time.sleep(TIME_TO_SLEEP)
            
            if n < start_num:
                # Skip to the target line
                # header is the first line
                n += 1
                continue
            
            if (n - start_num) % step != 0:
                # Only read every step-th line
                n += 1
                continue
    
            line = str(line,'utf-8')
    
            jsonFile, match_seq_num = ProducerFromS3.processLine(line)
    
            
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
    
            if effective_matches % 100000 == start_num:
                # Record current time for every 100k matches.
                t = "---------------------------------------------------------------\n" + \
                    "Total " + str(effective_matches) + " matches processed for this producer. " + \
                    "Current time is " + time.ctime(int(time.time())) + \
                    ". Timestamp is " + str(START_TIMESTAMP[0]) + \
                    "\n---------------------------------------------------------------\n"
                print(t)
                with open(LOG_FILENAME, "a") as f:
                    f.write(t)
                
                # Write the timestamp to a log file, so
                # we can set the start time accordingly next
                # time starting the program.
                with open(TIMESTAMP_LOG_FILENAME, "w") as f:
                    f.write(str(START_TIMESTAMP[0]))
    
    
    @staticmethod
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
        
        
        # Note that since the matches in the data dump is out of order,
        # (e.g, match in 2016-03-18 after the match in 2014-11-14), we
        # here simulate the start time of the match.
        # We add some randomness, +/-1200s
        simulated_time = START_TIMESTAMP[0] + randint(-1200,1201)
        
        # We will have 1/AVG_TIME_INTERVAL chance to increase the 
        # START_TIMESTAMP by one second. We use random.uniform(0,1)
        START_TIMESTAMP[0] += 1 if uniform(0,1) < 1/AVG_TIME_INTERVAL else 0
        
        # Set the start_time of the match to be the simulated value.
        result['start_time'] = simulated_time
        
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
    
        match_seq_num = result.get("match_seq_num", "0")
    
        # Convert to json string, and remove quotes and back slash
        result = json.dumps(result)
        result = re.sub(r"[\\\"]", "", result)
    
        return result, match_seq_num



if __name__ == "__main__":
    # Set the start time first
    import sys
    
    step = 3
    
    if len(sys.argv) < 2:
        print("Usage: python3 <.py_filename> <num_of_producers>\n" + 
              "Using 3 producers by default...\n")
    else:
        step = int(sys.argv[1])
        
    try:
        with open(TIMESTAMP_LOG_FILENAME, "r") as f:
            START_TIMESTAMP[0] = int(f.readline())
            if START_TIMESTAMP[0] > 0:
                print("Starting from timestamp " + str(START_TIMESTAMP[0]) + "\n")
        
    except Exception as e:
        print("Error: ", e)
    
    # Create multiple producers
    producers = []
    for i in range(1,step+1):
        producers.append(ProducerFromS3(i,i,step=step))
    
    for producer in producers:
        producer.start()
