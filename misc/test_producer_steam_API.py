"""
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
"""

import os
import time
import json
import requests
from time import localtime, strftime

#############
# CONSTANTS #
#############

TIME_TO_SLEEP = 0.05         # In seconds.

#D2APIKEYS = [
#        os.environ['D2_API_KEY_1'],
#        os.environ['D2_API_KEY_2'],
#        os.environ['D2_API_KEY_3'],
#        os.environ['D2_API_KEY_4']
#        ]

D2APIKEYS = [
        "A868F8A7BDA8E1E078F87E0C9166BDF2",
        "E5E354AFD52182764747E38EF65D43DB",
        "E376637472C986328832276D6D100EFC",
        "F1383F689EFF046CAF23218FFB61C71B"
        ]

STEAMAPI_GETMATCHHISTORY_URL = "https://api.steampowered.com/IDOTA2Match_570/GetMatchHistory/V001/"
STEAMAPI_GETMATCHDETAILS_URL = "https://api.steampowered.com/IDOTA2Match_570/GetMatchDetails/V001/"
STEAMAPI_GETMATCHHISTORYBYSEQUENCENUM = "https://api.steampowered.com/IDOTA2Match_570/GetMatchHistoryBySequenceNum/V001/"
OPENDOTA_GETMATCHDETAILS_URL = "https://api.opendota.com/api/matches/"
OPENDOTA_GETPLAYERINFO_URL = "https://api.opendota.com/api/players/"

STARTING_SEQ_NUM = "3000000156"

MIN_DURATION = 600                      # in seconds
CYCLES = 200000
COUNT = 1

CUR_KEY = 0

CSV_COL_NAMES = [
            'match_id',
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
            'engine'
            ]

#############################
# Call API. Collect matches #
#############################

def APICall(STARTING_SEQ_NUM=STARTING_SEQ_NUM, CUR_KEY=CUR_KEY):
    
    start_time = strftime("%Y-%m-%d %H:%M:%S", localtime())
    start_seq = int(STARTING_SEQ_NUM)
    t = '\n\n'+'Start: '+start_time+'\t'+str(start_seq)+'\n'
    with open('dotaoracle_log.txt','a') as f:
        f.write(t)

        # Call Steam API GetMatchHistoryBySequenceNum
        cycle = 0
        while cycle < CYCLES:
            
            for _ in range(10):
                # Write to file every for every 1000 matches.
                with open('steamAPIJSON.csv', 'ab') as csv_file:
                    try:
                        r = requests.get(STEAMAPI_GETMATCHHISTORYBYSEQUENCENUM, \
                            {"key":D2APIKEYS[CUR_KEY], "matches_requested":100, "start_at_match_seq_num":start_seq})
                        
                        raw_data = r.json()
                        
                        if 'result' not in raw_data:
                            
                            continue
                            
                        if raw_data['result']['status'] == 429:
                            # Too many requests, switch to next API_KEY
                            time.sleep(5)
                            CUR_KEY = (CUR_KEY+1)%len(D2APIKEYS)
            
                            continue
                        
                        data = raw_data['result']
                        matches = data['matches']
                        start_seq = data['matches'][-1]['match_seq_num'] + 1
                        
                        for match in matches:
                            try:
                                duration = match['duration']
                                if match['human_players']==10 and duration >= MIN_DURATION and match['game_mode'] in (1,2,3,5,16,22):
                                    
                                    # Format the match and return a comma separated string
                                    res = get_match_details(match).encode()
                                    
                                    csv_file.write(res)
                                    
                            except Exception as e:
                                print(e)
                        time.sleep(TIME_TO_SLEEP)
                        print(start_seq)
                        
                    except Exception as e:
                        time.sleep(20)
                        # Sometimes the error occurs because the Steam API is down. In that case,
                        # we may want to keep trying until it recovers.
                        cycle -= 1
            #            start_seq += 1
                    STARTING_SEQ_NUM = str(start_seq+1)
                    
                    t = "\tCycle: {}. Last_seq_num: {}.".format(cycle, start_seq)
                    print(t)
                
                    with open('dotaoracle_log.txt','a') as f:
                        f.write("\n"+t)
                    cycle += 1
            
        end_time = strftime("%Y-%m-%d %H:%M:%S", localtime())
        end_seq = STARTING_SEQ_NUM
    
        t = '\nEnd: '+end_time+'\t'+end_seq
        print("DONE\n"+t)
        with open('steamAPI_call_log.txt','a') as f:
            f.write(t)

def get_match_details(match):
    
    # From a "match" dictionary, returns a comma separated string
    # in the format of the OpenDOTA data dump
    
    res = []
    
    for col in CSV_COL_NAMES:
        res.append(str(match.get(col)))
    
    for _ in range(8):
        res.append("")
    
    # Players
    player_str = "{"
    pattern =   '""{}"":left_par""account_id"":{},""hero_id"":{},""player_slot"":{}right_par,'
    
    for player in match['players']:
        player_str += pattern.format(
                        player["player_slot"],
                        player["account_id"],
                        player["hero_id"],
                        player["player_slot"]
                        )
        
    player_str += "}"
    
    player_str = player_str.replace("left_par","{").replace("right_par","}")
    
    res.append(player_str)
    
    return ",".join(res) + "\n\n"
    
    

APICall()