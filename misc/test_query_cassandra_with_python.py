# https://github.com/datastax/python-driver
# http://datastax.github.io/python-driver/getting_started.html

import os

def getHeroWinRateHistory(heroId):

    from cassandra.cluster import Cluster

    CASSANDRA_URL = "ec2-34-213-32-67.us-west-2.compute.amazonaws.com"

    cluster = Cluster([CASSANDRA_URL])

    session = cluster.connect("ks")

    rows = session.execute('SELECT * FROM hero_win_rate WHERE hero_id=%s', (str(heroId),))

    for row in rows:
        print(row.month)

import json
from cassandra.cluster import Cluster

CASSANDRA_URL = "ec2-34-213-32-67.us-west-2.compute.amazonaws.com"
cluster = Cluster([CASSANDRA_URL])
cassSession = cluster.connect("ks")

# Get num of players in each region from Redis
def realTimeRegion(date):

    year, month, day = int(date[:4]), int(date[4:6]), int(date[6:])

    rows = cassSession.execute('SELECT * FROM region_num_of_players ' +
                'WHERE year=%s AND month=%s AND day=%s',
                (year, month, day))

    for row in rows:
        cluster_id, num = row.cluster_id, row.num_of_players
        print(row)


def getDAU(date):
    
    year, month, day = int(date[:4]), int(date[4:6]), int(date[6:])

    rows = cassSession.execute('SELECT * FROM daily_active_ ' +
                'WHERE year=%s AND month=%s AND day=%s',
                (year, month, day))

    for row in rows:
        cluster_id, num = row.cluster_id, row.num_of_players
        print(row)
        

import time

start_time = time.time()
year, month, day = 2012, 8, 3

rows = cassSession.execute("SELECT year,month,day,account_id FROM ks.daily_player " +
                        'WHERE year=%s AND month=%s AND day=%s',
                        (year, month, day))

new_date = "2012-08-03"
prev_acc_id, count = -1, 0


for r in rows:
    acc_id = r.account_id
    if acc_id != prev_acc_id:
        count += 1
        prev_acc_id = acc_id

print(count)

print(time.time() - start_time)
    

















    