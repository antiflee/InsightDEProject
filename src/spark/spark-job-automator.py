# Detect the new date that Flink processed, if the date is new, 
# run Spark on it.
import os
import time
import shlex

from pathlib import Path
from subprocess import call
from cassandra.cluster import Cluster

SPARK_HOME = os.getenv("SPARK_HOME")

DEFAULT_DATE = "2012-05-29"

SPARK_MASTER_URL = "spark://ip-10-0-0-5.us-west-2.compute.internal:7077"
#CASSANDRA_URL = os.getenv("CASSANDRA_URL")
CASSANDRA_URL = "ec2-34-213-32-67.us-west-2.compute.amazonaws.com"
cluster = Cluster([CASSANDRA_URL])
cassSession = cluster.connect("ks")

jar_name = "~/test2_2.11-0.1.jar "

def set_default_date(date):
    global DEFAULT_DATE
    DEFAULT_DATE = date
    
def set_jar_name(new_name):
    global jar_name
    jar_name = new_name

def spark_player_daily():
    
    # Update ks.player_daily table in Cassandra when there is a 
    # new date coming in.
    
    # Default value.
    start_date = DEFAULT_DATE
    
    # Get the start date from the log file.
    last_date_file = "./player_daily_date_log.txt"
    
    # Log to write to
    spark_player_daily_log_file = "./spark_player_daily_log.txt"
    
    try:
        with open(last_date_file, "r") as f:
            start_date = f.readline()
    except Exception as e:
        print(e)
        
    # Since the hero_win_rate data table is not ordered by time DESC, we
    # just iterate the whole table. This part could be optimized by tweaking
    # the schema of the data table.
    query = "SELECT year,month,day FROM ks.hero_win_rate where hero_id='1'"
    
    while True:
        
        # If last_date_file is not empty, read the date from it.
        # This is the date that Spark executes on last time.
        # We'll start searching from that date.
#        if last_date_file.is_file():
#            with open(last_date_file, "r") as f:
#                start_date = f.readline()
        
        # Check date by looking at the hero_win_rate data table in Cassandra
        rows = cassSession.execute(query)
        
        new_date = start_date
        for r in rows:
            tmp = '-'.join([format(r.year,'04'),format(r.month,'02'),format(r.day,'02')])
            
            if new_date < tmp:
                new_date = tmp
                break
        
        if new_date != start_date:
            # Got new date, run Spark jobs.        
            print("Found new date " + new_date + ", Starting Spark...\n")
            
            call(shlex.split(SPARK_HOME+"/bin/spark-submit " +
                  "--packages datastax:spark-cassandra-connector:2.0.1-s_2.11 " +
                  "--master " + SPARK_MASTER_URL + " " +
                  "--class com.yufeil.dotainsight.spark.PlayerDailyProcessor " +
                  jar_name + " " + new_date))
        
            # Update the date
            
            with open(spark_player_daily_log_file, "a") as f:
                f.write("\nUpdated ks.player_daily table for date |{}|. Finished at {}.\n"
                        .format(new_date, time.time()))

            with open(last_date_file, "w") as f:
                f.write(new_date)
            
            start_date = new_date
            
        # Check updates every 2 min.
        time.sleep(120)

def spark_DAU():
    # Update ks.daily_active_users table in Cassandra when there is a 
    # new date written into "spark_player_daily_log_file" by the other
    # Spark job.
    
    # Default value.
    start_date = DEFAULT_DATE
    
    # Get the start date from the log file.
    last_date_file = "./DAU_date_log.txt"
    
    spark_DAU_log_file = "./spark_DAU_log.txt"
    
    try:
        with open(last_date_file, "r") as f:
            start_date = f.readline()
    except Exception as e:
        print(e)
    
    # Since the daily_player data table is not ordered by time DESC, we
    # just iterate the whole table. This part could be optimized.
    query = "SELECT year,month,day FROM ks.hero_win_rate where hero_id='1'"
    
    while True:
        
        
        print("Checking date " + start_date + "...\n")
        
        
        rows = cassSession.execute(query)
        
        new_date = start_date
        for r in rows:
            tmp = '-'.join([format(r.year,'04'),format(r.month,'02'),format(r.day,'02')])
            
            if new_date < tmp:
                new_date = tmp
                break
        
        if new_date != start_date:
            
            print("Found new date " + new_date + ", Starting Spark...\n")
            
            # Got new date, run Spark jobs.        
            call(shlex.split(SPARK_HOME+"/bin/spark-submit " +
                  "--packages datastax:spark-cassandra-connector:2.0.1-s_2.11 " +
                  "--master " + SPARK_MASTER_URL + " "
                  "--class com.yufeil.dotainsight.spark.DAU " +
                  jar_name + " " + new_date))
        
            # Update the date
            with open(last_date_file, "w") as f:
                f.write(new_date)
            
            start_date = new_date
    
            with open(spark_DAU_log_file, "a") as f:
                f.write("\nUpdated ks.daily_active_user table for date |{}|. Finished at {}.\n"
                        .format(new_date, time.time()))
        
        # Check updates every 2 min.
        time.sleep(120)


if __name__ == "__main__":
    import sys
    if len(sys.argv) < 3:
        print("Usage: python <python_file_name> <jar_name> <player_daily or DAU> <default_start_date>")
        
    else:
        set_jar_name(sys.argv[1])
        set_default_date(sys.argv[3])
        
        if sys.argv[2] == "player_daily":
            spark_player_daily()
        elif sys.argv[2] == "DAU":
            spark_DAU()
        else:
            print("Please indicate the Spark job to run: player_daily or DAU")
            print("Usage: python <python_file_name> <jar_name> <player_daily or DAU> <default_start_date>")
