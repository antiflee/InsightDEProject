import os
import time

os.chdir("/Volumes/YL/OpenDOTADataDump")


lines_per_file = 2*10**6
lines_per_file = 10000


last_position = 0
cur_file_num = 1

log_file_name = "split_log.txt"

with open("./matches.csv","rb") as f:
    
    f.seek(last_position+1)
    
    while True:
        
        start_time = time.time()
        
        file_name = "matches_split_" + str(cur_file_num)
        
        t1 = "{}: starting from position {}, ending at ".format(file_name, last_position+1)
        
        t = b""
        
        for _ in range(lines_per_file):
            t += f.readline()
        
        # Save the current position
        last_position = f.tell()
        
        end_time = time.time()
        
        t1 += str(last_position) + "\n"
        # Write to the log
        with open(log_file_name, "a") as f1:
            f1.write(t1)
            
        # Overwrite last_position.txt
        with open("last_position.txt", "w") as f2:
            f2.write(str(last_position))
        
        print(t1 + "\n\tSpent {0:.2f} seconds.".format(end_time - start_time))
        
        # Write data to a new file
        with open(file_name, "wb") as f3:
            f3.write(t)
        
        cur_file_num += 1

# Test
#with open("./matches_split_5", "rb") as f:
#    head = [next(f) for _ in range(1)]
#print(head)