import sys
import os
import time
import mmap
import statistics
import pydig
import re

def generateDicts(log_fh):
    currentDict = {}
    for line in log_fh:
        if line.startswith(matchDate(line)):
            if currentDict:
                yield currentDict
            if "Read" in line.split("  ",5)[-1]:
            	#print('Bytes')
            	#print(line.split("  ",5)[-1].split(' ')[2])
            	currentDict = {"date":line.split("INFO")[0][:19],"type":"INFO","text":line.split("  ",5)[-1],"Bytes_read":line.split("  ",5)[-1].split(' ')[2],"source_IP":'n/a',"source_port":'n/a'}
            elif "File was" in line.split("  ",5)[-1]:
            	print(line.split("  ",5)[-1].split(' ')[-1].strip('\n'))
            	x = pydig.query(line.split("  ",5)[-1].split(' ')[-1].strip('\n').split(':')[0],'A')
            	currentDict = {"date":line.split("INFO")[0][:19],"type":"INFO","text":line.split("  ",5)[-1],"Bytes_read":'n/a',"source_IP":x,"source_port":line.split("  ",5)[-1].split(' ')[-1].strip('\n').split(':')[-1]}
            	#print(currentDict)
            	#currentDict{"date":line.split("INFO")[0][:19],"type":"INFO","text":line.split("  ",5)[-1],}
            else:
            	currentDict = {"date":line.split("INFO")[0][:19],"type":"INFO","text":line.split("  ",5)[-1],"Bytes_read":'n/a', "source_IP":'n/a', "source_port":'n/a'}
            print(currentDict)
            time.sleep(1)
            

        else:
            currentDict["text"] += line
    yield currentDict




def matchDate(line):
        matchThis = ""
        matched = re.match(r'\d\d\d\d-\d\d-\d\d\ \d\d:\d\d:\d\d',line)
        if matched:
            #matches a date and adds it to matchThis            
            matchThis = matched.group()
            #print(matchThis) 
        else:
            matchThis = "NONE"
        return matchThis

with open("/Users/agiannakou/Downloads/logname") as f:
    listNew= list(generateDicts(f))
    print(listNew)
    exit(0)










with open(r'/Users/agiannakou/Downloads/logname', 'r') as file:
	hostnames = set()
	sizes = []
	i = 0
	for line in file.readlines():
		if 'File was served from' in line:
			print(line.split(' ')[-1])
			i+=1
			hostnames.add(line.split(' ')[-1].strip('\n'))
		if 'Read' in line:
			print(line.split(' ')[1])
			sizes.append(float(line.split(' ')[1]))

	print('How many streams did you find?')
	print(i)
	print("Distinct source hosts")
	#print(len(hostnames))
	#print(hostnames)
	print(set(list(map(lambda x: x.split(':')[0],hostnames))))
	hosts = list(map(lambda x: x.split(':')[0],hostnames))
	print(len(set(list(map(lambda x: x.split(':')[0],hostnames)))))
	print('Avg stream size in bytes')
	print(statistics.mean(sizes))
	ips = list(map(lambda x: pydig.query(x,'A'),hosts))
	print(*ips,sep = ', ')