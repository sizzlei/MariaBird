import birdModule as nest
import queue
import threading
import time
import os
import sys

# Config Reading
confWorker = nest.configure('bird.cnf')
dbConf = confWorker.getConfig('DATABASE')
condition = confWorker.getConfig('CONDITION')
repl = confWorker.getConfig('REPL')
birdConf = confWorker.getConfig('BIRD')
pidFile = "/tmp/birdcdc.pid"
birdEndFile = "birdEndPos.info"

# Worker Delcare
sourceWorker = nest.getDBdata(ip=dbConf['source_ip'],port=dbConf['source_port'],user=dbConf['dbuser'],passwd=dbConf['dbpass'])
LogWorker = nest.binlogParser(condition['database'],condition['tables'])
dumpWorker = nest.logDumper(birdConf['mysql_client_path'],birdConf['binarylog_path'],birdConf['dumpfile_path'])
exeWorker = nest.logRunner(birdConf['mysql_client_path'],ip=dbConf['target_ip'],port=dbConf['target_port'],user=dbConf['dbuser'],passwd=dbConf['dbpass'],dumppath=birdConf['dumpfile_path'])
# Global Transaction Queue
logQueue = queue.Queue()

# Load Data
def EventDataLoader(binlogFile, binlogPos):
    pntFile = binlogFile
    pntPos = binlogPos

    while True:
        LogData = sourceWorker.getEventLoader(pntFile,pntPos)       
        if len(LogData) > 1:
            parsingData = LogWorker.makeTRX(LogData)
            if parsingData:
                logQueue.put(parsingData)

            if LogData[-1]['Event_type'] == 'Rotate':
                nextLog = LogData[-1]['Info'].split(';')
                nextPos = nextLog[1].split('=')
                pntFile = nextLog[0]
                pntPos = int(nextPos[1])
            else:
                pntFile = LogData[-1]['Log_name']
                pntPos = LogData[-1]['End_log_pos']  
                time.sleep(2) 
        else:
            pntFile = LogData[0]['Log_name']
            pntPos = LogData[0]['End_log_pos']     
            time.sleep(3) 
        

def EventDataDumper():
    while True:
        dataSet = logQueue.get()        
        if dataSet:
            for dataEvent in dataSet:
                dumpWorker.dumpMaker(
                    dataEvent['Logname'],
                    dataEvent['startPos'],
                    dataEvent['endPos'],
                    condition['database'],
                    repl['todb'])
        else:
            time.sleep(1)

def EventRunner(): 
    while True:
        eventList = exeWorker.eventSorted()
        if eventList:                  
            exeWorker.eventRunner(eventList,birdConf['base_path'],birdEndFile)
        else:
            time.sleep(2)


def mainService():
    # Thread Configure
    dataLoader = threading.Thread(target=EventDataLoader,args=(repl['binary_file'],repl['binary_pos']))
    dataExporter = threading.Thread(target=EventDataDumper)
    dataRunner = threading.Thread(target=EventRunner)

    # Thread Start
    dataLoader.start()
    dataExporter.start()
    dataRunner.start()


if __name__ == "__main__":
    if sys.argv[1] == "start":
        try:
            f = open(pidFile,'r')
            pid = f.read()
            f.close()

            print("Already Running BirdCDC PID : " + str(pid))
        except Exception as ss:
            print(ss)
            # Start Process
            print("Starting BirdCDC........")
            f = open(pidFile, 'w')
            pid = os.getpid()
            f.write(str(pid))
            f.close()

            mainService()

    elif sys.argv[1] == "stop":
        try:
            f = open(pidFile,'r')
            pid = f.read()
            f.close()

            print("Stopping BirdCDC........")
            killCmd = "kill " + pid
            os.system(killCmd)
            os.remove(pidFile)
        except:
            print("Check BirdCDC PID File.")
    else:
        exit()