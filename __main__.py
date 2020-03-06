import birdModule as nest
import queue
import threading
import time
import os
import sys
import mysql.connector
import argparse

if __name__ == '__main__':
    argParm = argparse.ArgumentParser()
    argParm.add_argument("--config",help="Configure File Path.")
    argParm.add_argument("status",help="[ start | stop ]")
    argv = argParm.parse_args()
    
    try:
        # Config File Check
        confWorker = nest.configure(argv.config)

        # Config Reading
        dbConf = confWorker.getConfig('DATABASE')
        repl = confWorker.getConfig('REPL')
        birdConf = confWorker.getConfig('BIRD')

    except Exception as configErr:
        print(configErr)      
        print("[Fail] Configure load Fail.")
        sys.exit()    

    # Worker Delcare
    sourceWorker = nest.getDBdata(ip=dbConf['source_ip'],port=dbConf['source_port'],user=dbConf['dbuser'],passwd=dbConf['dbpass'])
    dumpWorker = nest.logDumper(birdConf['mysql_client_path'],birdConf['binarylog_path'])
    tableCond = nest.tableConfLoader(birdConf['table_conf'])
    tableConf = tableCond.getTableList() 
    LogWorker = nest.binlogParser(repl['fromdb'],tableConf)
    eventWorker = nest.dataExcuter()
    fileWorker = nest.FileMaker(birdConf["end_pos_file"],birdConf['pid_file'])
    mainLogger = nest.logwriter(birdConf["log_file"],5)

    # Global Transaction Queue
    logQueue = queue.Queue()
    queryQueue = queue.Queue()

    """
    Binary Event Load
    """
    def EventDataLoader(binlogFile, binlogPos):
        mainLogger.info("[ OK ] Binary Log Event Load.")
        mainLogger.info("[ OK ] Binary File : "+binlogFile)
        mainLogger.info("[ OK ] Binary Position : "+str(binlogPos))

        # Start Set File/Position
        pntFile = binlogFile
        pntPos = binlogPos

        # Event Load Loop
        while True:
            try:
                # get Event
                LogData = sourceWorker.getEventLoader(pntFile,pntPos)       
                if len(LogData) > 1:
                    # Data Dictionary
                    parsingData = LogWorker.makeTRX(LogData)
                    # Load Data Push Queue
                    if parsingData:
                        logQueue.put(parsingData)

                    # Next File and Position 
                    if LogData[-1]['Event_type'] == 'Rotate':
                        nextLog = LogData[-1]['Info'].split(';')
                        nextPos = nextLog[1].split('=')
                        pntFile = nextLog[0]
                        pntPos = int(nextPos[1])
                    else:
                        # Next Event Load
                        pntFile = LogData[-1]['Log_name']
                        pntPos = LogData[-1]['End_log_pos']  

                        fileWorker.posMake(pntFile,pntPos)

                else:
                    # None Event
                    pntFile = LogData[0]['Log_name']
                    pntPos = LogData[0]['End_log_pos']     
                    fileWorker.posMake(pntFile,pntPos)
                    time.sleep(3) 
            except Exception as DataLoaderErr:
                mainLogger.err("[Fail] EventDataLoader")
                mainLogger.err(DataLoaderErr)
                sys.exit()
            
    """
    Binary Event Make Query Dictionary
    """
    def EventDataDumper():
        mainLogger.info("[ OK ] Binary Event Dumper.")
        while True:
            try:
                # Get Queue Data
                dataSet = logQueue.get()        
                if dataSet:
                    for dataEvent in dataSet:
                        queryData = dumpWorker.dumpMaker(
                            dataEvent['Logname'],
                            dataEvent['startPos'],
                            dataEvent['endPos'],
                            repl['fromdb'],
                            repl['todb'])
                        # Push Query Queue
                        queryQueue.put(queryData)
            except Exception as DataDumperErr:
                mainLogger.err("[Fail] EventDataDumper")
                mainLogger.err(DataDumperErr)
                sys.exit()

    """
    Binary Event Query Runner
    """
    def EventRunner():
        mainLogger.info("[ OK ] Binary Event Query Runner.")
        
        # Load Table Config
        tableConfig = tableCond.getAllTableConf()
        # Target Connection
        targetDB = {
                    'host':dbConf['target_ip'],
                    'port':dbConf['target_port'],
                    'user':dbConf['dbuser'],
                    'password':dbConf['dbpass']                     
                }

        tcon = mysql.connector.connect(**targetDB)    
        tcon.autocommit = True
        
        while True:
            tcur = tcon.cursor()  
            # Query Queue Load
            querySet = queryQueue.get()
            for queueQuery in querySet:
                # Query Maker  
                queryString = eventWorker.queryMaker(queueQuery,tableConfig)
                
                # Debug 
                # mainLogger.info(queueQuery)
                
                try:          
                    # Run Query
                    tcur.execute(queryString,params=None) 
                    # mainLogger.info(queueQuery['File']+" StartPos = "+str(queueQuery['strPos'])+ " EndPos = "+str(queueQuery['endPos']))        
                    # mainLogger.info(queueQuery["div"] + " Affetch Count : " + str(tcur.rowcount))
                    
                    # raise Error
                    if queueQuery["div"] != "DELETE" and tcur.rowcount == 0:
                        raise ValueError()
                except Exception as DataRunnerErr:
                    # UPDATE / INSERT -> REPLACE INTO
                    mainLogger.err("EventDataRunner")    
                    mainLogger.err(DataRunnerErr)
                    if queueQuery["div"] != "DELETE" and tcur.rowcount == 0:                                            
                        mainLogger.err(queueQuery['File']+" StartPos = "+str(queueQuery['strPos'])+ " EndPos = "+str(queueQuery['endPos']))        
                        mainLogger.err("Exception Handler====================================")
                        mainLogger.err("Origin Query : " + queryString)

                        # Exception Main Exe
                        preQueryString = eventWorker.queryReplacer(queueQuery,tableConfig)                
                        tcur.execute(preQueryString,params=None) 

                        mainLogger.err("Exception Query : " + preQueryString)
                        mainLogger.err(queueQuery["div"] + " Exception Affetch Count : " + str(tcur.rowcount))
                        mainLogger.err("=================================================")
       
            tcur.execute("select 1",params=None)   
            tcur.close()     
                
        # Connection Close        
        tcon.close()  
            
    """
    Main Thread
    """
    def mainThread():
        try:
            # Thread Configure
            dataLoader = threading.Thread(target=EventDataLoader,args=(repl['binary_file'],repl['binary_pos']))
            dataExporter = threading.Thread(target=EventDataDumper)
            dataRunner = threading.Thread(target=EventRunner)

            # Thread Start
            dataLoader.start()
            dataExporter.start()
            dataRunner.start()
        except Exception as MainErr:
            mainLogger.err("[Fail] Main Thread.")
            mainLogger.err(MainErr)
            sys.exit()

    """
    Daemon Thread
    """
    def cdcDaemon():
        try:
            pid = os.fork()            
            if pid > 0:
                print("[ OK ] BirdCDC PID: %d" % pid)
                fileWorker.pidMake(pid)
                sys.exit()
        except Exception as e:
            print('Unable to Fork.')
            sys.exit()
        # Main Service Start
        mainThread()

    # Service Start
    if argv.status == "start" or argv.status == "START":
        if os.path.isfile(birdConf['pid_file']):
            print("[ Fail ] BirdCDC Stop Running.")
            sys.exit()
        else:
            try:            
                # Daemon Process
                cdcDaemon()
            except Exception as startErr:
                print(startErr)
                sys.exit()

    # Service Stop
    elif argv.status == "stop" or argv.status == "STOP":
        try:            
            if os.path.isfile(birdConf['pid_file']):
                fileWorker.pidKill()
                print("[ OK ] BirdCDC Stop Running.")
                mainLogger.info("BirdCDC Shutdown...")
            else:
                raise Exception("[Fail] PID File not Exists.")
        except Exception as stopErr:
            print(stopErr)
            sys.exit()
    else:
        sys.exit()