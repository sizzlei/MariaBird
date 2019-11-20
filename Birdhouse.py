import BirdFamily
import BirdNest
import time
import sys
import threading

# Module
netCnf = BirdNest.configure('bird.cnf')
makeEgg = BirdFamily.Collector(ip=netCnf.source('IP'),
                                port=netCnf.source('PORT'),
                                user=netCnf.source('DBUSER'),
                                passwd=netCnf.source('DBPASS'),
                                sshuser=netCnf.source('SSHUSER'),
                                sshpass=netCnf.source('SSHPASS'),
                                sshport=netCnf.source('SSHPORT')
                                )

incuEgg = BirdFamily.Executor(ip=netCnf.target('IP'),
                                port=netCnf.target('PORT'),
                                user=netCnf.target('DBUSER'),
                                passwd=netCnf.target('DBPASS')
                                )

def BirdMommy():
    QueueID = 1
    mapFile = netCnf.cdc('BINARY_FILE')
    mapPos = netCnf.cdc('BINARY_POSITION')

    while True:
        eggFlag = makeEgg.binFlag(mapFile,mapPos)
        if eggFlag == 1:
            eggName = makeEgg.binLoader(mapFile,mapPos)

            # Condition Filtering
            if eggName[0]['XID'] == 0:
                eggList = eggName
            else:
                if netCnf.condition('DATABASE') == 'Undefined' or netCnf.condition('TABLES') == 'Undefined':
                    eggList = eggName
                else:
                    eggList = makeEgg.binFilter(eggName,netCnf.condition('DATABASE'),netCnf.condition('TABLES'))   

            # Next Search Position
            if eggList == 0:    
                if eggName:   
                    nextPosition = eggName[len(eggName)-1]['ENDPOS']
                else:
                    nextPosition = mapPos
            else:
                if eggList[0]['XID'] == 0:
                    nextPosition = eggList[len(eggList)-1]['ENDPOS']
                    eggList = 0
                else:
                    nextPosition = eggList[len(eggList)-1]['ENDPOS']
            # Make Dictionary
            eggGroup = {'idx':QueueID,'QueueData':eggList,'ENDPOSITION':nextPosition,'BINLOGFILE':mapFile}
            
            # Increment
            if eggGroup['QueueData'] == 0:
                QueueID = QueueID
            else:
                QueueID += 1
                # Make Egg
                makeEgg.binMaker(eggGroup)

            # Next Queue Start Position
            mapPos = eggGroup['ENDPOSITION']

        elif eggFlag == 2:
            time.sleep(1)
            mapFile = makeEgg.binFinder(mapFile)
            mapPos = 0
        elif eggFlag == 0:
            time.sleep(1)
        else:
            sys.exit()

def BirdDaddy():
    while True:
        # Binary List Load
        eggList = incuEgg.binlistCollector()
        if eggList:
            # Binary Exe
            IncStatus = incuEgg.binExecutor(eggList)
            
            # Remove Binary File
            if IncStatus == 0:                
                incuEgg.binaryRemover(eggList)      

            else:
                sys.exit()
        else:
            time.sleep(1)

def Birdhouse():
    # Thread Configure
    threadMommy = threading.Thread(target=BirdMommy)          
    threadDaddy = threading.Thread(target=BirdDaddy)      

    # Thread Start
    threadMommy.start()
    threadDaddy.start()
