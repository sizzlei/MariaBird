import configparser
import mysql.connector
import re
import os
import sys
import logzero

class configure:
    def __init__(self,configPath):
        self.config = configparser.ConfigParser()
        self.config.read(configPath)
    
    def getConfig(self,sectionName):
        optDic = {}
        for key in self.config[sectionName]:
            optDic[key] = self.config[sectionName][key]

        return optDic
     
class identifier:
    def __init__(self):
        self.reschema = re.compile(r'[`a-zA-Z0-9_]+[.]+[`a-zA-Z0-9_]+',re.I)
    
    def schema(self,text):
        colSchema = self.reschema.search(text)
        mapData = colSchema.group()
        returnSchema = mapData.split('.')
        return returnSchema

class getDBdata:
    def __init__(self,**kwargs):
        self.dbConf = {
            'host':kwargs['ip'],
            'port':kwargs['port'],
            'user':kwargs['user'],
            'password':kwargs['passwd']                        
        }

    def getEventLoader(self,binFile,binPos):
        conn = mysql.connector.connect(**self.dbConf)
        try:
            cur = conn.cursor(dictionary = True)
            cur.execute("show binlog events in %s from %s",(binFile,int(binPos)))
            data = cur.fetchall()
            cur.close()
            if data :
                return data
            else:
                data = []
                rowDic = {
                    'Log_name' : binFile,
                    'End_log_pos' : binPos,
                    'Event_type' : 'Next'
                }
                data.append(rowDic)
                return data
        except Exception as connecterr:           
            print(connecterr)
        finally:
            conn.close()        
   
class binlogParser:
    def __init__(self,refDB,refTB):
        self.refTable = refTB.split(',')
        self.refDatabase = refDB
        self.mapParser = identifier()

    def makeTRX(self,logData):
        rowTRX = {}
        backLog = []
        eventType = ["Write_rows_v1", "Update_rows_v1", "Delete_rows_v1"]
        chkTablemap = 0
        for idx, logRow in enumerate(logData):           
            if logRow['Event_type'] == 'Table_map' and chkTablemap == 0:
                # Check Condition
                mapName = self.mapParser.schema(logRow['Info'])               
                if mapName[0] == self.refDatabase and mapName[1] in self.refTable:
                    rowTRX['Type'] = 'Query'
                    rowTRX['DB'] = mapName[0]
                    rowTRX['Table'] = mapName[1]
                    rowTRX['Logname'] = logRow['Log_name']
                    rowTRX['startPos'] = logRow['Pos']
                    chkTablemap = 1

            elif logRow['Event_type'] in eventType and chkTablemap == 1:
                rowTRX['endPos'] = logRow['End_log_pos']
                rowTRX['Event'] = logRow['Event_type']
                               
                if 'STMT_END_F' in logRow['Info']:                    
                    backLog.append(rowTRX)                    
                    # Init TRX
                    rowTRX = {}
                    chkTablemap = 0

        return backLog

class logDumper:
    def __init__(self,mysqlPath,logPath,dumpPath):
        self.mysqlClientPath = mysqlPath
        self.binlogPath = logPath
        self.logDumpPath = dumpPath

    def dumpMaker(self,logFile,strPos,endPos,fromdb,todb): 
        dumpString = self.mysqlClientPath + "/bin/mysqlbinlog %s/%s --start-position=%d --stop-position=%d --rewrite-db='%s->%s' > %s"
        fileString = self.logDumpPath + '/%s.%s'
        fileName = fileString % (logFile,strPos)
        dumpCommand = dumpString % (self.binlogPath,logFile,int(strPos),int(endPos),fromdb,todb,fileName)
        # print(dumpCommand)
        try:
            os.system(dumpCommand)
        except Exception as dumperr:
            print(dumperr)
        
class logRunner:
    def __init__(self,mysqlPath,**kwargs):
        self.mysqlClientPath = mysqlPath
        self.targetConf = {
            'host':kwargs['ip'],
            'port':kwargs['port'],
            'user':kwargs['user'],
            'password':kwargs['passwd'],
            'dumpPath':kwargs['dumppath']                      
        }

    def eventSorted(self):
        eggList = os.listdir(self.targetConf['dumpPath'])        
        if eggList:
            for idx,eggname in enumerate(eggList):
                eggList[idx] = eggname.split('.')
            
            eggList = sorted(eggList,key = lambda x : (x[1],int(x[2])))

            for idx,eggname in enumerate(eggList):
                eggList[idx] = '.'.join(eggname)
        
        return eggList

    def eventRunner(self,dataList,cdcPath,endFile):
        runString = self.mysqlClientPath + "/bin/mysql -u%s -p'%s' --host=%s --port=%s < %s/%s"
        fileInfo = cdcPath + "/" + endFile
        endF = open(fileInfo,'w')
        for eventNm in dataList:
            runCommand = runString % (self.targetConf['user'],self.targetConf['password'],self.targetConf['host'],self.targetConf['port'],self.targetConf['dumpPath'],eventNm)            
            try:            
                os.system(runCommand)
                eggPath = self.targetConf['dumpPath'] + '/' + str(eventNm)

                if os.path.isfile(eggPath):
                    os.remove(eggPath)            
                
                endF.write(eventNm+"\n")

            except Exception as binaryRemoverERR:
                print(binaryRemoverERR)

        endF.close()
