import configparser
import mysql.connector
import re
import os
import sys
import logzero
import subprocess
import json

"""
Logzero Module Import
"""
class logwriter:
    def __init__(self,file,backCnt):
        log_format = '%(color)s[%(asctime)s] [%(levelname)s]%(end_color)s %(message)s'
        formatter = logzero.LogFormatter(fmt=log_format)        
        self.logger = logzero.setup_default_logger(file,disableStderrLogger=True,formatter=formatter,maxBytes=100000,backupCount=backCnt)
                
    def info(self,msg):
        self.logger.info(msg)
    def err(self,msg):
        self.logger.error(msg)

"""
Configparer Module Import
"""
class configure:
    def __init__(self,configPath):
        self.config = configparser.ConfigParser()
        self.config.read(configPath)
    
    def getConfig(self,sectionName):
        optDic = {}
        for key in self.config[sectionName]:
            optDic[key] = self.config[sectionName][key]

        return optDic

"""
Source Info Regexp
"""
class identifier:
    def __init__(self):
        self.reschema = re.compile(r'[`a-zA-Z0-9_]+[.]+[`a-zA-Z0-9_]+',re.I)
    
    def schema(self,text):
        colSchema = self.reschema.search(text)
        mapData = colSchema.group()
        returnSchema = mapData.split('.')
        return returnSchema

"""
Binary Log Event Search
"""
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
   
"""
Binary Log Event Parsing -> Dict
"""
class binlogParser:
    def __init__(self,refDB,refTB):
        self.refTable = refTB
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

"""
Binary Log Event Get
"""
class logDumper:
    def __init__(self,mysqlPath,logPath):
        self.mysqlClientPath = mysqlPath
        self.binlogPath = logPath

    def dumpMaker(self,logFile,strPos,endPos,fromdb,todb): 
        dumpString = self.mysqlClientPath + "/bin/mysqlbinlog %s/%s --start-position=%d --stop-position=%d --rewrite-db='%s->%s' --base64-output=DECODE-ROWS -v"
        dumpCommand = dumpString % (self.binlogPath,logFile,int(strPos),int(endPos),fromdb,todb)
        
        try:
            result = subprocess.run(dumpCommand,stdout=subprocess.PIPE,shell=True)
            deResult = result.stdout.decode('utf-8')
            queryParsor = re.compile(r'###.*',re.M)
            data = queryParsor.findall(deResult)
                        
            for idx,stricQuery in enumerate(data):
                data[idx] = stricQuery.replace('###','',1).strip()
                data[idx] = data[idx].split("=",1)

                if len(data[idx]) == 1:
                    data[idx] = data[idx][0]


            queryPack = []                   
            divLen = data[0]
            makeList = []
            
            setCol = 0
            for idx, dataSub in enumerate(data):
                if dataSub == divLen:
                    setCol += 1
            
            # Query Length
            setQueryIdx = len(data) / setCol

            # Save And Div Query
            strIdx = 0
            for queryIdx in range(1,setCol+1):   
                endIdx = setQueryIdx * queryIdx             
                makeList.append(data[int(strIdx):int(endIdx)])
                strIdx = endIdx
            
            for inIdx, inData in enumerate(makeList):    
                queryDic = {}     
                queryDiv = inData[0].split(" ")            
                queryDic["div"] = queryDiv[0]
                queryDic["Target"] = queryDiv[-1].replace("`","")            

                if queryDic["div"] == "DELETE" or queryDic["div"] == "INSERT": 
                    del inData[:2]
                    colDic = {}
                    for cols in inData:
                        colDic[cols[0]] = cols[1]

                    queryDic["data"] = colDic    
                elif queryDic["div"] == "UPDATE":            
                    del inData[:1]
                    colDic = {}
                    conDic = {}

                    for cols in inData:   
                        if cols == 'WHERE':
                            setDiv = 0
                        elif cols =="SET":
                            setDiv = 1

                        if setDiv == 0:
                            if cols != 'WHERE':
                                conDic[cols[0]] = cols[1]
                        elif setDiv == 1:
                            if cols != 'SET':                           
                                colDic[cols[0]] = cols[1]

                    queryDic["data"] = colDic
                    queryDic["condition"] = conDic  
                    
                queryDic["strPos"] = strPos
                queryDic["endPos"] = endPos
                queryDic["File"] = logFile  
                queryPack.append(queryDic)  
                
            return queryPack
        except Exception as dumperr:
            print(dumperr)

"""
Table Config Json LOAD
"""
class tableConfLoader:
    def __init__(self,confPath):
        self.tableConfigFile = confPath
        with open(self.tableConfigFile) as jsonFile:
            jsonData = json.load(jsonFile)
            self.tableAllconf = dict(jsonData)
         
    def getTableList(self):
        tableList = list(self.tableAllconf.keys())        
        return tableList

    def getAllTableConf(self):
        return self.tableAllconf

"""
Event Dict using Make Query
"""
class dataExecuter:
    def queryMaker(self,queueData,tableConf):
        setSourceNm = queueData["Target"].split(".")
        # Change Info
        for mainKey in tableConf.keys():
            if setSourceNm[1] == mainKey:
                runTarget = setSourceNm[0]+"."+tableConf[mainKey][0]["targetName"]

                # Query Set
                if queueData["div"] == "DELETE":
                    setQueryStr = "DELETE FROM %s where %s"
                    setWhereDiv = []
                    for idx,(key, value) in enumerate(queueData["data"].items()):                        
                        if tableConf[mainKey][0]["targetColumn"][idx] != "":
                            if value == "NULL":
                                condition = tableConf[mainKey][0]["targetColumn"][idx] + " is null"
                                setWhereDiv.append(condition)
                            else:
                                condition = tableConf[mainKey][0]["targetColumn"][idx] + "=" + value
                                setWhereDiv.append(condition)
                            
                    
                    setWhere = " and ".join(setWhereDiv)

                    setQuery = setQueryStr % (runTarget,setWhere)
                    return setQuery+";"

                elif queueData["div"] == "INSERT":
                    setQueryStr = "INSERT INTO %s (%s) values (%s)"
                    setKeysDiv = []
                    setValueDiv = []
                    for idx,(key, value) in enumerate(queueData["data"].items()):
                        if tableConf[mainKey][0]["targetColumn"][idx] != "":
                            setKeysDiv.append(tableConf[mainKey][0]["targetColumn"][idx])
                            setValueDiv.append(value)
                
                    setKeys = ",".join(setKeysDiv)
                    setValue = ",".join(setValueDiv)

                    setQuery = setQueryStr % (runTarget,setKeys,setValue)
                    return setQuery+";"
                
                elif queueData["div"] == "UPDATE":
                    setQueryStr = "UPDATE %s SET %s WHERE %s"
                    setDataDiv = []
                    setWhereDiv = []

                    for idx,(key, value) in enumerate(queueData["data"].items()): 
                        if tableConf[mainKey][0]["targetColumn"][idx] != "":
                            setSQL = tableConf[mainKey][0]["targetColumn"][idx] + "=" + value
                            setDataDiv.append(setSQL)
                        
                    for idx,(key, value) in enumerate(queueData["condition"].items()):  
                        if tableConf[mainKey][0]["targetColumn"][idx] != "":
                            if value == "NULL":
                                condition = tableConf[mainKey][0]["targetColumn"][idx] + " is null"
                                setWhereDiv.append(condition)
                            else:
                                condition = tableConf[mainKey][0]["targetColumn"][idx] + "=" + value
                                setWhereDiv.append(condition)                            

                    setData = ",".join(setDataDiv)
                    setWhere = " and ".join(setWhereDiv)

                    setQuery = setQueryStr % (runTarget,setData,setWhere)

                    return setQuery +";"


    def queryReplacer(self,queueData,tableConf):
        setSourceNm = queueData["Target"].split(".")
        
        for mainKey in tableConf.keys():
            if setSourceNm[1] == mainKey:
                modrunTarget = setSourceNm[0]+"."+tableConf[mainKey][0]["targetName"]

                setQueryStr = "REPLACE INTO %s (%s) values (%s)"
                setKeysDiv = []
                setValueDiv = []
                for idx,(key, value) in enumerate(queueData["data"].items()):
                    if tableConf[mainKey][0]["targetColumn"][idx] != "":
                        setKeysDiv.append(tableConf[mainKey][0]["targetColumn"][idx])
                        setValueDiv.append(value)
            
                setKeys = ",".join(setKeysDiv)
                setValue = ",".join(setValueDiv)

                setQuery = setQueryStr % (modrunTarget,setKeys,setValue)
                return setQuery+";"
                
"""
Etc File Maker
"""                
class FileMaker:
    def __init__(self,posFile,pidFile):
        self.endPosFile = posFile
        self.cdcPidFile = pidFile
    def posMake(self,logFile,endPos):
        with open(self.endPosFile,"w") as ep:
            ep.write("Last Searching File : " + logFile + "\n")
            ep.write("Last Searching Position : " + str(endPos) + "\n")

    def pidMake(self,pid):
        with open(self.cdcPidFile,"w") as pf:
            pf.write(str(pid))

    def pidKill(self):
        with open(self.cdcPidFile,"r") as pf:
            pid = pf.read()

        killCmd = "kill %s" % pid
        os.system(killCmd)
        os.remove(self.cdcPidFile)
