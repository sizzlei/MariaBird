import mysql.connector
import BirdNest
import paramiko
import os
import sys

# Configure
config = BirdNest.configure('bird.cnf')
logwrite = BirdNest.signlog(config.cdc('CDC_LOG_PATH'),10,10485760)
eggIdentifier = BirdNest.identifier()

class Collector:
    def __init__(self,**kwargs):
        self.dbconfig = {
                        'host':kwargs['ip'],
                        'port':kwargs['port'],
                        'user':kwargs['user'],
                        'password':kwargs['passwd']
                        }
        self.sshconfig = {
                        'host':kwargs['ip'],
                        'port':kwargs['sshport'],
                        'user':kwargs['sshuser'],
                        'password':kwargs['sshpass'],
                        'timeout':3
                        }

        self.nestSSH = paramiko.SSHClient()
        self.nestSSH.set_missing_host_key_policy(paramiko.AutoAddPolicy())        

    def binFlag(self,binaryfile,position):        
        conn = mysql.connector.connect(**self.dbconfig)
        try:
            cur = conn.cursor(dictionary = True)
            cur.execute("SHOW BINARY LOGS")
            curData = cur.fetchall()
            cur.close()
        except Exception as DBERR:
            logwrite.err(DBERR)
        finally:
            conn.close() 

        # Searching
        maxSrcindex = len(curData)-1
        minChecker = 0
        position = int(position)

        # Return Value
        # 0 : Waiting
        # 1 : Running
        # 2 : NextFile
        # 9 : Error
        for Srcindex,Srcinfo in enumerate(curData):
            # binaryfile in Log list
            if binaryfile == Srcinfo['Log_name']:
                # binaryfile equal Last File
                if Srcindex == maxSrcindex:
                    # position equal Last File Size
                    if position == Srcinfo['File_size']:
                        return 0
                    else:
                        return 1
                # binaryfile after binaryfile
                elif Srcindex < maxSrcindex:
                    # position equal Last File Size (Next File Signal)
                    if position == Srcinfo['File_size']:
                        return 2
                    # position dif Last File Size (Running)
                    elif position < Srcinfo['File_size']:
                        return 1
                    else:
                        return 9
                # After Binaryfile not exists
                else:
                    return 0 
            else:
                minChecker += 1        
        if minChecker == len(curData):
            return 9
    
    def binLoader(self,binaryfile,position):
        # Events
        conn = mysql.connector.connect(**self.dbconfig)
        try:
            cur = conn.cursor(dictionary = True)
            cur.execute('show binlog events in %s from %s',(binaryfile,int(position)))            
            eventData = cur.fetchall()
            cur.close()
        except Exception as DBERR:
            logwrite.err(DBERR)
        finally:
            conn.close()      
        
        # Declare Queue Variables
        trx = {}
        commitQueue = []
        mapDBList = []
        mapTBList = []
        try:
            if eventData:
                
                for rowData in eventData:
                    if rowData['Event_type'] == 'Gtid': 
                        # Init Transaction    
                        trx = {}              

                        # GTID Parsing
                        gtidno = eggIdentifier.gtid(rowData['Info'])
                        trx['GTID'] = gtidno
                        trx['STRPOS'] = rowData['Pos']    

                        # Target DB / Table
                        mapDBList = []
                        mapTBList = []     
                    elif rowData['Event_type'] == 'Table_map':
                        schemainfo = eggIdentifier.schema(rowData['Info'])
                        mapDBList.append(schemainfo[0])
                        mapTBList.append(schemainfo[1])
                        trx['MAPDB'] = list(set(mapDBList))
                        trx['MAPTB'] = list(set(mapTBList))
                    elif rowData['Event_type'] == 'Xid':
                        trx['ENDPOS'] = rowData['End_log_pos']
                        trx['XID'] = rowData['Info']  
                        commitQueue.append(trx)
                        
                        

                if commitQueue:
                    return commitQueue
                else:
                    resultEnd = eventData[len(eventData)-1]['End_log_pos']
                    noneDataDic = {'XID':0,'ENDPOS':resultEnd}
                    commitQueue.append(noneDataDic)
                    return commitQueue
            else:
                noneDataDic = {'XID':0,'ENDPOS':0}
                commitQueue.append(noneDataDic)
                return commitQueue
        except Exception as binLoaderERR:
            logwrite.err(binLoaderERR)
            
    def binFilter(self,datalist,conditionDB,conditionTB):
        returnData = []
        try:
            if datalist:
                for rowData in datalist:
                    dbcheckpnt = 0
                    tbcheckpnt = 0
                    # Database Filter
                    if conditionDB:
                        for rowDB in rowData['MAPDB']:
                            if rowDB in conditionDB:
                                dbcheckpnt += 1                                
                                if conditionTB:
                                    for rowTB in rowData['MAPTB']:
                                        if rowTB in conditionTB:
                                            tbcheckpnt += 1
                                            
                                            if dbcheckpnt == len(rowData['MAPDB']) and tbcheckpnt == len(rowData['MAPTB']):
                                                returnData.append(rowData)
                                        else:
                                            # Trx Table Not in Condition Table
                                            pass
                                else:
                                    if dbcheckpnt == len(rowData['MAPDB']):
                                        returnData.append(rowData)
                            else:
                                # Trx Database Not in Condition Database
                                pass
                    # Not set Database      
                    else:
                        if conditionTB:
                            for rowTB in rowData['MAPTB']:
                                if rowTB in conditionTB:
                                    tbcheckpnt += 1
                                            
                                    if tbcheckpnt == len(rowData['MAPTB']):
                                        returnData.append(rowData)
                                else:
                                    # Trx Table Not in Condition Table
                                    pass 
                        else:
                            returnData.append(rowData)
            else:
                returnData = 0   

            if returnData:
                return returnData
            else:
                returnData = 0
                return returnData
        except Exception as filtererr:
            logwrite.err(filtererr)

    def binFinder(self,binaryFile):
        # Find NextBinary log
        conn = mysql.connector.connect(**self.dbconfig)
        try:
            cur = conn.cursor(dictionary = True)
            cur.execute('SHOW BINARY LOGS')            
            binaryList = cur.fetchall()
            cur.close()
        except Exception as DBERR:
            logwrite.err(DBERR)
        finally:
            conn.close()    

        for binidx,binfile in enumerate(binaryList):
            if binaryFile == binfile['Log_name']:
                return binaryList[binidx+1]['Log_name']

    def binMaker(self,dataDic):
        try:
            self.nestSSH.connect(self.sshconfig['host'],
                                username=self.sshconfig['user'],
                                # password=self.sshconfig['password'],
                                port=self.sshconfig['port'],
                                timeout=self.sshconfig['timeout']                           
                                )
        except Exception as sshconnecterr:
            logwrite.err(sshconnecterr)

        binaryParserCom = config.cdc('CDC_MYSQL_CLIENT_PATH') + '/bin/mysqlbinlog -u%s -p%s %s --start-position=%s --stop-position=%s'   
        try:             
            for queueRow in dataDic['QueueData']:
                binaryParser = binaryParserCom % (self.dbconfig['user'],self.dbconfig['password'],(config.cdc('SOURCE_BINARY_LOG_PATH') + '/' + dataDic['BINLOGFILE']),queueRow['STRPOS'],queueRow['ENDPOS'])
                
                stdin, stdout, stderr = self.nestSSH.exec_command(binaryParser)
                rowData = stdout.read()
                rowData = rowData.decode('ISO-8859-1')
                convData = rowData.split('\n')

                eggFile = config.cdc('CDC_EGG_FILE_PATH') + '/' + str(dataDic['BINLOGFILE'])+'.'+ str(queueRow['ENDPOS']) + '.sql'

                sqlFile = open(eggFile,'w')
                for row in convData:
                    sqlFile.write(row + '\n')
                sqlFile.close()

            self.nestSSH.close()
        except Exception as binMakerERR:
            logwrite.err(binMakerERR)
 

class Executor:
    def __init__(self,**kwargs):
        self.dbconfig = {
                'host':kwargs['ip'],
                'port':kwargs['port'],
                'user':kwargs['user'],
                'password':kwargs['passwd']
                }

    def binlistCollector(self):
        eggList = os.listdir(config.cdc('CDC_EGG_FILE_PATH'))

        for idx,eggname in enumerate(eggList):
            eggList[idx] = eggname.split('.')

        eggList = sorted(eggList,key = lambda x : (x[1],int(x[2])))
        
        for idx,eggname in enumerate(eggList):
            eggList[idx] = '.'.join(eggname)
        
        return eggList
    
    def binaryRemover(self,dataList):
        try:
            for eggFile in dataList:
                eggPath = config.cdc('CDC_EGG_FILE_PATH') + '/' + str(eggFile)

                if os.path.isfile(eggPath):
                    os.remove(eggPath)
        except Exception as binaryRemoverERR:
            logwrite.err(binaryRemoverERR)

    def binExecutor(self,dataList):
        exeComtemp = config.cdc('CDC_MYSQL_CLIENT_PATH') + '/bin/mysql -u%s -p%s --host=%s --port=%s < %s'
        try:
            for eggFile in dataList:
                eggPath = config.cdc('CDC_EGG_FILE_PATH') +'/'+ str(eggFile)    
                exeCommand = exeComtemp % (self.dbconfig['user'],self.dbconfig['password'],self.dbconfig['host'],self.dbconfig['port'],eggPath)
                
                if os.system(exeCommand) == 0:
                    logwrite.info('END EXECUTE : ' + str(eggFile))                    
                else:
                    logwrite.err(str(eggFile))

            return 0
        except Exception as INCERR:
            logwrite.err(INCERR)