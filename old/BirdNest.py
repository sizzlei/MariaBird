import configparser
import logzero
from logzero import logger
import re


class configure:
    def __init__(self,_configPath):
        self.config = configparser.ConfigParser()
        self.config.read(_configPath)       
    
    def source(self,option_name):
        try:
            return_data = self.config['SOURCE_DB'][option_name]
        except:
            return_data = 'Undefine'
        finally:    
            return return_data
    
    def target(self,option_name):
        try:
            return_data = self.config['TARGET_DB'][option_name]
        except :
            return_data = 'Undefined '
        finally:    
            return return_data         
    def cdc(self,option_name):
        try:
            return_data = self.config['CDC'][option_name]
        except:
            return_data = 'Undefined '
        finally:    
            return return_data

    def condition(self,option_name):
        try:
            if option_name == 'DATABASE' or option_name == 'TABLES':
                return_data = self.config['CONDITION'][option_name]
                return_data = return_data.split(',')
            else:
                return_data = self.config['CONDITION'][option_name]
        except:
            return_data = 'Undefined'
        finally:    
            if return_data[0] == '':
                return_data = 'Undefined'
            
            return return_data

class signlog:
    def __init__(self,file,backCnt,MaxsizeBytes):
        log_format = '%(color)s[%(asctime)s] [%(levelname)s]%(end_color)s %(message)s'
        formatter = logzero.LogFormatter(fmt=log_format)
        logzero.setup_default_logger(formatter=formatter,maxBytes=MaxsizeBytes,backupCount=backCnt)
        logzero.logfile(file,disableStderrLogger=True)  
    def info(self,msg):
        logger.info(msg)
    def err(self,msg):
        logger.error(msg)

class identifier:
    def __init__(self):
        self.reGtid = re.compile(r'[0-9]+[-][0-9]+[-][0-9]+')
        self.reschema = re.compile(r'[`a-zA-Z0-9_]+[.]+[`a-zA-Z0-9_]+',re.I)
    
    def gtid(self,text):
        colGtid = self.reGtid.search(text)
        returnGtid = colGtid.group()
        return returnGtid

    def schema(self,text):
        colSchema = self.reschema.search(text)
        mapData = colSchema.group()
        returnSchema = mapData.split('.')
        return returnSchema
    

