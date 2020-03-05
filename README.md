# BirdCDC for ISE
학습기 Database 분리시 데이터 동기화를 목적으로 개발된 스크립트로서 필요시 NoSQL 데이터베이스로의 데이터 이전도 가능(약간의 스크립트 수정 필요)

## Configure
### bird.cnf
> 설정파일 명은 서비스 시작시에 명시함으로 변경 되어도 상관 없습니다. 

#### DATABASE
    Database 섹션은 소스 서버와 반영서버의 정보
+ SOURCE_IP
    - 데이터 캡쳐 대상 서버
+ SOURCE_PORT
    - 데이터 캡쳐 대상 서버 포트
+ TARGET_IP
    - 변경 데이터 반영 대상 서버
+ TARGET_PORT
    - 변경 데이터 반영 대상 서버 포트
+ DBUSER
    - Source / Target 서버 접근 계정
+ DBPASS
    - Source / Target 서버 접근 계정 패스워드

#### BIRD
    Bird 섹션은 BirdCDC에서 사용되는 Path를 설정
+ MYSQL_CLIENT_PATH
    - MySQL Client가 존재하는 Base 경로
+ BINARYLOG_PATH
    - Binary Log 적재 경로
+ TABLE_CONF
    - 테이블 Mapping 설정 파일
+ END_POS_FILE
    - Binary Log 탐색시 마지막으로 탐색한 파일 및 포지션을 기록하는 파일
+ PID_FILE
    - 서비스 PID 파일
+ LOG_FILE
    - CDC 로그 기록 파일

#### REPL
    Repl 섹션은 CDC의 시작과 변경에 관한 설정
+ BINARY_FILE
    - CDC 시작 Binary File
+ BINARY_POS
    - CDC 시작 Binary File Position
+ TODB
    - 반영 서버의 스키마
+ FROMDB
    - 소스 서버의 스키마

#### Example
```
[DATABASE]
SOURCE_IP = 172.16.-
SOURCE_PORT = 3306
TARGET_IP = 172.16.-
TARGET_PORT = 3307
DBUSER = bird
DBPASS = Bird123!@#

[BIRD]
MYSQL_CLIENT_PATH = /mysql
BINARYLOG_PATH = /log/binary_log
TABLE_CONF = /home/server/birdcdc/tableConfig.cnf
END_POS_FILE = /home/server/birdcdc/birdEndpos.log
PID_FILE = /home/server/birdcdc/birdcdc.pid
LOG_FILE = /home/server/birdcdc/cdc.log

[REPL]
BINARY_FILE = mysql-bin.000286
BINARY_POS = 25542480
TODB = test1
FROMDB = test
```

### tableConfig.cnf
tableConfig 파일은 소스 테이블에 대한 컬럼 맵핑과 반영 테이블에 대한 컬럼 정보를 Json 형태로 관리

#### Json Struct
```
{
    "{Source Table Name}": [
        {
            "Column" : ["Source TB Column 1","Source TB Column 2","Source TB Column 3"],
            "targetName" : "{Target Table Name}",
            "targetColumn" : ["Target TB Column 1","Target TB Column 2","Target TB Column 3"]
        }
    ]
}
```

#### Example
```
{
    "test1": [
        {
            "Column" : ["id","pass","name"],
            "targetName" : "test_1",
            "targetColumn" : ["id","pass","name"]
        }
    ],
    "test2": [
        {
            "Column" : ["id","pass","name"],
            "targetName" : "test_2",
            "targetColumn" : ["id","pass","name"]
        }
    ]
}
```
> 맵핑시 컬럼수가 틀린경우에도 Column Array와 targetColumn Array의 길이는 동일 해야하며 빈공백으로 입력합니다.

```
{
    "test1": [
        {
            "Column" : ["id","pass","name"],
            "targetName" : "test_1",
            "targetColumn" : ["id","pass",""]
        }
    ]
}
```

## Start
BirdCDC는 python3.0 이상을 기반으로 작성되었으며, 아래와 같이 실행할 수 있습니다. 

``` 
python3 server/birdcdc --help
usage: birdcdc [-h] [--config CONFIG] status

positional arguments:
  status           [ start | stop ]

optional arguments:
  -h, --help       show this help message and exit
  --config CONFIG  Configure File Path.
```
#### Example
##### Service Start
``` python3 birdcdc /home/server/birdcdc/bird.cnf start ```

##### Service Stop
``` python3 birdcdc /home/server/birdcdc/bird.cnf stop ```

## Connect Accout Privileges
Source Database 와 Target Database에는 아래의 권한을 가진 계정이 존재해야합니다. 
#### Source Database : ``` select, replication client, replication slave ```
#### Target Database : ``` select, update, insert, delete ```

