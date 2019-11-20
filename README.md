# MariaBird
MariaDB Transaction CDC
MariaBird는  MariaDB로 입력되는 트랜잭션에 대해 원하는 조건을 Tracking하여 물리적으로 분리된 서버로 데이터를 이관하는 스크립트입니다.

## 개발 Python 버전
Python 3.7.3

##  사용 모듈
|Module Name|Version|
|:---------:|:-----:|
|parkmiko|2.6.0|
|mysql.connector-python|8.0.16|
|logzero|1.5.0|
|configparser|3.7.4|

## CDC 사용 조건
+ Source Database 는 아래 옵션이 적용되어야 합니다.
  + binlog_format = row
+ CDC가 실행되는 서버에는 eggFile(sql 파일)이 저장될 충분한 공간이 제공되어야 합니다.
  + 트랜잭션당 생성
+ CDC가 실행되는 서버에는 MySQL 클라이언트가 존재해야 합니다.
+ eggFile을 Read 하고 실행시 MySQL Client를 통해 타겟 데이터베이스에 적용
+ 포트 정책은 아래와 같이 허용되어야 합니다.
  + CDC → Source Database : 22
  + CDC → Source Database : 3306
  + CDC → Target Database : 3306
+ CDC에서 각 Database 로 접근하는 계정은 아래의 권한을 획득해야합니다.
  + SELECT
  + INSERT
  + UPDATE
  + DELETE
  + CREATE
  + DROP
  + SUPER
  + EXECUTE
  + REPLICATION SLAVE
  + REPLICATION CLIENT
+하나의 트랜잭션안에 원하는(Condition 조건)테이블외에 다른 테이블에 대한 데이터가 존재하는 경우 해당 테이블 또는 db에 대해 조건 추가 및 테이블 데이터 이관이 동시에 이루어져야합니다.
+현재는 하나의 트랜잭션에서 각기 다른 쿼리를 구분할 수 없습니다.

***MariaBird-CDC는 Structure의 변화에 대응하지 않습니다.  그러므로 테이블 구조 변경시에는 Dump를 통해 재적용되어야 합니다.***

## MariaBird Thread
MariaBird는 2개의 Thread로 실행되며 그 역활은 아래와 같습니다.
+ threadMommy: Source Database 의 Data가 Target Database에 적용되기 직전까지 의 모든 역할을 수행하며 Egg File을 생성합니다.(sql File)
+ threadDaddy: threadMommy에서 생성한 Egg File을 Source Database에 적용 후 후처리를 진행합니다.

## BirdNest
BirdNest.py 는 __main__.py 와 BirdFamily.py에서 사용하는 기본적인 모듈을 제공합니다.

### configure
MariaBird 디렉토리 내의 bird.cnf 파일을 읽어 섹션별로 정리합니다.

Section은 크게 4가지가 있으며 이는 별도로 다룰 예정입니다.
SOURCE_DB : 소스 데이터베이스에 대한 접근 정보
TARGET_DB : 타겟 데이터베이스에 대한 접근 정보
CDC : CDC를 사용하기 위한 디렉터리 설정 및 시작 파일 설정
CONDITION : BINARY 로그를 읽을시 적용되는 데이터베이스와 테이블 조건

  ##### signlog
  각 모듈에서 사용하는 로그 기록을 정의 합니다.
  메인 로직과 세부 로직의 로그를 별도로 구성 할 수 있습니다.

  ##### identifier
  GTID와 TABLE MAP에 대한 정규식 적용 및 값을 반환 합니다.
  추후 Structure에 대응하도록 수정 예정입니다.

## BirdFamaily
### Collector
바이너리 로그에 대한 이벤트를 수집하고 SQL파일을 생성하는 클래스이며 아래의 Function을 가지고 있습니다.
  ##### binFlag(Binary File,position)
  바이너리 로그와 포지션을 입력받아 진행 여부상태를 판독합니다.
  + 0 ; 대기
  + 1 : 진행
  + 2 : 다음 바이너리 파일
  + 9 : 에러
  ##### binLoader(Binary File,position)
  최초 입력된 바이너리 파일과 포지션으로부터 시작해서 바이너리 이벤트를 Parsing하고 트랜잭션 단위로 구성합니다.
  구성시 Queue를 생성하여 리턴 합니다.

  ##### binFilter(Queue,Condition Database, Condition Table)
  binLoader에서 리턴한 Queue를 입력받아 Config에서 설정한 조건대로 필터링 하여 가공된 Queue를 리턴 합니다.

  ##### binFinder(Binary File)
  binFlag 에서 리턴값이 2인경우 다음 바이너리 파일을 인식하여 전달합니다.

  ##### binMaker
  binFilter에서 가공된 Queue를 전달 받아 sql파일을 생성합니다.

### Executor
Collector 에서 발행된 SQL파일을 리스트 정렬을 통해 순서대로 정렬하고 읽어서 타겟 데이터에 베이스에 적용합니다.
  ##### binlistCollector
  sql파일이 생성된 디렉터리를 읽어 sql파일 리스트를 추출하고 순서에 맞게 재정렬 합니다.

  ##### binExecutor(Binary File List)
  binlistCollector에서 전달된 리스트를 읽고 각파일을 타겟테이블에 적용합니다.

  ##### binaryRemover(BinaryFile List)
  binExecutor에서 파일을 모두 적용하게 되면 리스트를 반환하는데 이 리스트를 전달 받아 적용된 파일을 삭제 처리합니다.
