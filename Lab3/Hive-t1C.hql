set hivevar:studentId=20105729; --Please replace it with your student id 
DROP TABLE ${studentId}_facebooktraffic;
DROP TABLE ${studentId}_mytraffic;


CREATE TABLE ${studentId}_mytraffic (url STRING, ipaddress STRING, time TIMESTAMP)
ROW FORMAT DELIMITED FIELDS TERMINATED BY ',';
LOAD DATA LOCAL INPATH './Input_data/1/traffic.csv'
INTO TABLE ${studentId}_mytraffic;


CREATE TABLE ${studentId}_facebooktraffic AS
SELECT * FROM ${studentId}_mytraffic
WHERE url=="www.Facebook.com" AND 
time > unix_timestamp('2014-02-14 00:00:00') AND time <=unix_timestamp('2014-02-15 00:00:00');
--
INSERT OVERWRITE LOCAL DIRECTORY './task1c-out/'
     ROW FORMAT DELIMITED
     FIELDS TERMINATED BY '\t'
     STORED AS TEXTFILE
  SELECT * FROM ${studentId}_facebooktraffic;

