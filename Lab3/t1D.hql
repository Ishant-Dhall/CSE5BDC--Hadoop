set hivevar:studentId=20105729; --Please replace it with your student id 
DROP TABLE ${studentId}_CategoryTraffic;
DROP TABLE ${studentId}_mydomains;
DROP TABLE ${studentId}_mytraffic;
---------

CREATE TABLE ${studentId}_mydomains (url STRING, category STRING)
ROW FORMAT DELIMITED FIELDS TERMINATED BY ',';
LOAD DATA LOCAL INPATH './Input_data/1/domains.csv'
INTO TABLE ${studentId}_mydomains;

--------
CREATE TABLE ${studentId}_mytraffic (url STRING, ipaddress STRING, time TIMESTAMP)
ROW FORMAT DELIMITED FIELDS TERMINATED BY ',';
LOAD DATA LOCAL INPATH './Input_data/1/traffic.csv'
INTO TABLE ${studentId}_mytraffic;

--------
CREATE TABLE ${studentId}_CategoryTraffic AS
SELECT ${studentId}_mydomains.category, count(1) AS counts
FROM ${studentId}_mydomains 
INNER JOIN ${studentId}_mytraffic
ON (${studentId}_mytraffic.url = ${studentId}_mydomains.url)
GROUP BY ${studentId}_mydomains.category
ORDER BY counts DESC;

--
INSERT OVERWRITE LOCAL DIRECTORY './task1d-out/'
     ROW FORMAT DELIMITED
     FIELDS TERMINATED BY '\t'
     STORED AS TEXTFILE
  SELECT * FROM ${studentId}_CategoryTraffic;
