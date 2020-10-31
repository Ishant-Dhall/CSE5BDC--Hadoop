set hivevar:studentId=12345678; --Please replace it with your student id 

DROP TABLE ${studentId}_mydomains;
CREATE TABLE ${studentId}_mydomains (url STRING, category STRING)
ROW FORMAT DELIMITED FIELDS TERMINATED BY ',';
LOAD DATA LOCAL INPATH './Input_data/1/domains.csv'
INTO TABLE ${studentId}_mydomains;

-- TODO: [Task 1A] Load the other data files into tables
