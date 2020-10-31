set hivevar:studentId=20105729; --Please replace it with your student id 
DROP TABLE ${studentId}_1b;

CREATE TABLE ${studentId}_1b AS
SELECT url,count(url) AS num FROM ${studentId}_mytraffic
GROUP BY url
ORDER BY num DESC ;
--
INSERT OVERWRITE LOCAL DIRECTORY './task1b-out/'
     ROW FORMAT DELIMITED
     FIELDS TERMINATED BY '\t'
     STORED AS TEXTFILE
  SELECT * FROM ${studentId}_1b;
