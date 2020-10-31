set hivevar:studentId=12345678; --Please replace it with your student id 
DROP TABLE ${studentId}_myinput;
DROP TABLE ${studentId}_mywords;

CREATE TABLE ${studentId}_myinput (line STRING);

-- Load the text from the local filesystem
LOAD DATA LOCAL INPATH './Input_data/2/'
  INTO TABLE ${studentId}_myinput;

-- Table containing all the words in the myinput table
-- The difference between this table and myinput is that myinput stores each line as a separate row
-- whereas mywords stores each word as a separate row.
CREATE TABLE ${studentId}_mywords AS
SELECT EXPLODE(SPLIT(LCASE(REGEXP_REPLACE(line,'[\\p{Punct},\\p{Cntrl}]','')),' ')) AS word
FROM ${studentId}_myinput;

-- Dump the output to file
INSERT OVERWRITE LOCAL DIRECTORY './task4-out/'
ROW FORMAT DELIMITED
  FIELDS TERMINATED BY '\t'
STORED AS TEXTFILE
  SELECT * FROM ${studentId}_mywords;
