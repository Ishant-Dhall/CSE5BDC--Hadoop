set hivevar:studentId=12345678; --Please replace it with your student id 

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

-- Create a table with the words cleaned and counted
CREATE TABLE ${studentId}_wordcount AS
SELECT word, count(1) AS count
FROM ${studentId}_mywords
WHERE word NOT LIKE ""
GROUP BY word;

-- Dump the output to file
INSERT OVERWRITE LOCAL DIRECTORY './task2-out/'
  SELECT * FROM ${studentId}_wordcount;
