set hivevar:studentId=20105729; --Please replace it with your student id 
DROP TABLE ${studentId}_ipcountries;
DROP TABLE ${studentId}_facebooktrafficcountries;
DROP TABLE ${studentId}_facebookregioncounts;
-- Don't forget to drop the tables you create here


-- Task 1E step 2
-- Create a table with two columns - a list of ip addresses with
-- their associated region names.
CREATE TABLE ${studentId}_ipcountries AS
SELECT ${studentId}_myips.ipAddress AS IP, ${studentId}_myregions.regionName AS RN
FROM ${studentId}_myips CROSS JOIN ${studentId}_myregions
WHERE ${studentId}_myips.intAddress >= ${studentId}_myregions.intMin AND
${studentId}_myips.intAddress <= ${studentId}_myregions.intMax ;


-- Task 1E step 3
-- Create a table with two columns - the name of a region for each hit to Facebook
-- between the two given dates, and the time of that visit.
CREATE TABLE ${studentId}_facebooktrafficcountries AS
SELECT ${studentId}_ipcountries.RN AS region , ${studentId}_facebooktraffic.time AS times
FROM ${studentId}_ipcountries INNER JOIN ${studentId}_facebooktraffic
ON (${studentId}_ipcountries.IP = ${studentId}_facebooktraffic.ipAddress);

-- Task 1E step 4
-- Create a table which contains the number of visits to Facebook from 
-- each country between the given dates.
CREATE TABLE ${studentId}_facebookregioncounts AS
SELECT ${studentId}_facebooktrafficcountries.region, count(1) AS Counts 
FROM ${studentId}_facebooktrafficcountries
GROUP BY ${studentId}_facebooktrafficcountries.region
ORDER BY Counts DESC ;

-- Task 1E step 5
-- Write the contents of the table created in step 4 to the directory './task1e-out/'
INSERT OVERWRITE LOCAL DIRECTORY './task1e-out/'
     ROW FORMAT DELIMITED
     FIELDS TERMINATED BY '\t'
     STORED AS TEXTFILE
  SELECT * FROM ${studentId}_facebookregioncounts;
--SELECT 20105729_ipcountries.RN , 20105729_facebooktraffic.time
--FROM 20105729_ipcountries INNER JOIN 20105729_facebooktraffic
--ON (20105729_ipcountries.IP = 20105729_facebooktraffic.ipAddress)
--limit 10;
--SELECT count(1) from 20105729_facebooktrafficcountries AS Counts
--GROUP BY 20105729_facebooktrafficcountries.region
--ORDER BY Counts DESC ;
