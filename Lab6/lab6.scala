// CSE3BDC/CSE5BDC Lab 06 - Spark DataFrames and Datasets
/*******************************************************************************
 * Exercise 1
 * Write a case class called Fruit which corresponds to the data in
 * Input_data/TaskA/fruit.csv
 */
// TODO: Write your code here
case class Fruits(name:String, price:Double, stock:Int)


/*******************************************************************************
 * Exercise 2
 * Convert fridgeParquetDF into a Dataset and show the first 10 rows.
 */
// TODO: Write your code here
val fridgeParquetDS = fridgeParquetDF.as[Fridge]
fridgeParquetDS.show(10)



/*******************************************************************************
 * Exercise 3
 * Using DataFrames, find the full name of every country in Oceania (continent “OC”).
 * Show the first 10 country names in ascending alphabetical order.
 */
// TODO: Write your code here
val continentDF = spark.read.json("/user/ashhall1616/bdc_data/lab_6/continent.json")
val namesDF=spark.read.json("/user/ashhall1616/bdc_data/lab_6/names.json")
continentDF.filter($"continent"==="OC").select($"CountryCode").join(namesDF,"CountryCode").select($"name").orderBy($"name").show(10)

// OUTPUT
+----------------+
|            name|
+----------------+
|  American Samoa|
|       Australia|
|    Cook Islands|
|      East Timor|
|            Fiji|
|French Polynesia|
|            Guam|
|        Kiribati|
|Marshall Islands|
|      Micronesia|
+----------------+
only showing top 10 rows


/*******************************************************************************
 * Exercise 4
 * Calculate the average refrigerator efficiency for each brand. Order the results
 * in descending order of average efficiency and show the first 5 rows.
 */
// TODO: Write your code here
scala> fridgeDF.groupBy($"brand").agg(avg("efficiency").as("AVG")).orderBy($"AVG".desc).show(5)

//Output
+--------------------+------------------+
|               brand|               AVG|
+--------------------+------------------+
|Minus Forty Techn...|     34.5190774644|
|             INFRICO|24.447894024066667|
|  Jono Refrigeration|     22.2967678746|
|              Imbera| 21.74823766813333|
|  TRUE MANUFACTURING|20.504535147400002|
+--------------------+------------------+
only showing top 5 rows


/*******************************************************************************
 * Exercise 5
 * Redo Exercise 3 using spark.sql instead of the DataFrames API.
 */
// TODO: Write your code here
spark.sql(""" SELECT name FROM names INNER JOIN continents ON names.CountryCode==continents.CountryCode
     | where continent=="OC" ORDER BY name """).show(10)

//OUTPUT
+----------------+
|            name|
+----------------+
|  American Samoa|
|       Australia|
|    Cook Islands|
|      East Timor|
|            Fiji|
|French Polynesia|
|            Guam|
|        Kiribati|
|Marshall Islands|
|      Micronesia|
+----------------+
only showing top 10 rows



/*******************************************************************************
 * Exercise 6
 * Using toPercentageUdf, add a new column to fractionDF called “percentage”
 * containing the fraction as a formatted percentage string and drop the original
 * fraction column.
 */
// TODO: Write your code here
val percentageDF= fractionDF.drop($"fraction").withColumn("Percentage", toPercentageUdf($"count"/continentDF.count) )
percentageDF.show()

//Another Way
val percentageDF= fractionDF.withColumn("Percentage", toPercentageUdf($"fraction") ).drop($"count")

// TODO: Copy and paste the result here
scala> percentageDF.show()
+---------+--------+----------+
|continent|fraction|Percentage|
+---------+--------+----------+
|       NA|   0.164|    16.40%|
|       SA|   0.056|     5.60%|
|       AS|   0.208|    20.80%|
|       AN|    0.02|     2.00%|
|       OC|   0.108|    10.80%|
|       EU|   0.212|    21.20%|
|       AF|   0.232|    23.20%|
+---------+--------+----------+
