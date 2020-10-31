// CSE3BDC/CSE5BDC Lab 05 - Processing Big Data with Spark
/*******************************************************************************
 * Exercise 1
 * Write code that first finds the square root of each number in an RDD and then
 * sums all the square roots together.
 */
// TODO: Write your code here
val someNumbers = sc.parallelize(1 to 1000)
val result = someNumbers.map(x=>math.sqrt(x)).reduce(_+_)

// TODO: Copy and paste the result here

result: Double = 21097.45588748074



/*******************************************************************************
 * Exercise 2
 * Sum up the total salary for each occupation and then report the output in
 * ascending order according to occupation - in *one line* of code.
 */
val people = sc.parallelize(Array(("Jane", "student", 1000),
                                  ("Peter", "doctor", 100000),
                                  ("Mary", "doctor", 200000),
                                  ("Michael", "student", 1000)))
val result = people.map {
     | case (name,occupation,salary) => ((occupation), (salary)) 
     | }.reduceByKey(_+_) .sortBy(x=> x._1, true)
result.collect
// TODO: Copy and paste the result here
res26: Array[(String, Int)] = Array((doctor,300000), (student,2000))



/*******************************************************************************
 * Exercise 3
 * Use the map method to create a new pair RDD where the key is "native-country"
 * and the value is age. Use the filter function to remove records with missing
 * data.
 */
// TODO: Write your code here
val countryAge = censusSplit.map( x=> (x(13),x(0).toInt)).filter(x=> x._1 !="?")
println(countryAge.count()) // Does this equal 31978?
31978
println(countryAge)
// TODO: Copy and paste the result here
MapPartitionsRDD[21] at filter at <console>:28
Array[(String, Int)] = Array((United-States,39), (United-States,50), (United-States,38), (United-States,53), (Cuba,28), (United-States,37), (Jamaica,49)
, (United-States,52), (United-States,31), (United-States,42), (United-States,37), (India,30), (United-States,23), (United-States,32), (Mexico,34), (United-Stat
es,25), (United-States,32), (United-States,38), (United-States,43), (United-States,40), (United-States,54), (United-States,35), (United-States,43), (United-Sta
tes,59), (United-States,56), (United-States,19), (South,54), (United-States,39), (United-States,49), (United-States,23), (United-States,20), (United-States,45)
, (United-States,30), (United-States,22), (Puerto-Rico,48), (United-States,21), (United-States,19), (United-States,48), (United-States,31), (United-States,53),
 (...


/*******************************************************************************
 * Exercise 4
 * Find the age of the oldest person from each country so the resulting RDD
 * should contain one (country, age) pair per country. 
 */
// TODO: Write your code here
val oldestPerCountry = countryAge.distinct().reduceByKey( math.max(_,_))
// TODO
// TODO: Copy and paste the result here
Array[(String, Int)] = Array((Hungary,81), (Portugal,78), (United-States,90), (Canada,80), (Jamaica,66), (Japan,61), (Honduras,58), (Hong,60), (Peru,69)
, (Cambodia,65), (El-Salvador,79), (Vietnam,73), (Iran,63), (Columbia,75), (Taiwan,61), (Scotland,62), (Yugoslavia,66), (Poland,85), (Outlying-US(Guam-USVI-etc
),63), (South,90), (Mexico,81), (Ecuador,90), (Laos,56), (Nicaragua,67), (China,75), (Italy,77), (Greece,65), (France,64), (Germany,74), (Puerto-Rico,90), (Hol
and-Netherlands,32), (Cuba,82), (Haiti,63), (Ireland,68), (Dominican-Republic,78), (Philippines,90), (Guatemala,66), (Thailand,55), (Trinadad&Tobago,61), (Indi
a,61), (England,90))



/*******************************************************************************
 * Exercise 5
 * Output the top 7 countries in terms of having the oldest person. The output
 * should again be the country followed by the age of the oldest person.
 */
// TODO: Write your code here
val top7OldestPerCountry =  oldestPerCountry.sortBy(x=> x._2, false).take(7)
// TODO: Copy and paste the result here

  Array[(String, Int)] = Array((United-States,90), (South,90), (Ecuador,90), (Puerto-Rico,90), (Philippines,90), (England,90), (Poland,85))



/*******************************************************************************
 * Exercise 6
 * Output the top 7 countries in terms of having the oldest person. The output
 * should again be the country followed by the age of the oldest person.
 */
// TODO: Write your code here
val allPeople = censusSplit.map( x=> (x(13),x(3),x(6),x(9)))
allPeople.take(5).foreach(println)
// TODO: Copy and paste the result here
  (United-States,Bachelors,Adm-clerical,Male)
(United-States,Bachelors,Exec-managerial,Male)
(United-States,HS-grad,Handlers-cleaners,Male)
(United-States,11th,Handlers-cleaners,Male)
(Cuba,Bachelors,Prof-specialty,Female)

val filteredPeople = allPeople.filter ( x=> x._3 !="?") // TODO: Step 2
filteredPeople.count // Does this equal 30718?
res5: Long = 30718

val canadians = filteredPeople.filter( x=> x._1=="Canada")
val americans = filteredPeople.filter( x=> x._1 =="United-States")
canadians.count // Does this equal 107?
  res7: Long = 107
americans.count // Does this equal 27504?
  res6: Long = 27504

val repCandidates = canadians.map { 
     | case ( a,b,c,d ) => ( (c), (a,b,d) ) }. join (
     | americans.map { 
     | case ( a,b,c,d ) => ( (c), (a,b,d) ) }).values
repCandidates.count // Does this equal 325711?
  res32: Long = 325711

val includingDoctorate = repCandidates.filter( x => x._1._2 =="Doctorate" || x._2._2=="Doctorate")// TODO: Step 5
repCandidates.count // Does this equal 31110?
// TODO: Copy and paste the result here
 res9: Long = 31110
