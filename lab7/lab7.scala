/*
    TODO: [Exercise 1]
    Write the three different queries mentioned in step 7 and exercise 1.
    Below each query write the amount of data read in to complete that query, then
    answer the question at the bottom.
*/
// When the date is EQUAL to "2017-11":
807.0 B

// When the date is BEFORE "2017-11":
3.2 KB


// When the date is AFTER "2017-11":
809.0 B

// Is Spark able to optimise the reading partitioned data when the filtering is
// not using the equality operator?

ANSWER: YES, it is able to optimize. 


/*
    TODO: [Exercise 2]
    After trying different values for the tree depth argument of the treeReduce
    function, compare the number of stages used in the Spark web interface and
    make a brief comment about your findings.
*/
ANSWER: The depth level 3, we can see more treereduce operation getting added and shuffling of data of 1206.0 as compared with the depth level 2 which is approximtely similiar to mentioning no depth. 


/*
    TODO: [Exercise 3]
    After changing the order of the filtering and sorting, compare the input
    and shuffle data sizes for the RDD query. Make a note of them below.
*/
// Ordering then filtering:
Input Data-197.4 KB / Shuffle Data-19.9KB


// Filtering then ordering:
Input Data-197.4 KB / Shuffle Data-9.6KB

