from __future__ import print_function

import os
import sys
import requests
from operator import add

from pyspark import SparkConf,SparkContext
from pyspark.streaming import StreamingContext

from pyspark.sql import SparkSession
from pyspark.sql import SQLContext

from pyspark.sql.types import *
from pyspark.sql import functions as func
from pyspark.sql.functions import *

# spark = SparkSession.builder.master("local[*]").getOrCreate()
# sc = SparkContext.getOrCreate()
# sqlContext = SQLContext(sc)
#Exception Handling and removing wrong datalines
def isfloat(value):
    try:
        float(value)
        return True
 
    except:
         return False

#Function - Cleaning
#For example, remove lines if they don’t have 16 values and 
# checking if the trip distance and fare amount is a float number
# checking if the trip duration is more than a minute, trip distance is more than 0.1 miles, 
# fare amount and total amount are more than 0.1 dollars
def correctRows(p):
    if(len(p)==17):
        if(isfloat(p[5]) and isfloat(p[11])):
            if(float(p[4])> 60 and float(p[5])>0 and float(p[11])> 0 and float(p[16])> 0):
                return p


#Main
if __name__ == "__main__":
    if len(sys.argv) != 3:
        print("Usage: main_task2 <file> <output> ", file=sys.stderr)
        exit(-1)
    
    
    spark = SparkSession.builder.master("local[*]").getOrCreate()
    sc = SparkContext.getOrCreate()

    # Read the data in Spark DataFrame
    
    #Task 1
    #Your code goes here

    data = spark.read.csv(sys.argv[1])
    testRDD = data.rdd.map(tuple)
    taxilinesCorrected = testRDD.filter(correctRows)



    taxiMap = taxilinesCorrected.map(lambda x: (x[0], x[1], x[2], x[3], x[4], x[5], x[6], x[7], x[8], x[9], x[10], x[11], x[12], x[13], x[14], x[15], x[16]))


    #Task 2
    #Your code goes here
    
    driverAvgMoney = taxiMap.map(lambda idx: (idx[1],(((float(idx[16]))/(float(idx[4]))*60),1))).reduceByKey(lambda x,y: (x[0]+y[0], x[1]+y[1])).map(lambda x: (x[0], x[1][0]/x[1][1]))
    avgTop = driverAvgMoney.top(10, lambda x:x[1])
    topRDD = spark.sparkContext.parallelize(avgTop)
    results_2 = topRDD
    #savings output to argument
    results_2.coalesce(1).saveAsTextFile(sys.argv[2])



    sc.stop()