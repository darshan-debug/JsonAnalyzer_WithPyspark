#spark-submit --conf spark.driver.extraJavaOptions="-Dlog4j.rootLogger=ERROR" --conf spark.executor.extraJavaOptions="-Dlog4j.rootLogger=ERROR" main.py

import os
import pandas as pd
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, udf,explode
from pyspark.sql.types import *
import json

def process_json_payload(obj,path='.'):
    results=[]
    if(isinstance(obj,dict)):
        for keyy,valuee in obj.items():
            new_path=path+'.'+keyy if path!='.' else keyy
            if(isinstance(valuee,bool)):
                continue
            elif(isinstance(valuee,(int,float))):
                results.append((new_path,valuee))
            elif(isinstance(valuee,str)):
                results.append((new_path,len(valuee)))
            elif(isinstance(valuee,(list,dict))):
                results.extend(process_json_payload(valuee,new_path))
    elif(isinstance(obj,list)):
        for item in obj:
            results.extend(process_json_payload(item,path))
    elif(isinstance(obj,str)):
        results.append((path,len(obj)))
    elif(isinstance(obj,(int,float))):
        results.append((path,obj))
    return results

# Define the UDF output schema
output_schema = ArrayType(StructType([
    StructField("path", StringType(), True),
    StructField("value", DoubleType(), True)
]))

if __name__ == "__main__":
    spark = SparkSession.builder.appName("DataProcessing").getOrCreate()

    spark.sparkContext.setLogLevel("ERROR")
    payload_df=spark.read.format("text").load("payload.json")
    process_udf = udf(process_json_payload, output_schema)
    # print(payload_df.count())
    payload_df.show(truncate=False)
    payload_df.printSchema()

    #processed_df = payload_df.withColumn("processed", process_udf(col("value")))

    # Stop the Spark session
    spark.stop()
