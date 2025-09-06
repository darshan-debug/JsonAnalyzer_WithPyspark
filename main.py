# spark-submit --conf spark.driver.extraJavaOptions="-Dlog4j.rootLogger=ERROR" --conf spark.executor.extraJavaOptions="-Dlog4j.rootLogger=ERROR" main.py

import os
import pandas as pd
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, udf,explode,max,pandas_udf,asc,sum
from pyspark.sql.types import *
import json

# Define the UDF output schema
output_schema = ArrayType(StructType([
    StructField("key_path", StringType(), True),
    StructField("max_value_or_len", LongType(), True),
    StructField("key_freq", LongType(), True)
]))

#A Pandas UDF(also called vectorised udf) is a type of PySpark UDF that uses Apache Arrow to transfer data between the Python worker and the JVM (Spark's core). It operates on batches of data, rather than row by row. This vectorization significantly reduces the serialization overhead, leading to much better performance for certain types of operations
@pandas_udf(returnType=output_schema)
def process_payload(payload_series):
    #Parses a Pandas Series of JSON strings and returns a Pandas Series of lists.
    results_list = []
    def process_json_payload(obj,path='.'):
        results=[]
        if(isinstance(obj,dict)):
            for keyy,valuee in obj.items():
                new_path=path+'.'+keyy if path!='.' else keyy
                if(isinstance(valuee,bool)):
                    continue
                elif(isinstance(valuee,(int,float))):
                    results.append((new_path,valuee,1))
                elif(isinstance(valuee,str)):
                    results.append((new_path,len(valuee),1))
                elif(isinstance(valuee,(list,dict))):
                    #results.append((new_path+'(freqAskey)',-1,1))
                    if(isinstance(valuee,list) and len(valuee)>0):
                        results.append((new_path+'(sumEleCount)',-1,len(valuee)))
                    elif(isinstance(valuee,dict) and len(valuee)>0): 
                        results.append((new_path+'(freqAskey)',-1,1))                                   
                    results.extend(process_json_payload(valuee,new_path))
        elif(isinstance(obj,list)):
            for item in obj:
                results.extend(process_json_payload(item,path))
        elif(isinstance(obj,str)):
            results.append((path,len(obj),0))
        elif(isinstance(obj,(int,float))):
            results.append((path,obj,0))
        return results
    for payload in payload_series:
        try:
            json_obj=json.loads(payload)
            #return process_json_payload(json_obj)
            results_list.append(process_json_payload(json_obj))
        except Exception as e:
            print(f"Error processing payload:{payload} \nError:{e}")
            #return []
            results_list.append([])
    return pd.Series(results_list)



def test_process_payload():
    test_payload_array=['{"product_id": 9876.5, "product_name": "Ultra HD 4K Monitor", "is_available": false, "specifications": {"color": "silver", "weight_grams": 4500}, "tags": [{"display": ["4k","1028P"]}, {"gaming":"home_office"}]}',
                        '{"product_id": 2468.0, "product_name": "Portable Bluetooth Speaker", "is_available": true, "specifications": {"color": "blue", "weight_grams": 750}, "tags": ["audio", "bluetooth", "portable", "rechargeable"]}']
    for test_payload in test_payload_array:
        result = process_payload(test_payload)
        print(f"Payload: {test_payload}")
        print("Processed Result:")
        for path, value in result:
            print(f"Path: {path}, Value: {value}")
        print("\n")
if __name__ == "__main__":
    #test_process_payload()
    spark = SparkSession.builder.appName("DataProcessing").getOrCreate()

    spark.sparkContext.setLogLevel("ERROR")
    payload_df=spark.read.format("text").schema('payload string').load("payload.json")
    #process_udf = udf(process_payload, output_schema) #after using decorator:@udf,  this is not needed
    # print(payload_df.count())
    #payload_df.show(truncate=False) #{"product_id": 98765, "product_name": "Ultra HD 4K Monitor", "is_available": false, "specifications": {"color": "silver", "weight_grams": 4500}, "tags": ["display", "4K", "gaming", "home_office"]} 
    #payload_df.printSchema() #payload: string (nullable = true)

    processed_df = payload_df.withColumn("processed", process_payload(col("payload"))).select(col("processed"))
    #processed_df.show(truncate=False)  #[{product_id, 98765}, {product_name, 19}, {specifications.color, 6}, {specifications.weight_grams, 4500}, {tags, 7}, {tags, 2}, {tags, 6}, {tags, 11}]
    exploded_df = processed_df.select(explode(col("processed")).alias("exploded"))
    #exploded_df.show(truncate=False) 
    # |{product_id, 98765}                |
    # |{product_name, 19}                 |
    # |{specifications.color, 6}          |
    exploded_df= exploded_df.repartition(col("exploded.key_path")) #optional, it distributes workload evenly across cluster nodes based on key_path
    #orderBy is optionalin below code, its just to get sorted output
    # key_path and max_value_or_len are coming from "output_schema" ,which is the output schema of process_payload udf
    result_df = exploded_df.groupBy(col("exploded.key_path")).agg(max(col("exploded.max_value_or_len")).alias('max_value_or_len'),sum(col("exploded.key_freq")).alias('key_freq')).orderBy(asc("key_path"))
    
    
    result_df.show(n=result_df.count(),truncate=False)
    # |tags                       |16                            |
    # |product_name               |27                            |
    # |product_id                 |98765                         |
    # |specifications.color       |6                             |
    # |specifications.weight_grams|4500                          |
    # Stop the Spark session
    spark.stop()
    