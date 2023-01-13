from datetime import timedelta

from pyspark.sql import functions as F
import json
from _collections import defaultdict
import math
from timeloop import Timeloop
from pyspark.conf import SparkConf
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *
from kafka import KafkaConsumer
#from elasticsearch import Elasticsearch
from subprocess import check_output

topic = "flightsdata"
ETH0_IP = check_output(["hostname"]).decode(encoding="utf-8").strip()

SPARK_MASTER_URL = "local[*]"
SPARK_DRIVER_HOST = ETH0_IP
spark = SparkSession \
.builder \
    .master("local[*]")\
    .appName("test") \
    .getOrCreate()

sc = spark.sparkContext
sc.setLogLevel("ERROR")
spark.conf.set("spark.sql.execution.arrow.enabled", "true")
spark.conf.set("es.nodes", "127.0.0.1")
spark.conf.set("spark.es.port", "9200")
spark.conf.set("es.nodes.wan.only", "true")



def getrows(df, rownums=None):
    return df.rdd.zipWithIndex().filter(lambda x: x[1] in rownums).map(lambda x: x[0])





if __name__ == "__main__":

    df = spark.readStream.format("kafka") \
        .option("kafka.bootstrap.servers", "localhost:9092")\
        .option("subscribe", "flightsdata")\
        .option("enable.auto.commit", "true") \
        .load()



    dff=df.selectExpr('CAST(value AS STRING)')
    def fun_call(df,batch_id):
        req = df.rdd.map(lambda x: x.coord).collect()
        print(req)
    dff.printSchema()
    #query = dff.writeStream.format("console").foreachBatch(fun_call).trigger(processingTime=("30 seconds")).start()
    #query.awaitTermination()
    schema=StructType([StructField("value",StringType(),True)])
    #df1=dff.select(from_json(col('value'),schema))
    #query = df1.writeStream.format("console").foreachBatch(fun_call).trigger(processingTime=("30 seconds")).start()
    #query.awaitTermination()
    #df1.printSchema()
    df2=dff.withColumnRenamed('from_json(value)',"proc")
    #print('df2 schemaa')
    #df2.printSchema()
    #df3=df2.selectExpr("proc.value")
    #query = df3.writeStream.format("console").foreachBatch(fun_call).trigger(processingTime=("30 seconds")).start()
    #query.awaitTermination()
    #df3=df3.withColumnRenamed("value","json")
    print("df4 ============")
    #df3.printSchema()
    #valueschema=ArrayType(StructType([StructField("longitude",FloatType(),True),StructField("latitude",FloatType(),True),StructField("country",StringType(),True),StructField("time",StringType(),True)]))
    valueschema=ArrayType(StructType([StructField("coord",ArrayType(FloatType(),True),True),StructField("country",StringType(),True),StructField("time",StringType(),True)]))
    df4=(df2.withColumn("value",F.from_json("value",valueschema)).selectExpr("inline(value)"))
    df4.printSchema()
    #query = df4.writeStream.format("console").foreachBatch(fun_call).trigger(processingTime=("30 seconds")).start()
    #query.awaitTermination()
    #print("firas")









"""
    print("#############doneee#########")
def stream_batches(df,batch_id):
          req = df.rdd.map(lambda x: x.value).collect()
          print(req )
          df=  df.write.format("org.elasticsearch.spark.sql") \
                .option("es.port", "9200") \
                .option("es.nodes", "127.0.0.1") \
                .mode("append") \
                .save("skyy")


#write_df = df4.writeStream.outputMode("update").foreachBatch(stream_batches).start()
#print('firas')
#write_df.awaitTermination()
#tl.start(block=True)"""

stream_query1=df4 \
    .writeStream \
    .queryName('test') \
    .format("memory") \
    .start()

tl = Timeloop()


@tl.job(interval=timedelta(seconds=15))
def write_to_elasticsearch():

    df4 = spark.sql("""select * from test""")
    df4.write.format("org.elasticsearch.spark.sql") \
    .option("es.port", "9200")\
    .option("es.nodes","127.0.0.1")\
    .mode("append") \
    .save("skyyyy")
    df4.show()
    print("streaming from kafka")
tl.start(block=True)

#write_df.awaitTermination

    #print("PySpark Structured Streaming with Kafka Application Completed....")"""
