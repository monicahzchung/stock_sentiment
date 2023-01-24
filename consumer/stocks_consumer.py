from pyspark.sql import SparkSession
from pyspark.sql.types import StringType, StructType
from pyspark.sql.functions import from_json

import logging
from datetime import datetime
import os
import time


class StockConsumer():

    # -*- coding: utf-8 -*-
    '''
    Subscribes to Kafka topic comments to preprocess and analyze sentiment
    '''

    def __init__(self, spark, topic, schema):
        self.spark = spark
        self.topic = topic
        self.schema = schema

    def start(self):
        spark, topic, schema = self.spark, self.topic, self.schema

        df = spark \
            .readStream \
            .format('kafka') \
            .option('kafka.bootstrap.servers', 'localhost:9092') \
            .option('subscribe', topic) \
            .option('startingOffsets', 'earliest') \
            .load()
        
        df = df.selectExpr("CAST(value AS STRING)")
        df = df.select(from_json(df.value,schema=schema).alias("value"))
        df = df.selectExpr("value.*")

        return df 



def main():
    date = datetime.now().strftime('%Y%m%d')
    logpath = '/Users/hyezheechung/Documents/Projects/2022/Kafka/reddit/test/logs/' + date + '/'

    if not os.path.isdir(logpath):
        os.mkdir(logpath)

    logging.basicConfig(
        filename= logpath + 'stocks_consumer.log', 
        filemode='w', 
        format='%(name)s - %(levelname)s - %(message)s'
    )
    logging.warning('Starting at %s', date)

    spark = SparkSession.builder.getOrCreate()

    # consume stocks batch
    schema = StructType() \
            .add('datetime', StringType()) \
            .add('ticker', StringType()) \
            .add('quote', StringType()) \
            .add('year', StringType()) \
            .add('month', StringType()) \
            .add('day', StringType()) \
            .add('hour', StringType()) \
            .add('minute', StringType()) \
            .add('second', StringType())
    
    topic_name = 'stocks'

    consumer = StockConsumer(spark, topic_name, schema)
    stocks = consumer.start()

    # stream 
    query = stocks \
        .writeStream \
        .outputMode("append") \
        .format("console") \
        .start()
    
    time.sleep(2 * 60)

    query.stop()



if __name__ == '__main__':
    main()