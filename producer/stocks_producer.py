from kafka import KafkaAdminClient
from kafka.admin import NewTopic

from pyspark.sql import SparkSession
from pyspark.sql.types import StringType, StructType
from pyspark.sql.functions import struct, col, to_json


from stock_quotes import StockQuotes


from datetime import datetime
import os
import logging


class StockProducer():

    # -*- coding: utf-8 -*-
    '''
    Instantiates Kafka topic stocks to stream quotes for tickers 
    '''

    def __init__(self, spark, topic, schema, checkpoint, path):
        self.topic = topic
        self.spark = spark
        self.schema = schema
        self.event = True
        self.checkpoint = checkpoint + topic
        self.path = path

        if not os.path.isdir(checkpoint + topic):
            os.mkdir(checkpoint + topic)
    
    def stop(self):
        self.event = False
    
    def start(self):
        spark = self.spark
        topic = self.topic
        schema = self.schema
        path = self.path

        date = datetime.now().strftime('%Y%m%d')
        filepath = path + date + '/'

        df = spark \
            .readStream \
            .option('header', True) \
            .csv(filepath, schema=schema)


        df = df \
            .withColumn('value',
                struct(
                    col('datetime'),
                    col('ticker'),
                    col('quote'),
                    col('year'),
                    col('month'),
                    col('day'),
                    col('hour'),
                    col('minute'),
                    col('second')
                )
            ).drop(
                'datetime', 
                'ticker', 
                'quote', 
                'year', 
                'month', 
                'day', 
                'hour', 
                'minute', 
                'second'
            )

        df = df.select(to_json(col('value')).alias("value"))

        return df

    def write(self, stream, checkpoint):
        topic = self.topic
        checkpoint = self.checkpoint

        # kafka
        ds = stream \
            .writeStream \
            .format('kafka') \
            .option('kafka.bootstrap.servers', 'localhost:9092') \
            .option('topic', topic) \
            .option('checkpointLocation', checkpoint) \
            .start()
        
        return ds


def main():
    # create log file
    date = datetime.now().strftime('%Y%m%d')
    logpath = '/Users/hyezheechung/Documents/Projects/2022/Kafka/reddit/test/logs/' + date + '/'

    if not os.path.isdir(logpath):
        os.mkdir(logpath)

    logging.basicConfig(
        filename= logpath + 'stocks_producer.log', 
        filemode='w', 
        format='%(name)s - %(levelname)s - %(message)s'
    )
    logging.warning('Starting at %s', date)

    
    home_directory = '/Users/hyezheechung/Documents/Projects/2022/Kafka/reddit/test/'
    checkpoint = home_directory + 'checkpoint/'
    path = home_directory + 'data/'

    topic_name = 'stocks'

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

   
    try:
         # create kafka topic
        admin = KafkaAdminClient(bootstrap_servers='localhost:9092')
        
        topic = NewTopic(name=topic_name,
                        num_partitions=1,
                        replication_factor=1)

        admin.create_topics([topic])

    except Exception as e:
        logging.error('Exception occurred', exc_info=True)



    # start spark session
    spark = SparkSession \
        .builder \
        .getOrCreate()

    producer = StockProducer(spark, topic_name, schema, checkpoint, path)

    # start downloading from yfinance
    stockquotes = StockQuotes()
    stockquotes.start()

    # reading stream
    reading_stream = producer.start()

    # writing stream 
    writing_stream = producer.write(reading_stream, checkpoint)

    while not stockquotes.stop_event.is_set():
        continue

    writing_stream.stop()

    # stop spark session
    spark.stop()
    
    


if __name__ == '__main__':
    main()