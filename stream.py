import logging
import time,sys,os
from datetime import datetime 
# caution: path[0] is reserved for script path (or '' in REPL)
sys.path.insert(1, '/Users/hyezheechung/Documents/Projects/2022/Kafka/reddit/test/')
sys.path.insert(2, '/Users/hyezheechung/Documents/Projects/2022/Kafka/reddit/test/producer/')


from kafka import KafkaAdminClient
from kafka.admin import NewTopic

from pyspark.sql import SparkSession
from pyspark.sql.types import StringType, StructType
from pyspark.sql.functions import expr, to_timestamp

# producer
from producer.comments_producer import CommentProducer
from producer.stocks_producer import StockProducer
from producer.stock_quotes import StockQuotes

# consumer
from consumer.comments_consumer import CommentConsumer
from consumer.stocks_consumer import StockConsumer

# cassandra
from tables.db import create_tables



class Stream():

    # -*- coding: utf-8 -*-
    '''
    Stream-stream join between streams from Kafka topic comments and stocks
    saved to Cassandra
    '''
    def __init__(self, spark, topics, stop=True):
        self.spark = spark
        self.threads = None
        self.topics = topics
        self.streams = None
        self.market_stop = stop

    def stop(self):

        threads = self.threads
        streams = self.streams

        for stream in streams:
            stream.stop()
        
        for thread in threads:
            thread.stop()
        
        for thread in threads:
            thread.join()

    
    def start(self):
        topics = self.topics
        spark = self.spark
        stop = self.market_stop

        topic1 = topics[0]
        topic2 = topics[1]

        schema1 = StructType() \
                    .add('id', StringType()) \
                    .add('author', StringType()) \
                    .add('created_est', StringType()) \
                    .add('score', StringType()) \
                    .add('permalink', StringType()) \
                    .add('body', StringType())

        schema2 = StructType() \
                    .add('datetime', StringType()) \
                    .add('ticker', StringType()) \
                    .add('quote', StringType()) \
                    .add('year', StringType()) \
                    .add('month', StringType()) \
                    .add('day', StringType()) \
                    .add('hour', StringType()) \
                    .add('minute', StringType()) \
                    .add('second', StringType())

        home_directory = '/Users/hyezheechung/Documents/Projects/2022/Kafka/reddit/test/'
        checkpoint = home_directory + 'checkpoint/'
        path = home_directory + 'data/'

        # create Kafka topics
        for topic in topics:

            try:
                admin = KafkaAdminClient(
                            bootstrap_servers='localhost:9092'
                        )
                
                topic = NewTopic(
                            name=topic,
                            num_partitions=1,
                            replication_factor=1
                        )

                admin.create_topics([topic])

            except Exception as e:
                logging.error('Exception occurred', exc_info=True)
        

        # producers
        quotes = StockQuotes(stop)
        comments_producer = CommentProducer(topic1)
        stocks_producer = StockProducer(spark, topic2, schema2, checkpoint, path)

        # start producers
        quotes.start()
        comments_producer.start()
        reading_stream = stocks_producer.start()
        writing_stream = stocks_producer.write(reading_stream, checkpoint)


        # consumers
        comments_consumer = CommentConsumer(spark, topic1, schema1)
        stocks_consumer = StockConsumer(spark, topic2, schema2)

        # start consumers
        lines = comments_consumer.start()
        lines = comments_consumer.preprocess(lines)
        lines = comments_consumer.explode_processed_words(lines)
        tickers = stocks_consumer.start()

        # convert to timestamp 
        lines = lines.withColumn('created_est', to_timestamp('created_est'))
        tickers = tickers.withColumn('datetime', to_timestamp('datetime'))

        # stream-stream join
        # watermark
        tickers = tickers.withWatermark('datetime', '2 hours')
        lines = lines.withWatermark('created_est', '1 hours')


        lines = lines.join(
                tickers,
                expr(
                    '''
                    ticker = processed AND
                    datetime <= created_est AND
                    datetime >= created_est + INTERVAL -1 MINUTE
                    '''
                )
        )

        # sentiment
        lines = comments_consumer.sentiment(lines)

        # drop duplicates
        lines = lines \
                .dropDuplicates([
                    'id', 
                    'author',
                    'created_est', 
                    'score', 
                    'permalink', 
                    'body', 
                    'final', 
                    'ticker'
            ])
        
        # create table in cassandra
        keyspace = 'test'
        table = 'stock_sentiment'
        create_tables(keyspace, table)

        query = lines \
            .writeStream \
            .option("checkpointLocation", checkpoint + 'cassandra')\
            .format("org.apache.spark.sql.cassandra")\
            .option("keyspace", keyspace)\
            .option("table", table)\
            .start()
        

        self.threads = [quotes, comments_producer]
        self.streams = [query, writing_stream]




def main():
    date = datetime.now().strftime('%Y%m%d')
    logpath = '/Users/hyezheechung/Documents/Projects/2022/Kafka/reddit/test/logs/' + date + '/'

    if not os.path.isdir(logpath):
        os.mkdir(logpath)

    logging.basicConfig(
        filename= logpath + 'stream.log', 
        filemode='w', 
        format='%(name)s - %(levelname)s - %(message)s'
    )
    logging.warning('Starting at %s', date)


    spark = SparkSession \
                .builder \
                .getOrCreate()

    topics = ['comments', 'stocks']

    stream = Stream(spark, topics)
    stream.start()

    time.sleep(30)

    quote = stream.threads[0]
    
    while not quote.stop_event.is_set():
        continue

    stream.stop()
    spark.stop()

if __name__ == '__main__':
    main()