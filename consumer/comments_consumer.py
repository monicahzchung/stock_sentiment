from pyspark.sql import SparkSession
from pyspark.sql.types import StringType, StructType, FloatType
from pyspark.sql.functions import from_json, regexp_replace, concat_ws, explode, udf, array_distinct
from pyspark.ml.feature import StopWordsRemover, Tokenizer

from nltk.sentiment import SentimentIntensityAnalyzer
from nltk.corpus import stopwords

import logging
from datetime import datetime
import os
import time


class CommentConsumer():

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
        .format("kafka") \
        .option("kafka.bootstrap.servers", "localhost:9092") \
        .option("subscribe", topic) \
        .option('startingOffsets', 'earliest') \
        .load()
    
        df = df.selectExpr("CAST(value AS STRING)")
        df = df.select(from_json(df.value,schema=schema).alias("value"))
        df = df.selectExpr("value.*")

        return df 

    def preprocess(self, stream):
        stop = stopwords.words('english')
        stop.append('')

        # remove handlers 
        lines = stream.withColumn('processed_text', \
                    regexp_replace('body', '@[^\s]+', ''))

        # remove URLs
        lines = lines.withColumn('processed_text', \
                    regexp_replace('processed_text', r'http\S+', ''))

        # remove special characters 
        lines = lines.withColumn('processed_text', \
                    regexp_replace('processed_text', r'\W+', ' '))
        
        # tokenize selftext
        tokenizer = Tokenizer(inputCol= 'processed_text', outputCol='words')
        lines = tokenizer.transform(lines)

        # remove stop words 
        remover = StopWordsRemover(stopWords=stop, inputCol='words',
            outputCol='processed_words')
        lines = remover.transform(lines)

        # concatenate 
        lines = lines.withColumn('final', concat_ws(' ', 'processed_words'))

        # remove duplicates
        lines = lines.withColumn('processed_words', array_distinct('processed_words'))

        return lines

    
    def explode_processed_words(self, lines):
        lines = lines.select(
                        lines.id,
                        lines.author,
                        lines.created_est,
                        lines.score,
                        lines.permalink,
                        lines.body,
                        explode('processed_words').alias('processed'),
                        lines.final)
        return lines

    @staticmethod
    def text_blob_analysis(words):
        # nltk 
        sia = SentimentIntensityAnalyzer()
        return sia.polarity_scores(words)['compound']

    def sentiment(self, lines):
        # sentiment analysis
        compound = udf(self.text_blob_analysis, FloatType())
        lines = lines.withColumn('sentiment', compound('final'))
        lines = lines.drop('processed')

        return lines


def main():
    date = datetime.now().strftime('%Y%m%d')
    logpath = '/Users/hyezheechung/Documents/Projects/2022/Kafka/reddit/test/logs/' + date + '/'

    if not os.path.isdir(logpath):
        os.mkdir(logpath)

    logging.basicConfig(
        filename= logpath + 'comments_consumer.log', 
        filemode='w', 
        format='%(name)s - %(levelname)s - %(message)s'
    )
    logging.warning('Starting at %s', date)

    spark = SparkSession.builder.getOrCreate()

    # consume submission stream and preprocess text
    schema = StructType() \
                .add('id', StringType()) \
                .add('author', StringType()) \
                .add('created_est', StringType()) \
                .add('score', StringType()) \
                .add('permalink', StringType()) \
                .add('body', StringType())
    
    topic_name = 'comments'

    consumer = CommentConsumer(spark, topic_name, schema)

    lines = consumer.start()
    lines = consumer.preprocess(lines)
    lines = consumer.explode_processed_words(lines)

    # join stream
    # table of tickers
    stocks_path = '/Users/hyezheechung/Documents/Projects/2022/Kafka/reddit/test/data/stocks.csv'

    tickers = spark.read \
            .option('header', True) \
            .csv(stocks_path)

    lines = lines.join(tickers, tickers.symbol == lines.processed)
    lines = consumer.sentiment(lines)

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
                'symbol'
        ])

    
    query = lines \
        .writeStream \
        .outputMode("append") \
        .format("console") \
        .start()
    
    time.sleep(2 * 60)

    query.stop()


if __name__ == '__main__':
    main()