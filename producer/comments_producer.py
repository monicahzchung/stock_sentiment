from kafka import KafkaProducer, KafkaAdminClient
from kafka.admin import NewTopic

import praw
from redditsecret import client, secret_key, agent

from datetime import datetime
import pytz
import logging
import threading
import time
from json import dumps
import os


class CommentProducer(threading.Thread):

    # -*- coding: utf-8 -*-
    '''
    Instantiates Kafka topic comments from wallstreetbets for stock sentiment
    '''

    def __init__(self, topic):
        threading.Thread.__init__(self)
        self.stop_event = threading.Event()
        self.topic = topic
    
    def stop(self):
        self.stop_event.set()
    
    def run(self):
        reddit = praw.Reddit(
                client_id=client,
                client_secret=secret_key,
                user_agent=agent
        )

        producer = KafkaProducer(bootstrap_servers='localhost:9092',
            key_serializer=lambda x: x.encode('utf-8'),
            value_serializer=lambda x: dumps(x).encode('utf-8'))

        subreddit = reddit.subreddit('wallstreetbets')

        est = pytz.timezone('US/Eastern')
        utc = pytz.utc
        format = '%Y-%m-%d %H:%M:%S'

        start_datetime = datetime.now(est).strftime(format)


        while not self.stop_event.is_set():
            for comment in subreddit.stream.comments(skip_existing=True):

                comment_datetime_utc = datetime.fromtimestamp(comment.created_utc, utc)
                comment_datetime_est = comment_datetime_utc.astimezone(est).strftime(format)
                
                if start_datetime <= comment_datetime_est:

                    key = comment.id

                    value = {
                        'id': key,
                        'author': comment.author.id,
                        'created_est':comment_datetime_est,
                        'score': comment.score,
                        'permalink': comment.permalink,
                        'body': comment.body
                    }

                    producer.send(self.topic, key=key, value=value)

                if self.stop_event.is_set():
                    break
        
        producer.close()



def main():
    date = datetime.now().strftime('%Y%m%d')
    logpath = '/Users/hyezheechung/Documents/Projects/2022/Kafka/reddit/test/logs/' + date + '/'

    if not os.path.isdir(logpath):
        os.mkdir(logpath)

    logging.basicConfig(
        filename= logpath + 'comments_producer.log', 
        filemode='w', 
        format='%(name)s - %(levelname)s - %(message)s'
    )
    logging.warning('Starting at %s', date)

    topic_name = 'comments'
    
    task = CommentProducer(topic_name)

    try:
        admin = KafkaAdminClient(bootstrap_servers='localhost:9092')
        
        topic = NewTopic(name=topic_name,
                        num_partitions=1,
                        replication_factor=1)

        admin.create_topics([topic])

    except Exception as e:
        logging.error('Exception occurred', exc_info=True)


    task.start()
    
    time.sleep(2 * 60)

    task.stop()



if __name__ == '__main__':
    main()