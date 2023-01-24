from cassandra.cluster import Cluster

import logging 
from datetime import datetime

def create_tables(keyspace, table):

     # -*- coding: utf-8 -*-
    '''
    Create table in Cassandra
    '''

    cluster = Cluster()
    session = cluster.connect(keyspace)

    try:
        session.execute(
            '''
            CREATE TABLE %s(
                id text,
                author text,
                created_est timestamp,
                score int,
                permalink text,
                body text,
                final text,
                datetime timestamp,
                ticker text,
                quote float,
                year int,
                month int,
                day int,
                hour int,
                minute int,
                second int,
                sentiment float,
                PRIMARY KEY ((ticker), year, month, day, hour)
            );
            ''' % table
        )
    
    except Exception as e:
        logging.error('Exception occurred', exc_info=True)



def drop_tables(keyspace, table):

     # -*- coding: utf-8 -*-
    '''
    Create table in Cassandra
    '''

    cluster = Cluster()
    session = cluster.connect(keyspace)

    try:
        session.execute(
            '''
            DROP TABLE %s;
            ''' % table
        )
    
    except Exception as e:
        logging.error('Exception occurred', exc_info=True)



def main():
    date = datetime.now().strftime('%Y%m%d')
    logpath = '/Users/hyezheechung/Documents/Projects/2022/Kafka/reddit/test/logs/' + date + '/'

    logging.basicConfig(
        filename= logpath + 'tables.log', 
        filemode='w', 
        format='%(name)s - %(levelname)s - %(message)s'
    )
    logging.warning('Starting at %s', date)

    create_tables()


if __name__ == '__main__':
    main()
