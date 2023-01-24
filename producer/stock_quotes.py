import yfinance as yf
import pandas as pd

from datetime import datetime
import os
import logging
import threading


class StockQuotes(threading.Thread):

    # -*- coding: utf-8 -*-
    '''
    Calls yfinance API to get the latest quote for tickers in stocks.csv
    '''

    def __init__(self, stop=True):
        threading.Thread.__init__(self)
        self.stop_event = threading.Event()
        self.market_stop = stop
    
    def stop(self):
        self.stop_event.set()

    def run(self):
        market_stop = self.market_stop
        print(market_stop)
        
        home_directory = '/Users/hyezheechung/Documents/Projects/2022/Kafka/reddit/test/'
        path = home_directory + 'data/'
        stocks_path = path + 'stocks.csv'

        date = datetime.now().strftime('%Y%m%d')
        filepath = path + date + '/'

        if not os.path.isdir(filepath):
            os.mkdir(filepath)

        # stock tickers
        stocks = pd.read_csv(stocks_path)
        stocks_list = stocks['symbol'].to_list()
        stocks_string = ' '.join(stocks_list)

        i = 1

        # latest datetime
        latest_datetime = datetime.now()

        while not self.stop_event.is_set():
            try:
                data = yf.download(
                            tickers = stocks_string,
                            period = '1d',
                            interval = '1m',
                            threads = True
                        )
                
                if self.stop_event.is_set():
                    break

                # slice, transpose and reformat
                close = data.loc[:, pd.IndexSlice[['Close'], :]]
                close.columns = close.columns.droplevel(0)
                tickers = close.columns.tolist()
                close = close.reset_index(level=0, names=['datetime'])
                close = pd.melt(close, id_vars=['datetime'], value_vars=tickers)

                # filter
                close = close.loc[close['datetime'] > latest_datetime]
                close = close.dropna()

                # extract year, month, day, hour, minute, seconds 
                close['year'] = close['datetime'].apply(lambda x: x.year)
                close['month'] = close['datetime'].apply(lambda x: x.month)
                close['day'] = close['datetime'].apply(lambda x: x.day)
                close['hour'] = close['datetime'].apply(lambda x: x.hour)
                close['minute'] = close['datetime'].apply(lambda x: x.minute)
                close['second'] = close['datetime'].apply(lambda x: x.second)

                close.columns = [
                        'datetime',
                        'ticker',
                        'quote',
                        'year',
                        'month',
                        'day',
                        'hour',
                        'minute',
                        'second'
                    ]

                close['ticker'] = close['ticker'].str.lower()

                file = str(i) + '.csv'
                close.to_csv(filepath + file, index=False)

                latest_datetime = max(close['datetime'])
                hour, minute = latest_datetime.hour, latest_datetime.minute

                # default stop at market close
                if market_stop:
                    if (hour == 15 and minute == 59) or (hour == 16 and minute ==0):
                        self.stop()
                
                
                
                i += 1
            
            except Exception as e:
                logging.error('Exception occurred', exc_info=True)
                break



def main():
    date = datetime.now().strftime('%Y%m%d')
    logpath = '/Users/hyezheechung/Documents/Projects/2022/Kafka/reddit/test/logs/' + date + '/'
    
    if not os.path.isdir(logpath):
        os.mkdir(logpath)

    logging.basicConfig(
        filename= logpath + 'stockquotes.log', 
        filemode='w', 
        format='%(name)s - %(levelname)s - %(message)s'
    )
    logging.warning('Starting at %s', date)

    quotes = StockQuotes(False)
    quotes.start()

    
if __name__ == '__main__':
    main()