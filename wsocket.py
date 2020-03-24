import pandas as pd
import pyarrow.parquet as pq
from fastparquet import write
from binance.client import Client
from binance.websockets import BinanceSocketManager
import boto3
import websocket
import s3fs
import json
import datetime
import time

class DataHandler:
    '''
    create a container with the provided data and set the current date
    '''
    def __init__(self, df, pair, exchange):
        self.df = df
        self.exchange = exchange
        self.pair = pair
        self.current_date = self.df['time'].max().date()
    '''
    add a new row to the container. if the new row is the same date as current_date,
    then add it to memory. else write what is in memory and start a new record.
    when written to memory update the current stored date
    '''
    def add(self, df_):
        if self.current_date == df_['time'].max().date():
            self.df = pd.concat([self.df, df_])
            #print("adding\n{}".format(df_))
        else:
            self.write()
            self.reset(df_)
            self.current_date = df_['time'].max().date()
    '''
    write memory to s3
    '''
    def write(self):
        s3 = s3fs.S3FileSystem()
        myopen = s3.open    
        write('s3://historical-data-misc/Raw_Trade_Data/{}/{}/{}.parquet'.format(self.exchange, self.pair, self.current_date), self.df, compression='GZIP', open_with=myopen)
        print('WRITING: {} to {} | s3://historical-data-misc/Raw_Trade_Data/{}/{}/{}.parquet'.format(self.df['time'].min(), self.df['time'].max(), self.exchange, self.pair, self.current_date))
    def get(self):
        return self.df
    '''
    reset memory to a new df
    '''
    def reset(self, df_):
        self.df = df_
    '''
    return the current_date
    '''
    def date(self):
        return self.current_date

class Socket:
    '''
    Set up the data container, create the ws app, and start the websocket running
    '''
    def __init__(self, exchange, pair, latest_day, ws_link, ws_subscription):
        websocket.enableTrace(True)
        self.dh = DataHandler(exchange=exchange, pair=pair, df=latest_day)
        self.ws_subscription = ws_subscription
        self.exchange = exchange
        self.pair=pair
        self.bp = "s3://historical-data-misc/Raw_Trade_Data/{}/{}/".format(exchange, pair)
        ws = websocket.WebSocketApp(ws_link, 
                on_message = lambda ws,message: self.on_message(ws, message),
                on_close = lambda ws: self.on_close(ws),
                on_error = lambda ws,error: self.on_error(ws, error),
                on_open = lambda ws: self.on_open(ws)
        )
        while True:
            try:
                ws.run_forever()
            except:
                pass

    def standardize(self, df): 
        if self.exchange == "Coinbase":
            df['time'] = pd.to_datetime(df['time']).dt.tz_convert(None)
            df['side'] = df.apply(lambda s: 1 if s['side'] == 'buy' else -1, axis=1)
            df = df.rename(columns={'trade_id':'unique_id', 'last_size':'volume'})
            df = df.drop_duplicates()
            df_ = df[['unique_id', 'time', 'volume', 'side', 'price']]
            df_ = df_.set_index('unique_id')
            df_ = df_.sort_values(by='time', ascending=False, axis=1)
            print(df_.head())
            return df_
        elif self.exchange == "Bitfinex":
            df['time'] = df.apply(lambda row: datetime.datetime.utcfromtimestamp(row['time']/1e3), axis=1)
            df['side'] = df.apply(lambda row: -1 if row['volume'] < 0 else 1, axis=1)
            df['volume'] = df.apply(lambda row: row['volume'] if row['volume'] > 0 else row['volume']*-1, axis=1)
            df = df.set_index('unique_id')
            df = df.drop_duplicates()
            df = df.sort_index(axis=0, ascending=True)
            #df = df.sort_values(by='time', ascending=False, axis=1)
            return df
        else:
            return df

    def on_open(self, ws):
        print(self.ws_subscription)
        ws.send(self.ws_subscription)

    def on_error(self, ws, error):
        print(error)
    
    def on_close(self, ws):
       time.sleep(5)
       self.dh.write() 
    
    def on_message(self, ws, message):
        if (self.exchange == "Coinbase" and "subscriptions" not in message): 
            df = pd.DataFrame([json.loads(message)], columns=['unqiue_id', 'time', 'price', 'volume', 'side'], index=[0])
            df = self.standardize(df)
            self.dh.add(df)
            print("{} {} to {} in memory".format(self.pair, self.dh.get()['time'].min(), self.dh.get()['time'].max()))
        elif (self.exchange == "Bitfinex"):
            data = json.loads(message)
            if ("event" not in message and len(data) > 2): 
                if ("te" in data[1]):
                    df = pd.DataFrame([data[2]], columns=['unique_id', 'time', 'volume', 'price'], index=[0])
                    df = self.standardize(df)
                    self.dh.add(df)
                    print("{} {} to {} in memory".format(self.pair, self.dh.get()['time'].min(), self.dh.get()['time'].max()))

class BinanceSocket:

    def standardize(self, df):
        df = df.rename(columns={'T':'time', 't':'unique_id', 'p':'price', 'q':'volume', 'm':'side'})
        df = df.sort_values(by='unique_id', ascending=False)
        df = df.set_index('unique_id')
        df = df[['time', 'price', 'volume', 'side']]
        df = df.sort_index(axis=1, ascending=False)
        df = df.drop_duplicates()
        df['time'] = df['time'].apply(lambda x: datetime.datetime.utcfromtimestamp(x/1e3))
        df['side'] = df.apply(lambda row: 1 if row['volume'] > 0 else -1, axis=1)
        return df

    def on_message(self, msg):
        df = pd.DataFrame(msg, index=[0])
        df = self.standardize(df)
        self.dh.add(df)
        print("range: {} to {}".format(self.dh.get()['time'].min(), self.dh.get()['time'].max()))

    def __init__(self, df, pair, client):
       self.dh = DataHandler(df, pair, "Binance")
       self.socket_manager = BinanceSocketManager(client)
       trade_socket = self.socket_manager.start_trade_socket(pair, self.on_message)
       self.socket_manager.start()

