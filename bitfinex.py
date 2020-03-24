import pandas as pd
import pyarrow.parquet as pq                    
from fastparquet import write
from wsocket import Socket
import boto3
import websocket
import s3fs
import requests
import json
import datetime
import time

s3 = s3fs.S3FileSystem()

def standardize(df):
    df['time'] = df.apply(lambda row: datetime.datetime.utcfromtimestamp(row['time']/1e3), axis=1)
    df['side'] = df.apply(lambda row: -1 if row['volume'] < 0 else 1, axis=1)
    df['volume'] = df.apply(lambda row: row['volume'] if row['volume'] > 0 else row['volume']*-1, axis=1)
    df = df.set_index('unique_id')
    df = df.drop_duplicates()
    df = df.sort_values(by='time', ascending=False)
    return df

def request(start, pair):
    base = "https://api.bitfinex.com/v2/trades/t{}/hist".format(pair)
    if start != 0:
        url = "{}?end={}".format(base, start)
    else:
        url = base
    while True:
        try:
            response = requests.get(url)
            if "ratelimit" in response.text or response == None or "DOCTYPE" in response.text:
                print("ratelimited. waiting one minute")
                raise Exception
            else:
                return response.text
        except Exception as e:
            time.sleep(60)

def create_frame(start, pair):
    t = request(start, pair)
    while True:
        try:
            df = pd.DataFrame(json.loads(t), columns=['unique_id', 'time', 'volume', 'price'])
            return df
        except Exception as e:
            print(e)
            time.sleep(10)
            t = request(start, pair)




def split(df):
    dates = []
    frames = []
    df_ = df
    df_['date'] = df_.apply(lambda row: row['time'].date(), axis=1)
    for key, val in df_.iterrows():
        if val['time'].date() not in dates:
            dates.append(val['time'].date())
    for date in dates:
        d = df_.loc[df_['date'] == date]
        d = d.drop('date', axis=1)
        frames.append(d)
    return frames

def write_s3(df, path):
    myopen = s3.open
    write('{}'.format(path), df, compression='GZIP', open_with=myopen)

def create_path(pair, date):
    return "s3://historical-data-misc/Raw_Trade_Data/Bitfinex/{}/{}.parquet".format(pair, date)

'''
Fix the index being set to default for some of the frames
'''
def fix_index(pair):
   s3_ = boto3.resource('s3')
   bucket = s3_.Bucket('historical-data-misc')
   for item in bucket.objects.all():
       if "Bitfinex/{}".format(pair) in item.key:
           try:
               df = pq.ParquetDataset('s3://historical-data-misc/{}'.format(item.key), filesystem=s3).read_pandas().to_pandas()
               if df.index.name != 'unique_id':
                   print("fixing {}".format(item.key))
                   df = df.set_index('unique_id')
                   write_s3(df, create_path(pair, df['time'].max().date()))
           except Exception as e:
                print(e)

def date_mismatches(pair):
   s3_ = boto3.resource('s3')
   bucket = s3_.Bucket('historical-data-misc')
   with open("bitfinex_report_{}".format(pair), "w") as f:
       for item in bucket.objects.all():
           if "Bitfinex/{}".format(pair) in item.key:
               try:
                   df = pq.ParquetDataset('s3://historical-data-misc/{}'.format(item.key), filesystem=s3).read_pandas().to_pandas()
                   if df['time'].min().date() != df['time'].max().date():
                       print("found mismatch on {}".format(item.key))
                       df['date'] = df.apply(lambda row: row['time'].date(), axis=1)
                       d = df['date'].max()
                       df = df.loc[df['date'] == d]
                       df = df[['time', 'price', 'volume', 'side']]
                       write_s3(df, create_path(item.key, d))
               except Exception as e:
                   print(e)
'''
Find issues in the stored parquet files for a given pair
'''
def time_gaps(pair, lim):
   s3_ = boto3.resource('s3')
   bucket = s3_.Bucket('historical-data-misc')
   error = False
   with open("bitfinex_report_{}".format(pair), "w") as f:
       for item in bucket.objects.all():
           if "Bitfinex/{}".format(pair) in item.key:
               try:
                   df = pq.ParquetDataset('s3://historical-data-misc/{}'.format(item.key), filesystem=s3).read_pandas().to_pandas()
                   mn = df['time'].min().hour
                   mx = df['time'].max().hour
                   res = "{}: ".format(item.key)
                   if df['time'].min().date() != df['time'].max().date():
                       res = "{} mismatched dates".format(res)
                       error = True
                   if mn > lim:
                       res = "{} min: {}".format(res, mn)
                       error = True
                   else:
                       res = "{} min: ok".format(res)
                   if mx < 24 - lim:
                       res = "{} max: {}".format(res, mx)
                       error = True
                   else:
                       res = "{} max: ok".format(res)
                   if error:
                       print(res)
                       f.write(res)
                       f.write("\n")
                       error = False
               except Exception as e:
                   print("couldn't load {}".format(item.key))


def get_last_day(pair):
    s3_ = boto3.resource('s3')
    bucket = s3_.Bucket('historical-data-misc')
    days = []
    for item in bucket.objects.all():
        if "Bitfinex/{}".format(pair) in item.key:
            days.append(item.key)
    return days[-1]

def update(pair):
    day = 's3://historical-data-misc/{}'.format(get_last_day(pair))
    df = pq.ParquetDataset(day, filesystem=s3).read_pandas().to_pandas()
    get_trades(0,( df['time'].min().timestamp()*1000)-1, pair)
    day = 's3://historical-data-misc/{}'.format(get_last_day(pair))
    df = pq.ParquetDataset(day, filesystem=s3).read_pandas().to_pandas()
    ws_link = "wss://api-pub.bitfinex.com/ws/2"
    ws_subscription = '{ "event": "subscribe", "channel": "trades", "symbol": "t' + pair + '"}'
    s = Socket(exchange="Bitfinex", pair=pair, latest_day=df, ws_link=ws_link, ws_subscription=ws_subscription)

def get_trades(start, stop, pair):
    df = create_frame(start, pair)
    df = standardize(df)
    current_ts = df['time'].max().timestamp()*1000
    current_date = df['time'].max().date()

    while current_ts > stop:
        print("BITFINEX | {} to {} | CURRENT TIME {}".format(df['time'].min(), df['time'].max(), datetime.datetime.now()))
        df0 = create_frame(current_ts, pair)
        df0 = standardize(df0)
        frames = split(df0)
        if "date" in df.columns.tolist(): 
            df = df.drop("date", axis=1)
        if "date" in df0.columns.tolist():
            df0 = df0.drop("date", axis=1)
        #check for the corner case when there are two different dates but the new date is entirely within df0
        if df0['time'].max().date() != df['time'].min().date():
            path = create_path(pair, current_date)
            print("WRITING corner branch: {} to {} to {}".format(df['time'].min(), df['time'].max(), create_path(pair, current_date)))
            print(df.head())
            write_s3(df, path)
            df = df0
        if len(frames) > 2:
            df = pd.concat([df, frames[0]])
            path = create_path(pair, current_date)
            write_s3(df, path)
            print("WRITING multi branch 0: {} to {} to {}".format(create_path(pair, df['time'].max().date()), df['time'].min(), df['time'].max()))
            for i in range(1,len(frames)-1):
                print("WRITING multi branch {}: {} to {} to {}".format(i, create_path(pair, frames[i]['time'].max().date()), frames[i]['time'].min(), frames[i]['time'].max()))
                write_s3(frames[i], create_path(pair, frames[i]['time'].max().date()))
            df = frames[-1]
        elif len(frames) == 2:
            df = pd.concat([df, frames[0]])
            print("WRITING: {} to {} to {}".format(df['time'].min(), df['time'].max(), create_path(pair, df['time'].max().date())))
            write_s3(df, create_path(pair, current_date))
            df = frames[1]
        else:
            df = pd.concat([df, frames[0]])
        current_ts = df['time'].min().timestamp()*1000-1
        current_date = df['time'].max().date()
        time.sleep(1)
#get_trades(0,0,"BTCUSD")
