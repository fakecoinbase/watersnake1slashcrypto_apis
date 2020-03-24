import pandas as pd
from wsocket import Socket
import websocket
import requests
import json

def request(endpoint, pair, params):
    url = 'https://www.bitstamp.net/api/v2/{}/{}'.format(endpoint, pair)
    if params != None:
        for key, value in params.items():
            url = '{}?{}={}'.format(url, key, value)
            return requests.get(url[:-1])
    return requests.get(url)

def get_ticker(pair, raw):
    if raw:
        return request('ticker', pair)
    else:
        return pd.DataFrame(json.loads((request('ticker', pair)).text)
                )
def get_hourly_ticker(pair, raw):
    if raw:
        return request('ticker_hour', pair)
    else:
        return pd.DataFrame(json.loads((request('ticker_hour', pair)).text))

def get_orderbook(pair, group, raw):
    if raw:
        return request('order_book', pair, {'group':group})
    else:
        return pd.DataFrame(json.loads((request('order_book', pair, {'group':group})).text))

def get_transactions(pair, time, raw):
    if raw:
        return request('transactions', pair)
    else:
        return pd.DataFrame(json.loads((request('transactions', pair)).text))

#TODO: add trading pairs info method
