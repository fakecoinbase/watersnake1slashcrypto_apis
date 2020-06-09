import pandas as pd
from wsocket import Socket
import websocket
import requests
import json

def request(params, time_params):
    url = 'https://api-pub.bitfinex.com/v2/'
    for p in params:
        url = "{}{}".format(url, p)
    if time_params != None:
        for key, value in time_params.items():
            url = '{}?{}={}&'.format(url, key, value)
            return requests.get(url[:-1])
    else:
        return requests.get(url)

def get_status():
    return request("platform", None)

def get_ticker(symbol, raw):
    response = request(['tickers/?symbols=', symbol], None)
    if raw:
        return response
    else:
        return pd.DataFrame(json.loads(response.text))

def get_trades(symbol, start, end, lim, sort, raw):
    if sort not in [0, 1] or lim > 1e4:
        return None
    if raw:
        return request(['trades', symbol], {'start':start, 'end':end, 'limit':lim, 'sort':sort})
    else:
        return pd.DataFame(json.loads((request(['trades', symbol], {'start':start, 'end':end, 'limit':lim, 'sort':sort})).text))

def get_book(symbol, precision, raw):
    if precision not in ['P0', 'P1', 'P2', 'P3', 'P4', 'R0']:
        return None
    if raw:
        return request(['book', symbol, precision])
    else:
        return pd.DataFrame(json.loads((request(['book', symbol, precision])).text))

def get_stats(key, size, symbol, side, section, raw):
    allowed_keys = ['funding.size', 'credits.size', 'credits.size.sym', 'pos.size', 'vol.1d', 'vol.7d', 'vol.30d', 'vwap']
    allowed_sizes = ['30m', '1d', '1m']
    if key not in allowed_keys or size not in allowed_sizes or (side != 'long' and side != 'short') or (section != 'last' and section != 'hist'):
        return None
    q = '{}:{}:{}/{}'.format(key, size, symbol, side)
    if raw:
        return request(['stats1', q]) 
    else:
        return pd.DataFrame(json.loads((request(['stats1', q])).text))

def get_candles(timeframe, symbol, section, period, start, end, lim, sort, raw):
    allowed_times = ['1m', '5m', '15m', '30m', '1h', '3h', '6h', '12h', '1D', '7D', '14D', '1M']
    if timeframe not in allowed_times or (section != 'last' and section != hist):
        return None
    q = 'trade:{}:{}/{}'.format(timeframe, symbol, section)
    if raw:
        return request(['candle', q], {'start':start, 'end':end, 'limit':lim, 'sort':sort})
    else:
        return pd.DataFrame(json.loads((request(['candle', q], {'start':start, 'end':end, 'limit':lim, 'sort':sort})).text))

def get_liquidations(start, end, lim, sort, raw):
    if raw:
        return request(['liquidations'], {'start':start, 'end':end, 'limit':lim, 'sort':sort})
    else:
        return pd.DataFrame(json.loads((request(['liquidations'], {'start':start, 'end':end, 'limit':lim, 'sort':sort})).text))

def get_leaderboards(key, timeframe, symbol, start, end, lim, sort):
    allowed_keys = ['plu_diff', 'plu', 'vol', 'plr']
    if key not in allowed_keys:
        return None
    q = 'rankings/{}:{}:{}/hist'.format(key, timeframe, symbol)
    if raw:
        return request(['rankings', q], {'start':start, 'end':end, 'limit':lim, 'sort':sort})
    else:
        return pd.DataFrame(json.loads((request(['rankings', q], {'start':start, 'end':end, 'limit':lim, 'sort':sort})).text))
