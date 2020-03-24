import pandas as pd
from wsocket import Socket
import websocket
import requests
import json

'''
make a request to the coinbase api
target: can be either "products" or "currencies"
endpoint can be: [book, ticker, trades, candles]
params should be a dict of key value pairs representing the request params
Returns: http response
'''
def request(target, product_id, endpoint, params):
    url = "https://api.pro.coinbase.com"
    if target != "currencies" and target != "products":
        return None
    if target == "currencies":
      return requests.get(url)
    
    url = "{}/{}/{}/{}?".format(url, target, product_id, endpoint)
    if params != None:
        for key, value in params.items():
            url = "{}{}={}&".format(url, key, value)
    return requests.get(url[:-1])
'''
Get the orderbook for a given product from the coinbase api
product_id: the name of the currency pair you want the book for
level: the level of detail desired (can be 1, 2 or 3)
raw: if True, return an http request object, else return a dataframe
if there is an error, None will be returned
'''
def get_orderbook(product_id, level, raw):
    if level < 1 or level > 3:
        return None
    response = request("products", product_id, "book", {'level':level})
    if raw == True:
        return response
    else:
        return pd.DataFrame((json.loads(response.text)))
'''
Get the ticker (latest trade) for a given product
product_id: the name of the currency pair you want the latest trade for
raw: if True return an http request object, else return a dataframe
'''
def get_ticker(product_id, raw):
    response = request("products", product_id, "ticker", None)
    if raw:
        return response
    else:
        return pd.DataFrame(json.loads(response.text), index=[0])
'''
Get trades for a given product from the api
product_id: the currency pair you want
fromid: the trade id to start from. pass 0 to get the most recent trades. 
lim: the number of trades to return. max is 100
raw: if True return a http response object, else return a dataframe
'''
def get_trades(product_id, fromid, lim, raw):
    if fromid > 0:
        response = request("products", product_id, "trades", {'after':fromid, 'limit':lim})
    else:
        response = request("products", product_id, "trades", {'limit':lim})
    if raw:
        return response
    else:
        return pd.DataFrame(json.loads(response.text))
'''
Get candles for a given product from the api
product_id: the currency pair you want
start: the start time in the tz (iso 8601) format
end: the end time in the tz (iso 8601) format
granularity: the level of detail for the candles. must be [60, 300, 900, 21600, 86400]
raw: if True return http response object, else return dataframe
'''
def get_candles(product_id, start, end, granularity, raw):
    if granularity not in [60, 300, 900, 21600, 86400]:
        return None
    response = request("products", product_id, "candles", {'start':start, 'end':end, 'granularity':granularity})
    if raw:
        return response
    else:
        return pd.DataFrame(json.loads(response.text))

def get_stats(product_id, raw):
    response = request("products", product_id, "stats", None)
    if raw:
        return response
    else:
        return pd.DataFrame(json.loads(response.text))


#print((request("products", "BTC-USD", "book", {'level':1})).text)
#print(get_orderbook("BTC-USD", 1, True))
#print(get_orderbook("BTC-USD", 1, False))
#print(get_ticker("BTC-USD", True))
#print(get_ticker("BTC-USD", False))
print(get_trades("BTC-USD", 0, 100, True))
print(get_trades("BTC-USD", 0, 100, False))
