import pandas as pd
from wsocket import Socket
import websocket
import requests
import json

def request(endpoint, params, time_params):
    url = 'https://api.bittrex.com/api/v1.1/public/'.format(endpoint)
    if params != None:
        for key, value in params.items():
            url = '{}?{}={}&'.format(url, key, value)
        return requests.get(url[:-1])
    return requests.get(url)

def get_markets(raw):
    if raw:
        return request("getmarkets", None, None)
    else:
        return pd.DataFrame(json.loads((request("getmarkets", None, None)).text))

def get_currencies(raw):
    if raw:
        return request("getcurrencies", None, None)
    else:
        return pd.DataFrame(json.loads((request("getcurrencies", None, None)).text))

def get_ticker(market, raw):
    if raw:
        return request("getticker", {'market':market})
    else:
        return pd.DataFrame(json.loads((request("getticker", {'market':market})).text))

def get_market_summaries(raw):
    if raw:
        return requet("getmarketsummaries", None, None)
    else:
        return pd.DataFrame(json.loads((requets("getmarketsummaries", None, None)).text))

def get_market_summary(market, raw):
    if raw:
        return request("getmarketsummary", {'market':market})
    else:
        return pd.DataFrame(json.loads((request("getmarketsummary", {'market':market})).text))

def get_orderbook(market, side, raw):
    if raw:
        return request("getorderbook", {'market':market, 'type':side})
    else:
        return pd.DataFrame(json.loads((request("getorderbook", {'market':market, 'type':side}).text)))

def get_market_history(market, raw):
    if raw:
        return request("getmarkethistory", {'market':market})
    else:
        return pd.DataFrame(json.loads((request("getmarkethistory", {'market':market})).text))


    
 

