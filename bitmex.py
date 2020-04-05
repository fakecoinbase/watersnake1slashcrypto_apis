import pandas as pd
from wsocket import Socket
import websocket
import requests
import json

def request(endpoint, params):
    url = "https://www.bitmex.com/api/v1/"
    if params != None:
        for key, value in params.items():
            url = "{}?{}={}&".format(url, key, value)
    return requests.get(url[:-1])

def format_output(raw, data):
    if raw:
        return data
    else:
        return pd.DataFrame(data)

def get_announcement(raw):
    response = request("announcement", None)
    return format_output(raw, response)

def chat(channels, connected, count, start, reverse, channelId, raw):
    if channels:
        return format_output(raw, request("chat/channels", None))
    if connected:
        return format_output(raw, request("chat/connected", None))
    return format_output(raw, request("chat", params={'count':count, 'start':start, 'reverse':reverse, 'channelId':channelId}))

def instrument(symbol, fil, columns, count, start, reverse, startTime, endTime, raw):
    params = {'symbol':symbol, 'filter':fil, 'columns':columns, 'count':count, 'start':start, 'reverse':reverse, 'startTime':startTime, 'endTime':endTime}
    return format_output(raw, request("instrument", params))

def instrument_active(raw):
    return format_output(raw, request("instrument/active", None))

def instrument_active_and_indices(raw):
    return format_output(raw, request("instrument/activeAndIndices", None))

def instrument_active_intervals(raw):
    return format_output(raw, request("instrument/activeIntervals", None))

def instrument_composite_index(raw, symbol, fil, columns, count, start, reverse, startTime, endTime):
    params = {'symbol':symbol, 'filter':fil, 'columns':columns, 'count':count, 'start':start, 'reverse':reverse, 'startTime':startTime, 'endTime':endTime}
    return format_output(raw, request("instrument/compositeIndex", params))

def instrument_indices(raw):
    return format_output(raw, request("instrument/indices", None))

def insurance(symbol, fil, columns, count, start, reverse, startTime, endTime):
    params = {'symbol':symbol, 'filter':fil, 'columns':columns, 'count':count, 'start':start, 'reverse':reverse, 'startTime':startTime, 'endTime':endTime}
    return format_output(raw, request("insurance", params))

def leaderboard(method, raw):
    params = {'method':method}
    return format_output(raw, request("leaderboard", params))

def liquidation(symbol, fil, columns, count, start, reverse, startTime, endTime, raw):
    params = {'symbol':symbol, 'filter':fil, 'columns':columns, 'count':count, 'start':start, 'reverse':reverse, 'startTime':startTime, 'endTime':endTime}
    return format_output(raw, request("liquidation", params))

def orderbook_l2(symbol, depth, raw):
    params = {'symbol':symbol, 'depth':depth}
    return format_output(raw, request("orderBook/L2", params))

def settlement(symbol, fil, columns, count, start, reverse, startTime, endTime, raw):
    params = {'symbol':symbol, 'filter':fil, 'columns':columns, 'count':count, 'start':start, 'reverse':reverse, 'startTime':startTime, 'endTime':endTime}
    return format_output(raw, request("settlement", params))

def stats(raw):
    return format_response(raw, request("stats", None))

def stats_history(raw):
    return format_response(raw, request("stats/history", None))

def stats_history_usd(raw):
    return format_response(raw, request("stats/history", None))

def trade(symbol, fil, columns, count, start, reverse, startTime, endTime, raw):
    params = {'symbol':symbol, 'filter':fil, 'columns':columns, 'count':count, 'start':start, 'reverse':reverse, 'startTime':startTime, 'endTime':endTime}
    return format_response(raw, request("trade", params))

def trade_bucketed(binSize, partial, symbol, fil, columns, count, start, reverse, startTime, endtime, raw):
    params = {'binSize':binSize, 'partial':partial, 'symbol':symbol, 'filter':fil, 'columns':columns, 'count':count, 'start':start, 'reverse':reverse, 'startTime':startTime, 'endTime':endTime}
    return format_response(raw, request("trade/bucketed", params))


