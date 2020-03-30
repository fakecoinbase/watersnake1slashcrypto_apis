import pandas as pd
import websocket
import requests
import json
import coinbase
import bitfinex
import binance
import bitstamp

def test_coinbase():
    #print((request("products", "BTC-USD", "book", {'level':1})).text)
    print(coinbase.get_orderbook("BTC-USD", 1, True))
    print(coinbase.get_orderbook("BTC-USD", 1, False))
    print(coinbase.get_ticker("BTC-USD", True))
    print(coinbase.get_ticker("BTC-USD", False))
    print(coinbase.get_trades("BTC-USD", 0, 100, True))
    print(coinbase.get_trades("BTC-USD", 0, 100, False))
    print(coinbase.get_candles("BTC-USD", "2020-03-18T23:59:59Z", "2020-03-20T23:59:59Z", 900, True))
    print(coinbase.get_candles("BTC-USD", "2020-03-18T23:59:59Z", "2020-03-20T23:59:59Z", 900, False))

def test_bitfinex():
    print(bitfinex.get_status())
    print(bitfinex.get_ticker("BTCUSD", False))
    print(bitfinex.get_trades("BTCUSD", 1578268799000, 1577923199000, 1000, 1, False))
    print(bitfinex.get_book("BTCUSD", "P0", False))
    print(bitfinex.get_stats("funding.size", "30m", "BTCUSD", "long", "last", False))
    print(bitfinex.get_candles("1m", "BTCUSD", "last", "", 1578268799000,1577923199000,1000, 1, False))
    print(bitfinex.get_liquidations(1578268799000,1577923199000,1000, 1, False))
    print(bitfinex.get_leaderboards("plu_diff", "1m", "BTCUSD", 1578268799000,1577923199000,1000, 1, False))
