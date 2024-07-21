from config import API_KEY, base_url
import yfinance as yf
import pandas as pd
import requests


# replace the "demo" apikey below with your own key from https://www.alphavantage.co/support/#api-key
# get real-time stock data

class DataCollector:
    def __init__(self, API_KEY, base_url, day='1d', interval='5m', symbol='AAPL' ):
        self.API_KEY = API_KEY
        self.base_url = base_url
        self.day = day
        self.interval = interval
        self.symbol = symbol

    def get_data(self):
        # Get the data of the stock
        stock = yf.Ticker(self.symbol)
        stock = stock.history(period='1d', interval=self.interval)
        stock['TimeStamp'] = stock.index
        stock['TimeStamp'] = stock['TimeStamp'].apply(lambda x: x.timestamp())
        stock.drop('Dividends', axis=1, inplace=True)
        stock.drop('Stock Splits', axis=1, inplace=True)
        print(stock.columns)
        return stock

class DataCollectorBuilder:
    def __init__(self):
        self.API_KEY = API_KEY
        self.base_url = base_url
        self.day = '1d'
        self.interval = '1m'
        self.symbol = 'AAPL'

    def build(self):
        return DataCollector(self.API_KEY, self.base_url)

if __name__ == '__main__':
    data_collector = DataCollectorBuilder().build()
    data = data_collector.get_data()
    print(data)