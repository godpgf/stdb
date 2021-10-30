#coding=utf-8
#author=godpgf
from .data_accessor import LocalDataProxy
from .code_accessor import LocalCodeProxy
import numpy as np
import pandas as pd
import os


def cal_market_data(stock_list):
    all_weight = 0
    weight_list = []
    for s in stock_list:
        data = s.bar
        cup = data[-1][4] * s.totals
        all_weight += cup
        weight_list.append(cup)
    if all_weight > 0:
        for i in range(len(weight_list)):
            weight_list[i] /= all_weight

    date = []
    open = []
    high = []
    low = []
    close = []
    volume = []
    turnover = []

    day_len = len(stock_list[0].bar)
    yestoday_is_use = [False] * len(stock_list)
    for i in range(day_len):
        openPrice = 0
        highPrice = 0
        lowPrice = 0
        closePrice = 0
        volumeValue = 0

        #先计算今天的开盘
        realOpenPrice = None
        if i > 0:
            #先用昨天有数据今天还有数据的股票计算出今天的开盘价
            lastOpenPrice = 0
            for j in range(len(stock_list)):
                if yestoday_is_use[j] and stock_list[j].bar[i][5] > 0:
                    lastOpenPrice += stock_list[j].bar[i-1][1] * weight_list[j]
                    openPrice += stock_list[j].bar[i][1] * weight_list[j]

            if lastOpenPrice != 0:
                realOpenPrice = openPrice * open[i-1] / lastOpenPrice
            else:
                #当遇到计算不出的情况取个近似
                realOpenPrice = close[i-1]
            openPrice = 0


        #计算为了得到这个开盘价所需要的缩放
        for j in range(len(stock_list)):
            yestoday_is_use[j] = False
            if stock_list[j].bar[i][5] > 0:
                openPrice += stock_list[j].bar[i][1] * weight_list[j]
                highPrice += stock_list[j].bar[i][2] * weight_list[j]
                lowPrice += stock_list[j].bar[i][3] * weight_list[j]
                closePrice += stock_list[j].bar[i][4] * weight_list[j]
                volumeValue += stock_list[j].bar[i][5] * weight_list[j]
                yestoday_is_use[j] = True

        if realOpenPrice:
            if volumeValue == 0:
                openPrice = close[i-1]
                highPrice = close[i-1]
                lowPrice = close[i-1]
                closePrice = close[i-1]
            else:
                k = realOpenPrice / openPrice
                openPrice *= k
                highPrice *= k
                lowPrice *= k
                closePrice *= k
                volumeValue *= k

        date.append( stock_list[0].bar[i][0] )
        open.append( openPrice )
        high.append( highPrice )
        low.append( lowPrice )
        close.append( closePrice )
        volume.append( volumeValue )
        turnover.append(0.0)


    stocktype = np.dtype([
        ('date', 'uint64'), ('open', 'float32'),
        ('high', 'float32'), ('low', 'float32'),
        ('close', 'float32'), ('price', 'float32'),
        ('volume', 'uint64'), ('turnover', 'float32')
    ])
    history_data = [(date[i], open[i], high[i], low[i], close[i], 0, volume[i], 0) for i in range(len(date))]
    return np.array(history_data, dtype=stocktype)


class StockData(object):
    def __init__(self, bar, market, industry, totals = 1, earning_ratios = 0):
        self.bar = bar
        self.market = market
        self.industry = industry
        self.totals = totals
        self.earning_ratios = earning_ratios


def download_stock_data(cache_path="data", is_offline=False, min_date="2012-01-01", is_real_time=False):
    codeProxy = LocalCodeProxy(cache_path, is_offline)
    codes = codeProxy.get_codes()
    dataProxy = LocalDataProxy(cache_path, is_offline, min_date)

    industry_map = {}
    markey_set = set()

    code_list = []
    price = []
    market = []
    industry = []
    days = []

    code_set = set()
    for index, row in codes.iterrows():
        if row["code"] in code_set:
            continue
        code_set.add(row["code"])
        data = dataProxy.get_all_data(row["code"], is_real_time)
        if data is not None and len(data) > 0:
            code_list.append(row["code"])
            price.append(row["price"])
            market.append(row["market"])
            markey_set.add(row['market'])
            industry.append(row["industry"])
            days.append(dataProxy.get_trading_days(row["code"]))
            dataProxy.get_all_data(row["market"])
            if row["industry"] not in industry_map:
                industry_map[row["industry"]] = list()
            industry_map[row["industry"]].append(StockData(data,
                                                           row['market'],
                                                           row['industry']))

    for key, value in industry_map.items():
        data = cal_market_data(value)
        df = pd.DataFrame({"date":data["date"],
                           "open":data["open"],
                           "high":data["high"],
                           "low":data["low"],
                           "close":data["close"],
                           "volume":data["volume"],
                           "turnover":data["turnover"]}, columns=["date","open","high","low","close","volume","turn"])
        code_list.append(key)
        price.append(data["close"][-1])

        market.append(None)
        industry.append(key)
        days.append(len(data["date"]))
        df.to_csv("%s/%s.csv" % (cache_path, key), index=False)

    for market_code in markey_set:
        data = dataProxy.get_all_data(market_code)
        code_list.append(market_code)
        price.append(data['close'][-1])
        market.append(market_code)
        industry.append(None)
        days.append(dataProxy.get_trading_days(market_code))

    pd.DataFrame({"code": np.array(code_list),
                  "price": np.array(price),
                  "market": np.array(market),
                  "industry": np.array(industry),
                  "days": np.array(days)},
                 columns=["code", "market", "industry", "price", "days"]).to_csv('%s/%s.csv' % (cache_path, 'codes'), index=False)


def download_industry(code_list, market_code, path, min_date="2005-09-01"):
    if not os.path.exists(path):
        os.mkdir(path)
    dataProxy = LocalDataProxy(path, min_date=min_date)
    price = []
    market = []
    industry = []
    days = []
    for code in code_list:
        data = dataProxy.get_all_data(code)
        price.append(data['close'][-1])
        market.append(market_code)
        industry.append(market_code)
        days.append(dataProxy.get_trading_days(code))

    if market_code:
        data = dataProxy.get_all_data(market_code)
        code_list.append(market_code)
        price.append(data['close'][-1])
        market.append(market_code)
        industry.append(None)
        days.append(dataProxy.get_trading_days(market_code))
    pd.DataFrame({"code": np.array(code_list),
                  "price": np.array(price),
                  "market": np.array(market),
                  "industry": np.array(industry),
                  "days":np.array(days)},
                 columns=["code", "market", "industry", "price", "days"]).to_csv('%s/%s.csv' % (path, 'codes'), index=False)