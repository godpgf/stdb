#coding=utf-8
#author=godpgf
from .data_accessor import LocalDataProxy
from .code_accessor import LocalCodeProxy
from .fundamental_accessor import LocalFundamentalProxy
from .classified_accessor import LocalClassifiedProxy
import numpy as np
import pandas as pd


def cal_market_data(stock_list):
    all_weight = 0
    weight_list = []
    for s in stock_list:
        data = s.bar
        cup = data[-1][4] * s.totals
        all_weight += cup
        weight_list.append(cup)
    if all_weight > 0:
        for i in xrange(len(weight_list)):
            weight_list[i] /= all_weight

    date = []
    open = []
    high = []
    low = []
    close = []
    vwap = []
    volume = []
    returns = []
    amount = []

    day_len = len(stock_list[0].bar)
    yestoday_is_use = [False] * len(stock_list)
    for i in xrange(day_len):
        openPrice = 0
        highPrice = 0
        lowPrice = 0
        closePrice = 0
        volumeValue = 0
        vwapPrice = 0
        returnsValue = 0
        amountValue = 0

        #先计算今天的开盘
        realOpenPrice = None
        if i > 0:
            #先用昨天有数据今天还有数据的股票计算出今天的开盘价
            lastOpenPrice = 0
            for j in xrange(len(stock_list)):
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
        for j in xrange(len(stock_list)):
            yestoday_is_use[j] = False
            if stock_list[j].bar[i][5] > 0:
                openPrice += stock_list[j].bar[i][1] * weight_list[j]
                highPrice += stock_list[j].bar[i][2] * weight_list[j]
                lowPrice += stock_list[j].bar[i][3] * weight_list[j]
                closePrice += stock_list[j].bar[i][4] * weight_list[j]
                volumeValue += stock_list[j].bar[i][5] * weight_list[j]
                vwapPrice += stock_list[j].bar[i][6] * weight_list[j]
                returnsValue += stock_list[j].bar[i][7] * weight_list[j]
                amountValue += stock_list[j].bar[i][8] * weight_list[j]
                yestoday_is_use[j] = True

        if realOpenPrice:
            if volumeValue == 0:
                openPrice = close[i-1]
                highPrice = close[i-1]
                lowPrice = close[i-1]
                closePrice = close[i-1]
                vwapPrice = 0
                returnsValue = 0
            else:
                k = realOpenPrice / openPrice
                openPrice *= k
                highPrice *= k
                lowPrice *= k
                closePrice *= k
                volumeValue *= k
                vwapPrice *= k
                returnsValue = (closePrice - close[i-1]) / close[i-1]
                amountValue *= k

        date.append( stock_list[0].bar[i][0] )
        open.append( openPrice )
        high.append( highPrice )
        low.append( lowPrice )
        close.append( closePrice )
        volume.append( volumeValue )
        vwap.append( vwapPrice )
        returns.append( returnsValue )
        amount.append(amountValue)


    stocktype = np.dtype([
        ('date', 'uint64'), ('open', 'float64'),
        ('high', 'float64'), ('low', 'float64'),
        ('close', 'float64'), ('volume', 'float64'),
        ('vwap', 'float64'), ('returns', 'float64'),
        ('amount','float64'), ('turn', 'float64'),
        ('tcap', 'float64'), ('mcap', 'float64')
    ])
    history_data = [(date[i], open[i], high[i], low[i], close[i], volume[i], vwap[i],returns[i], amount[i], 0, 0, 0) for i in xrange(len(date))]
    return np.array(history_data, dtype=stocktype)


class StockData(object):
    def __init__(self, bar, market, industry, totals, earning_ratios = 0):
        self.bar = bar
        self.market = market
        self.industry = industry
        self.totals = totals
        self.earning_ratios = earning_ratios


def download_stock_data(cache_path = "data", is_offline = False):
    codeProxy = LocalCodeProxy(cache_path, is_offline)
    codes = codeProxy.get_codes()
    dataProxy = LocalDataProxy(cache_path, is_offline)

    industry_map = {}
    markey_set = set()

    code_list = []
    price = []
    cap = []
    pe = []
    market = []
    industry = []
    days = []

    for index, row in codes.iterrows():
        data = dataProxy.get_all_Data(row["code"])
        if data is not None and len(data) > 0:
            code_list.append(row["code"])
            price.append(row["price"])
            cap.append(row["cap"])
            pe.append(row['pe'])
            market.append(row["market"])
            markey_set.add(row['market'])
            industry.append(row["industry"])
            days.append(dataProxy.get_trading_days(row["code"]))
            dataProxy.get_all_Data(row["market"])
            if row["industry"] not in industry_map:
                industry_map[row["industry"]] = list()
            industry_map[row["industry"]].append(StockData(data,
                                                           row['market'],
                                                           row['industry'],
                                                           float(row['cap'])/float(row['price'])))

    for key, value in industry_map.items():
        data = cal_market_data(value)
        df = pd.DataFrame({"date":data["date"],
                           "open":data["open"],
                           "high":data["high"],
                           "low":data["low"],
                           "close":data["close"],
                           "volume":data["volume"],
                           "vwap":data["vwap"],
                           "returns":data["returns"],
                           "amount":data["amount"],
                           "turn":data["turn"],
                           "tcap":data["tcap"],
                           "mcap":data["mcap"]}, columns=["date","open","high","low","close","volume","vwap","returns","amount","turn","tcap","mcap"])
        code_list.append(key)
        price.append(data["close"][-1])
        cap.append(None)
        pe.append(None)
        market.append(None)
        industry.append(key)
        days.append(len(data["date"]))
        df.to_csv("%s/%s.csv"%(cache_path,key),index=False)

    for market_code in markey_set:
        data = dataProxy.get_all_Data(market_code)
        code_list.append(market_code)
        price.append(data['close'][-1])
        cap.append(None)
        pe.append(None)
        market.append(market_code)
        industry.append(None)
        days.append(dataProxy.get_trading_days(market_code))

    pd.DataFrame({"code": np.array(code_list),
                  "price": np.array(price),
                  "cap": np.array(cap),
                  "pe": np.array(pe),
                  "market": np.array(market),
                  "industry": np.array(industry),
                  "days":np.array(days)},
                 columns=["code", "market", "industry", "price", "cap", "pe", "days"]).to_csv('%s/%s.csv' % (cache_path, 'codes'), index=False)


def refresh_stock_data(cache_path = "data"):
    codeProxy = LocalCodeProxy(cache_path)
    codes = codeProxy.get_codes()
    dataProxy = LocalDataProxy(cache_path)
    for index, row in codes.iterrows():
        dataProxy.update_current_Data(row["code"])
    download_stock_data(cache_path, is_offline=True)

#TODO delete later
#------------------------------------------------------------





def cal_all_market_data(stock_dict, is_industry):
    market_list_dict = {}
    for key, value in stock_dict.items():
        cl_type = value.industry if is_industry else value.concept
        if cl_type not in market_list_dict:
            market_list_dict[cl_type] = list()
        market_list_dict[cl_type].append(value)

    market_dict = {}
    for key, value in market_list_dict.items():
        market_dict[key] = StockData(cal_market_data(value), None, None, None, 0, 0)
    return market_dict


def load_all_stock_flat(codeProxy, dataProxy, classifiedProxy):
    codes = codeProxy.get_codes()

    market_dict = {
        # 数据、市场、分类、概念、发行量
        "sh":StockData(dataProxy.get_all_Data('0000001'), None, None, None, 0, 0),
        "sz":StockData(dataProxy.get_all_Data('1399001'), None, None, None, 0, 0),
    }

    stock_size = 0
    stock_dict = {}
    for code in codes:
        data = dataProxy.get_all_Data(code[0])
        if data is not None:
            totals = int(code[2] / code[1])
            earning_ratios = code[3] / code[1]
            industry = classifiedProxy.industry_info(code[0])
            concept = classifiedProxy.concept_info(code[0])
            if industry is None or len(industry) == 0:
                industry = u'[其他行业]'
            else:
                industry = u'[%s]'%industry.encode('utf8')
            if concept is None or len(concept) == 0:
                concept = u'(其他概念)'
            else:
                concept = u'(%s)'%concept.encode('utf8')
            if code[0][0] == '0':
                market = 'sh'
            else:
                market = 'sz'

            stock_dict[code[0]] = StockData(data, market, industry, concept, totals, earning_ratios)
            stock_size += 1

    industry_dict = cal_all_market_data(stock_dict, True)
    concept_dict = cal_all_market_data(stock_dict, False)
    return stock_dict, market_dict, industry_dict, concept_dict