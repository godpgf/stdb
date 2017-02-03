
import string
import datetime,time
import numpy as np
import urllib2
import json

import tushare as ts


#返回所有股票码，0开头是上证，1开头是深证
def get_all_stock_code():
    url = "http://quotes.money.163.com/hs/service/diyrank.php?page=0&count=3000&sort=PERCENT&order=desc&query=STYPE:EQA&fields=CODE,PRICE"
    try:
        response = urllib2.urlopen(url)
        html = response.read().decode('GB2312')
        data = json.loads(html)["list"]
        codes = []
        for d in data :
            codes.append((d["CODE"].encode('UTF8'),d["PRICE"]))
        return codes
    except urllib2.HTTPError,e:
        print e.code
        return None


def date2int(date):
    tmp = date.split('-')
    return string.atoi(tmp[0]) * 10000 + string.atoi(tmp[1]) * 100 + string.atoi(tmp[2])


def int2date(date):
    year = int(date / 10000)
    month = int((date - year * 10000) / 100)
    day = int(date - year * 10000 - month * 100)
    return '%s%s%s'%('%d'%year,_2str(month),_2str(day))


def _2str(date):
    if date >= 10 :
        return '%d'%date
    else:
        return '0%d'%date


#返回某只股票的所有历史数据
def get_history_data(code):
    url = 'http://quotes.money.163.com/service/chddata.html?code='+code+'&start=19910403&end='+time.strftime("%Y%m%d")+ '&fields=TCLOSE;HIGH;LOW;TOPEN;LCLOSE;CHG;PCHG;VOTURNOVER;VATURNOVER'
    #url = 'http://quotes.money.163.com/service/chddata.html?code='+code+'&start=20100403&end='+time.strftime("%Y%m%d")+ '&fields=TCLOSE;HIGH;LOW;TOPEN;LCLOSE;CHG;PCHG;VOTURNOVER;VATURNOVER'

    try:
        response = urllib2.urlopen(url)
        html = response.read().decode('latin1').encode('UTF8')
        table = html.split('\r\n')
        stocks = []
        for i in range(1,len(table)-1):
            if table[i].find('None') != -1:
                break
            line = table[i].split(',')
            if string.atoi(line[10]) == 0:
                continue
            data = (
                date2int(line[0]),#date
                int(string.atof(line[6])*1000),#open
                int(string.atof(line[4])*1000),#high
                int(string.atof(line[5])*1000),#low
                int(string.atof(line[3])*1000),#close
                string.atoi(line[10]),#volume
                int(string.atof(line[11])/string.atoi(line[10])*1000),#vwap
                int(string.atof(line[9])*10000),#rise
            )
            stocks.append(data)
        return stocks
    except urllib2.HTTPError,e:
        print e.code
        return None


#返回某只股票的当前数据
def get_current_data(code):
    if code[0] == '0':
        code = 'sh' + code[1:]
    else :
        code = 'sz' + code[1:]
    url='http://hq.sinajs.cn/list=' + code
    try:
        response = urllib2.urlopen(url)
        html = response.read().decode('latin1').encode('UTF8')
        line = html.split(',')
        if len(line) < 2 :
            return None
        data = (
            date2int(line[30]),#date
            int(string.atof(line[1])*1000),#open
            int(string.atof(line[4])*1000),#high
            int(string.atof(line[5])*1000),#low
            int(string.atof(line[3])*1000),#close
            string.atoi(line[8]),#volume
            int(string.atof(line[9])/string.atoi(line[8])*1000),#vwap
            0,#rise
        )
        return data
    except urllib2.HTTPError,e:
        print e.code
        return None

"""
def get_ts_history_data(code):
    his = ts.get_hist_data(code)
    stocks = []
    for d in his.index:
        values = his.loc[d]
        data = (
            #date2int(d.strftime('%Y-%m-%d')),
            date2int(d),
            int(values['open']*1000),
            int(values['high']*1000),
            int(values['low']*1000),
            int(values['close']*1000),
            int(values['volume']),
            0
        )
        stocks.append(data)
    return stocks


def get_ts_current_data(code):
    cur = ts.get_realtime_quotes(code);
    data = (
        date2int(cur['date'].real[0]),
        int(string.atof(cur['open'].real[0])*1000),
        int(string.atof(cur['high'].real[0])*1000),
        int(string.atof(cur['low'].real[0])*1000),
        int(string.atof(cur['price'].real[0])*1000),
        int(string.atoi(cur['volume'].real[0])),
        0,
    )
    return data
"""

def get_ts_base_info():
    return ts.get_stock_basics()


def get_ts_report_info(year, quarter):
    return ts.get_report_data(year, quarter)


def get_ts_profit_info(year, quarter):
    return ts.get_profit_data(year, quarter)


def get_ts_operation_info(year, quarter):
    return ts.get_operation_data(year, quarter)


def get_ts_growth_info(year, quarter):
    return ts.get_growth_data(year, quarter)


def get_ts_debtpaying_info(year, quarter):
    return ts.get_debtpaying_data(year, quarter)

def get_ts_cashflow_info(year, quarter):
    return ts.get_cashflow_data(year, quarter)