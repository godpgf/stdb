
import string
import datetime,time
import numpy as np
import urllib2
import socket
import json
import re

import tushare as ts


#返回所有股票码，0开头是上证，1开头是深证
def get_all_stock_code():
    url = "http://quotes.money.163.com/hs/service/diyrank.php?page=0&count=3000&sort=PERCENT&order=desc&query=STYPE:EQA&fields=CODE,PRICE,TCAP,MCAP,PE,TURNOVER"
    try:
        response = urllib2.urlopen(url)
        html = response.read().decode('GB2312')
        data = json.loads(html)["list"]
        codes = []
        price = []
        cap = []
        pe = []
        for d in data :
            #代码、价格、总市值、PE
            codes.append(d["CODE"].encode('UTF8'))
            price.append(d["PRICE"])
            cap.append(d["TCAP"])
            pe.append(d["PE"] if "PE" in d else 0)
        return codes,price,cap,pe
    except urllib2.HTTPError,e:
        print e.code
        return None


def date2long(date):
    tmp = date.split('-')
    return string.atol(tmp[0]) * 10000 + string.atol(tmp[1]) * 100 + string.atol(tmp[2])


def long2date(date):
    year = long(date / 10000)
    month = long((date - year * 10000) / 100)
    day = long(date - year * 10000 - month * 100)
    return '%s%s%s'%('%d'%year,_2str(month),_2str(day))


def _2str(date):
    if date >= 10 :
        return '%d'%date
    else:
        return '0%d'%date


#返回某只股票的所有历史数据
def get_history_data(code, trading_calender_int = None):
    url = 'http://quotes.money.163.com/service/chddata.html?code='+code+'&start=%d&end='%(19910403 if trading_calender_int is None else trading_calender_int[0] / 1000000)+time.strftime("%Y%m%d")+ '&fields=TCLOSE;HIGH;LOW;TOPEN;LCLOSE;CHG;PCHG;VOTURNOVER;VATURNOVER;TURNOVER;TCAP;MCAP'
    #url = 'http://quotes.money.163.com/service/chddata.html?code='+code+'&start=20100403&end='+time.strftime("%Y%m%d")+ '&fields=TCLOSE;HIGH;LOW;TOPEN;LCLOSE;CHG;PCHG;VOTURNOVER;VATURNOVER'

    try:
        response = urllib2.urlopen(url)
        html = response.read().decode('latin1').encode('UTF8')
        table = html.split('\r\n')
        if len(table) < 3:
            return None
        stocks = []
        next_date = None
        for i in range(1,len(table)-1):
            if table[i].find('None') != -1:
                continue
            line = table[i].split(',')
            if string.atoi(line[10]) == 0:
                continue
            if len(line[12]) == 0:
                line[12] = '0'
                line[13] = '0'
                line[14] = '0'
            data = (
                date2long(line[0]),#date
                long(string.atof(line[6])*1000),#open
                long(string.atof(line[4])*1000),#high
                long(string.atof(line[5])*1000),#low
                long(string.atof(line[3])*1000),#close
                string.atol(line[10]),#volume
                long(string.atof(line[11])/string.atol(line[10])*1000),#vwap
                long(string.atof(line[9])*10000),#rise
                long(float(line[11])),#amount
                long(string.atof(line[12])*10000),#turn
                long(float(line[13])),#tcap
                long(float(line[14])),#mcap
            )

            if trading_calender_int is not None and len(stocks) > 0:
                cid = trading_calender_int.searchsorted(1000000 * data[0])
                if cid < len(trading_calender_int) - 1:
                    next_date = trading_calender_int[cid + 1] / 1000000
                    #在下一条数据打上缺失标记
                    assert stocks[-1][0] >= next_date
                    if stocks[-1][0] > next_date:
                        stocks.append((next_date,data[4],data[4],data[4],data[4],0,0,0,0,0,data[10],data[11]))

            stocks.append(data)
        if len(stocks) == 0:
            return None
        if trading_calender_int is not None:
            cid = trading_calender_int.searchsorted(1000000 * stocks[-1][0])
            assert cid < len(trading_calender_int)
            if cid > 0:
                #在最远一条数据打上以后缺失标记
                data = (trading_calender_int[cid-1] / 1000000,stocks[-1][1],stocks[-1][1],stocks[-1][1],stocks[-1][1],0,0,0,0,0,stocks[-1][10],stocks[-1][11])
                stocks.append(data)
        return stocks
    except urllib2.HTTPError,e:
        print e.code
        return None
    except socket.error, e:
        print e.message
        return None
    except urllib2.URLError, e:
        print e.message
        return None

def _sina_2_163(code):
    if code[1] == 'h':
        code = '0' + code[2:]
    else :
        code = '1' + code[2:]
    return code

def _163_2_sina(code):
    if code[0] == '0':
        code = 'sh' + code[1:]
    else :
        code = 'sz' + code[1:]
    return code

#返回某只股票的当前数据
def get_current_data(code, retry_count=3, pause=0.01):
    code = _163_2_sina(code)
    url='http://hq.sinajs.cn/list=' + code
    for _ in range(retry_count):
        time.sleep(pause)
        try:
            response = urllib2.urlopen(url)
            html = response.read().decode('latin1').encode('UTF8')
            line = html.split(',')
            if len(line) < 2 or line[8] == '0':
                return None
            data = (
                date2long(line[30]),#date
                long(string.atof(line[1])*1000),#open
                long(string.atof(line[4])*1000),#high
                long(string.atof(line[5])*1000),#low
                long(string.atof(line[3])*1000),#close
                string.atol(line[8]),#volume
                long(string.atof(line[9])/string.atol(line[8])*1000),#vwap
                long((string.atof(line[3]) - string.atof(line[2]))/string.atof(line[2])*100 * 10000),#rise
                long(float(line[9])),#amount
                0,0,0
            )
            return data
        except urllib2.HTTPError, e:
            print e.message
            return None
        except urllib2.URLError, e:
            print url
            print e.message
            return None
        else:
            break

def _get_detail(tag, retry_count=3, pause=0.001):
    p = 0
    code_list = []
    while(True):
        p = p+1
        for _ in range(retry_count):
            time.sleep(pause)
            try:
                url = "http://vip.stock.finance.sina.com.cn/quotes_service/api/json_v2.php/Market_Center.getHQNodeData?page=1&num=1000&sort=symbol&asc=1&node=%s&symbol=&_s_r_a=page" %tag
                response = urllib2.urlopen(url)
                text = response.read().decode('GBK')#.encode('UTF8')
            except urllib2.HTTPError,e:
                pass
            else:
                break
        reg = re.compile(r'\,(.*?)\:')
        text = reg.sub(r',"\1":', text)
        text = text.replace('"{symbol', '{"symbol')
        text = text.replace('{symbol', '{"symbol"')
        jstr = json.dumps(text)
        js = json.loads(jstr)
        js = u'{"pars":%s}'%js
        js = json.loads(js)
        pars = js['pars']
        for p in pars:
            code_list.append(_sina_2_163(p["symbol"]))
        return code_list

def get_industry():
    url = "http://vip.stock.finance.sina.com.cn/q/view/newSinaHy.php"
    try:
        industry_dict = {}
        response = urllib2.urlopen(url)
        html = response.read().decode('GBK').encode('UTF8')
        data_str = html.split('=')[1]
        data_json = json.loads(data_str)
        for row in data_json.values():
            industry_tag = row.split(',')[0]
            industry_value = row.split(',')[1]
            code_list = _get_detail(industry_tag)
            for code in code_list:
                industry_dict[code] = industry_value
        return industry_dict
    except urllib2.HTTPError,e:
        print e.code
        return None


def get_concept():
    url = "http://money.finance.sina.com.cn/q/view/newFLJK.php?param=class"
    try:
        concept_dict = {}
        response = urllib2.urlopen(url)
        html = response.read().decode('GBK').encode('UTF8')
        data_str = html.split('=')[1]
        data_json = json.loads(data_str)
        for row in data_json.values():
            concept_tag = row.split(',')[0]
            concept_value = row.split(',')[1]
            code_list = _get_detail(concept_tag)
            for code in code_list:
                concept_dict[code] = concept_value
        return concept_dict
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

def get_ts_concept():
    concept = ts.get_concept_classified()
    concept_dict = {}
    for index, row in concept.iterrows():
        concept_dict[row['code']] = row['c_name']
    return concept_dict

def get_ts_industry():
    industry = ts.get_industry_classified()
    industry_dict = {}
    for index, row in industry.iterrows():
        industry_dict[row['code']] = row['c_name']
    return industry_dict

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
