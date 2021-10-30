import requests
import datetime
import time
import urllib
import socket
import json
import re
import pandas as pd

try:
    from urllib.request import urlopen, Request
except ImportError:
    from urllib2 import urlopen, Request


# 返回所有股票码，0开头是上证，1开头是深证
def get_163_stock_code():
    url = "http://quotes.money.163.com/hs/service/diyrank.php?page=0&count=6000"
    try:
        response = urllib.request.urlopen(url)
        html = response.read().decode('gb2312', 'ignore')
        data = json.loads(html)["list"]
        codes = []
        price = []
        for d in data:
            # 代码、价格、总市值、PE
            codes.append(d["CODE"][1:])
            price.append(d["PRICE"])
        return codes, price
    except urllib.error.HTTPError as e:
        print(e.code)
        return None


def _parsing_dayprice_json(types=None, page=1):
    DAY_TRADING_COLUMNS = ['code', 'symbol', 'name', 'changepercent',
                           'trade', 'open', 'high', 'low', 'settlement', 'volume', 'turnoverratio',
                           'amount', 'per', 'pb', 'mktcap', 'nmc']
    """
           处理当日行情分页数据，格式为json
     Parameters
     ------
        pageNum:页码
     return
     -------
        DataFrame 当日所有股票交易数据(DataFrame)
    """
    request = Request(
        'http://vip.stock.finance.sina.com.cn/quotes_service/api/json_v2.php/Market_Center.getHQNodeData?num=80&sort=code&asc=0&node=%s&symbol=&_s_r_a=page&page=%s' % (
            types, page))
    text = urlopen(request, timeout=10).read()
    if text == 'null':
        return None
    reg = re.compile(r'\,(.*?)\:')
    text = reg.sub(r',"\1":', text.decode('gbk'))
    text = text.replace('"{symbol', '{"symbol')
    text = text.replace('{symbol', '{"symbol"')
    text = text.replace('"{', '{')
    text = text.replace('""', '"')
    text = text.replace(':",', ':"0",')
    jstr = json.dumps(text)

    js = json.loads(jstr)
    df = pd.DataFrame(pd.read_json(js, dtype={'code': object}),
                      columns=DAY_TRADING_COLUMNS)
    df = df.drop('symbol', axis=1)
    #     df = df.ix[df.volume > 0]
    return df


def get_vip_sina_stock_code():
    df = _parsing_dayprice_json('hs_a', 1)
    if df is not None:
        for i in range(2, 60):
            newdf = _parsing_dayprice_json('hs_a', i)
            df = df.append(newdf, ignore_index=True)
    df = df.append(_parsing_dayprice_json('shfxjs', 1),
                   ignore_index=True)
    return df


def date2long(date):
    tmp = date.split('-')
    return int(tmp[0]) * 10000 + int(tmp[1]) * 100 + int(tmp[2])


def long2date(date):
    year = int(date / 10000)
    month = int((date - year * 10000) / 100)
    day = int(date - year * 10000 - month * 100)
    return '%s%s%s' % ('%d' % year, _2str(month), _2str(day))


def _2str(date):
    if date >= 10:
        return '%d' % date
    else:
        return '0%d' % date


def get_xueqiu_data(code, trading_calender=None, min_date='19910403', retry_count=3, timeout=10, pause=0.01,
                    fuquan='before'):
    def time_transfer_timeStamp(time_str):
        timeArray = time.strptime(time_str, "%Y-%m-%d %H:%M:%S")
        timeStamp = int(time.mktime(timeArray))
        return str(timeStamp)

    s = requests.session()
    s.header = {
        'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_11_5) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/50.0.2661.102 Safari/537.36'}

    start_date = min_date if trading_calender is None else str(trading_calender[1])
    start_time_tmp = start_date[0:4] + '-' + start_date[4:6] + '-' + start_date[6:] + ' 00:00:00'
    end_date = time.strftime("%Y%m%d")
    end_time_tmp = end_date[0:4] + '-' + end_date[4:6] + '-' + end_date[6:] + ' 15:30:00'
    if code[0:2] == '60':
        code = 'SH' + code
    elif code[0:2] == 'sh':
        code = 'SH' + code[2:]
    elif code[0:2] == 'sz':
        code = "SZ" + code[2:]
    else:
        code = 'SZ' + code

    start_time = time_transfer_timeStamp(start_time_tmp) + '000'
    end_time = time_transfer_timeStamp(end_time_tmp) + '000'
    for _ in range(retry_count):
        time.sleep(pause)
        try:
            s.get('https://xueqiu.com/')
            url = 'https://xueqiu.com/stock/forchartk/stocklist.json?symbol=' + code + '&period=1day&type=' + fuquan + '&begin=' + start_time + '&end=' + end_time + '&_=' + end_time
            r = s.get(url, timeout=timeout)
            print(r.text)
            table = r.text.split('[{')[1][0:-3].split('},{')
            stocks = []
            for i in range(len(table) - 1, -1, -1):
                a = table[i]
                openp = float(a.split(',')[1].split(':')[1])
                closep = float(a.split(',')[3].split(':')[1])
                highp = float(a.split(',')[2].split(':')[1])
                lowp = float(a.split(',')[4].split(':')[1])
                volume = float(a.split(',')[0].split(':')[1])
                turnrate = float(a.split(',')[7].split(':')[1]) / 100.

                shijian_xueqiu = a.split(',')[-1].split('":"')[1][0:-1]
                c = time.mktime(time.strptime(shijian_xueqiu, "%a %b %d %H:%M:%S +0800 %Y"))
                time_tmp = datetime.datetime.fromtimestamp(c).strftime("%Y-%m-%d")
                date = date2long(time_tmp)
                data = (date, openp, highp, lowp, closep, closep, int(volume), turnrate)
                insert_stocks(stocks, data, trading_calender)
            if len(stocks) == 0:
                return None
            finish_stocks(stocks, trading_calender)
            print(code)
            return stocks
        except Exception as e:
            print(e)
        else:
            break
    return None


# 得到某个股票复权价
def get_sina_fuquan_price(code, type='qianfuquan', retry_count=3, timeout=10, pause=0.01):
    if code[0:2] == '60':
        code = 'sh' + code
    if code[0:2] == 'sh' or code[0:2] == 'sz':
        pass
    else:
        code = 'sz' + code
    url = 'http://finance.sina.com.cn/realstock/company/%s/%s.js?d=%s' % (code, type, time.strftime("%Y-%m-%d"))
    for _ in range(retry_count):
        time.sleep(pause)
        try:
            response = urllib.request.urlopen(url)
            html = response.read().decode('GBK')
            table = html.split(':{')[1].split('}}]')[0].split(',')
            if table is None or len(table) == 0:
                return None
            price_list = []
            for line in table:
                tmp = line.split(':')
                date = date2long(tmp[0][1:].replace('_', '-'))
                price = float(tmp[1].replace('"', ''))
                price_list.append((date, price))
            return price_list
        except urllib.error.HTTPError as e:
            # print(e.reason)
            return None
        except urllib.error.URLError as e:
            print(url)
            print(e.reason)
            return None
        except ValueError as e:
            print(url)
            print(e)
            return None
        except Exception as e:
            return None
        else:
            break


# 在stocks中插入一天的数据
def insert_stocks(stocks, data, trading_calender):
    if trading_calender is not None and len(stocks) > 0:
        cid = trading_calender.searchsorted(data[0])
        if cid < len(trading_calender) - 1:
            next_date = trading_calender[cid + 1]
            # 在下一条数据打上缺失标记
            assert stocks[-1][0] >= next_date
            if stocks[-1][0] > next_date:
                stocks.append((next_date, data[4], data[4], data[4], data[4], data[5], 0, 0))

    stocks.append(data)


def start_stocks(stocks, trading_calender):
    if trading_calender is not None:
        cid = trading_calender.searchsorted(stocks[0][0])
        if cid < len(trading_calender) - 1:
            next_date = trading_calender[cid + 1]
            close = stocks[0][4]
            price = stocks[0][5]
            stocks.insert(0, (next_date, close, close, close, close, price, 0, 0))


def finish_stocks(stocks, trading_calender):
    if trading_calender is not None:
        cid = trading_calender.searchsorted(stocks[-1][0])
        assert cid < len(trading_calender)
        if cid > 0:
            # 在最远一条数据打上以后缺失标记
            data = (
                trading_calender[cid - 1], stocks[-1][1], stocks[-1][1], stocks[-1][1], stocks[-1][1], stocks[-1][1], 0,
                0.0)
            stocks.append(data)


# 返回某只股票的所有历史数据
def get_163_data(code, trading_calender=None, min_date='19910403', retry_count=3, timeout=10, pause=0.01):
    # url = 'http://quotes.money.163.com/service/chddata.html?code='+code+'&start=20100403&end='+time.strftime("%Y%m%d")+ '&fields=TCLOSE;HIGH;LOW;TOPEN;LCLOSE;CHG;PCHG;VOTURNOVER;VATURNOVER'
    if code[0:2] == '60':
        code = '0' + code
    elif code[0:2] == 'sh':
        code = '0' + code[2:]
    elif code[0:2] == 'sz':
        code = '1' + code[2:]
    else:
        code = '1' + code
    url = 'http://quotes.money.163.com/service/chddata.html?code=' + code + '&start=%s&end=' % (
        min_date if trading_calender is None else str(trading_calender[1])) + time.strftime(
        "%Y%m%d") + '&fields=TCLOSE;HIGH;LOW;TOPEN;LCLOSE;CHG;PCHG;VOTURNOVER;VATURNOVER;TURNOVER;TCAP;MCAP'
    for _ in range(retry_count):
        time.sleep(pause)
        try:
            response = urllib.request.urlopen(url, timeout=timeout)
            html = response.read().decode('gb2312', 'ignore')
            table = html.split('\r\n')
            if len(table) < 3:
                return None
            stocks = []
            # next_date = None
            for i in range(1, len(table) - 1):
                if table[i].find('None') != -1:
                    continue
                line = table[i].split(',')
                if len(line[10]) == 0 or int(line[10]) == 0:
                    continue
                if len(line[12]) == 0:
                    line[12] = '0'
                    line[13] = '0'
                    line[14] = '0'
                data = (
                    date2long(line[0]),  # date
                    float(line[6]),  # open
                    float(line[4]),  # high
                    float(line[5]),  # low
                    float(line[3]),  # close
                    float(line[3]),  # price
                    int(line[10]),  # volume
                    float(line[12]) / 100.,  # turn
                )
                insert_stocks(stocks, data, trading_calender)
            if len(stocks) == 0:
                return None
            finish_stocks(stocks, trading_calender)
            print(code[1:])
            return stocks
        except urllib.error.HTTPError as e:
            print(e.code)
        except socket.error as e:
            print(e)
        except urllib.error.URLError as e:
            print(e.reason)
        else:
            break
    return None


# 返回某只股票的当前数据
def get_current_data(code, retry_count=3, pause=0.01):
    if code[0:2] == '60':
        code = 'sh' + code
    elif code[0:2] == 'sh' or code[0:2] == 'sz':
        pass
    else:
        code = 'sz' + code
    url = 'http://hq.sinajs.cn/list=' + code
    for _ in range(retry_count):
        time.sleep(pause)
        try:
            response = urllib.request.urlopen(url)
            html = response.read().decode('GBK')
            line = html.split(',')
            if len(line) < 2 or line[8] == '0':
                return None
            data = (
                date2long(line[30]),  # date
                float(line[1]),  # open
                float(line[4]),  # high
                float(line[5]),  # low
                float(line[3]),  # close
                float(line[3]),  # price
                int(line[8]),  # volume
                0.0
            )
            return data
        except urllib.error.HTTPError as e:
            print(e.reason)
            return None
        except urllib.error.URLError as e:
            print(url)
            print(e.reason)
            return None
        else:
            break


# 返回股票最近数据，弥补历史数据缺失的问题
def get_ifeng_data(code, retry_count=3, pause=0.01):
    if code[0:2] == '60':
        code = 'sh' + code
    elif code[0:2] == 'sh' or code[0:2] == 'sz':
        pass
    else:
        code = 'sz' + code
    url = 'http://api.finance.ifeng.com/akdaily/?code=%s&type=last' % code
    for _ in range(retry_count):
        time.sleep(pause)
        try:
            request = Request(url)
            lines = urlopen(request, timeout=10).read()
            if len(lines) < 15:  # no data
                return None
        except Exception as e:
            print(e)
        else:
            data_list = []
            js = json.loads(lines.decode('utf-8'))
            cols = ['date', 'open', 'high', 'close', 'low', 'volume', 'price_change', 'p_change',
                    'ma5', 'ma10', 'ma20', 'v_ma5', 'v_ma10', 'v_ma20']
            lines = js['record']
            pre_close = None
            for id, line in enumerate(lines):
                data = (
                    date2long(line[0]),  # date
                    float(line[1]),  # open
                    float(line[2]),  # high
                    float(line[4]),  # low
                    float(line[3]),  # close
                    float(line[3]),  # price
                    int(float(line[5]) * 100),  # volume
                    float(line[14]) / 100.0 if len(line) >= 15 else 0.0  # turnover
                )
                # pre_close = float(line[3])
                data_list.append(data)
            data_list.reverse()
            return data_list


def _get_detail(tag, retry_count=3, pause=0.001):
    p = 0
    code_list = []
    while (True):
        p = p + 1
        for _ in range(retry_count):
            time.sleep(pause)
            try:
                url = "http://vip.stock.finance.sina.com.cn/quotes_service/api/json_v2.php/Market_Center.getHQNodeData?page=1&num=1000&sort=symbol&asc=1&node=%s&symbol=&_s_r_a=page" % tag
                response = urllib.request.urlopen(url)
                text = response.read().decode('GBK')  # .encode('UTF8')
            except urllib.error.HTTPError as e:
                pass
            else:
                break
        reg = re.compile(r'\,(.*?)\:')
        text = reg.sub(r',"\1":', text)
        text = text.replace('"{symbol', '{"symbol')
        text = text.replace('{symbol', '{"symbol"')
        jstr = json.dumps(text)
        js = json.loads(jstr)
        js = u'{"pars":%s}' % js
        js = json.loads(js)
        pars = js['pars']
        for p in pars:
            code = p["symbol"]
            code_list.append(code[2:])
        return code_list


def get_industry():
    url = "http://vip.stock.finance.sina.com.cn/q/view/newSinaHy.php"
    try:
        industry_dict = {}
        response = urllib.request.urlopen(url)
        html = response.read().decode('gb2312', 'ignore')
        data_str = html.split('=')[1]
        data_json = json.loads(data_str)
        industry_value_2_type = {}
        type_cnt = 0
        for row in data_json.values():
            industry_tag = row.split(',')[0]
            industry_value = row.split(',')[1]
            code_list = _get_detail(industry_tag)
            if industry_value not in industry_value_2_type:
                industry_value_2_type[industry_value] = "type%d" % type_cnt
                type_cnt += 1
            for code in code_list:
                industry_dict[code] = industry_value_2_type[industry_value]

        return industry_dict
    except urllib.error.HTTPError as e:
        print(e.code)
        return None


def get_concept():
    url = "http://money.finance.sina.com.cn/q/view/newFLJK.php?param=class"
    try:
        concept_dict = {}
        response = urllib.request.urlopen(url)
        html = response.read().decode('gb2312', 'ignore')
        data_str = html.split('=')[1]
        data_json = json.loads(data_str)
        for row in data_json.values():
            concept_tag = row.split(',')[0]
            concept_value = row.split(',')[1]
            code_list = _get_detail(concept_tag)
            for code in code_list:
                concept_dict[code] = concept_value
        return concept_dict
    except urllib.error.HTTPError as e:
        print(e.code)
        return None
