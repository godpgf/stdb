import string
import time
import urllib
import socket
import json
import re
try:
    from urllib.request import urlopen, Request
except ImportError:
    from urllib2 import urlopen, Request


#返回所有股票码，0开头是上证，1开头是深证
def get_all_stock_code():
    url = "http://quotes.money.163.com/hs/service/diyrank.php?page=0&count=5000&sort=PERCENT&order=desc&query=STYPE:EQA&fields=CODE,PRICE,TCAP,MCAP,PE,TURNOVER"
    try:
        response = urllib.request.urlopen(url)
        html = response.read().decode('gb2312', 'ignore')
        data = json.loads(html)["list"]
        codes = []
        price = []
        cap = []
        pe = []
        for d in data :
            #代码、价格、总市值、PE
            codes.append(d["CODE"])
            price.append(d["PRICE"])
            cap.append(d["TCAP"])
            pe.append(d["PE"] if "PE" in d else 0)
        return codes,price,cap,pe
    except urllib.error.HTTPError as e:
        print(e.code)
        return None


def date2long(date):
    tmp = date.split('-')
    return int(tmp[0]) * 10000 + int(tmp[1]) * 100 + int(tmp[2])


def long2date(date):
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
def get_history_data(code, trading_calender_int = None, min_date = '19910403', retry_count=3,  timeout = 10, pause = 0.01):
    url = 'http://quotes.money.163.com/service/chddata.html?code='+code+'&start=%s&end='%(min_date if trading_calender_int is None else int(trading_calender_int[1] / 1000000))+time.strftime("%Y%m%d")+ '&fields=TCLOSE;HIGH;LOW;TOPEN;LCLOSE;CHG;PCHG;VOTURNOVER;VATURNOVER;TURNOVER;TCAP;MCAP'
    #url = 'http://quotes.money.163.com/service/chddata.html?code='+code+'&start=20100403&end='+time.strftime("%Y%m%d")+ '&fields=TCLOSE;HIGH;LOW;TOPEN;LCLOSE;CHG;PCHG;VOTURNOVER;VATURNOVER'

    for _ in range(retry_count):
        time.sleep(pause)
        try:
            response = urllib.request.urlopen(url, timeout=timeout)
            html = response.read().decode('gb2312', 'ignore')
            table = html.split('\r\n')
            if len(table) < 3:
                return None
            stocks = []
            next_date = None
            for i in range(1,len(table)-1):
                if table[i].find('None') != -1:
                    continue
                line = table[i].split(',')
                if int(line[10]) == 0:
                    continue
                if len(line[12]) == 0:
                    line[12] = '0'
                    line[13] = '0'
                    line[14] = '0'
                data = (
                    date2long(line[0]),#date
                    int(float(line[6])*1000),#open
                    int(float(line[4])*1000),#high
                    int(float(line[5])*1000),#low
                    int(float(line[3])*1000),#close
                    int(line[10]),#volume
                    int(float(line[11])/int(line[10])*1000),#vwap
                    int(float(line[9])*10000),#rise
                    int(float(line[11])),#amount
                    int(float(line[12])*10000),#turn
                    int(float(line[13])),#tcap
                    int(float(line[14])),#mcap
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
            print(code)
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
            response = urllib.request.urlopen(url)
            html = response.read().decode('GBK')
            line = html.split(',')
            if len(line) < 2 or line[8] == '0':
                return None
            data = (
                date2long(line[30]),#date
                int(float(line[1])*1000),#open
                int(float(line[4])*1000),#high
                int(float(line[5])*1000),#low
                int(float(line[3])*1000),#close
                int(line[8]),#volume
                int(float(line[9])/int(line[8])*1000),#vwap
                int((float(line[3]) - float(line[2]))/float(line[2])*100 * 10000),#rise
                int(float(line[9])),#amount
                0,0,0
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

#返回股票最近数据，弥补历史数据缺失的问题
def get_near_data(code, retry_count=3, pause=0.01):
    code = _163_2_sina(code)
    url = 'http://api.finance.ifeng.com/akdaily/?code=%s&type=last'%code
    for _ in range(retry_count):
        time.sleep(pause)
        try:
            request = Request(url)
            lines = urlopen(request, timeout = 10).read()
            if len(lines) < 15: #no data
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
                    int(float(line[1]) * 1000),  # open
                    int(float(line[2]) * 1000),  # high
                    int(float(line[4]) * 1000),  # low
                    int(float(line[3]) * 1000),  # close
                    int(float(line[5])),  # volume
                    int((float(line[3]) + float(line[2]) + float(line[4])) / 3 * 1000),  # vwap
                    int((float(line[3]) - pre_close) / pre_close * 100 * 10000 if pre_close else 1 * 100 * 10000),  # rise
                    int(float(line[5]) * (float(line[3]) + float(line[2]) + float(line[4])) / 3),  # amount
                    0, 0, 0
                )
                pre_close = float(line[3])
                data_list.append(data)
            data_list.reverse()
            return data_list

def _get_detail(tag, retry_count=3, pause=0.001):
    p = 0
    code_list = []
    while(True):
        p = p+1
        for _ in range(retry_count):
            time.sleep(pause)
            try:
                url = "http://vip.stock.finance.sina.com.cn/quotes_service/api/json_v2.php/Market_Center.getHQNodeData?page=1&num=1000&sort=symbol&asc=1&node=%s&symbol=&_s_r_a=page" %tag
                response = urllib.request.urlopen(url)
                text = response.read().decode('GBK')#.encode('UTF8')
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
                industry_value_2_type[industry_value] = "type%d"%type_cnt
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

