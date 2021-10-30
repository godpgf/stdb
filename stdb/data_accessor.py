#coding=utf-8
#author=godpgf

import abc
import datetime
import pandas as pd
import numpy as np
from six import with_metaclass, string_types
import os

from .data_source import LocalDataSource
from .bar import *
from .data_reader import date2long


class DataProxy(with_metaclass(abc.ABCMeta)):
    @abc.abstractmethod
    def get_bar(self, order_book_id, dt):
        """得到从dt时间开始的股票数据

        :param str order_book_id:
        :param datetime.datetime dt:
        :returns: bar object
        :rtype: BarObject

        """
        raise NotImplementedError

    @abc.abstractmethod
    def get_dividends_by_book_date(self, order_book_id, date):
        """得到股票分红信息

        :param str order_book_id:
        :param datetime.datetime date:
        :returns: dividend
        :rtype: pd.Series

        """
        raise NotImplementedError

    @abc.abstractmethod
    def history(self, order_book_id, dt, bar_count, frequency, field):
        """得到从dt开始的bar_count条历史数据

        :param str order_book_id:
        :param datetime dt:
        :param int bar_count:
        :param str frequency: '1d' or '1m'
        :param str field: "open", "close", "high", "low", "volume", "last", "total_turnover"
        :returns:
        :rtype: pandas.DataFrame

        """
        raise NotImplementedError

    def last(self, order_book_id, dt, bar_count, frequency, field):
        """get history data, will not fill empty data

        :param str order_book_id:
        :param datetime dt:
        :param int bar_count:
        :param str frequency: '1d' or '1m'
        :param str field: "open", "close", "high", "low", "volume", "last", "total_turnover"
        :returns:
        :rtype: pandas.DataFrame

        """
        raise NotImplementedError


class LocalDataProxy(DataProxy):
    """初始化股票信息读取代理
    :param str cache_path:缓存地址
    :param is_offline:是否离线
    """
    def __init__(self, cache_path=None, is_offline=False, min_date="1995-04-24"):
        self._cache_path = cache_path
        self._is_offline = False if cache_path is None else is_offline
        # 缓存对齐后的bar数据
        self._cache_alignment = {}
        # 缓存原始bar数据
        self._cache_source = {}
        self._trading_days = {}
        self._data_source = LocalDataSource()
        self.min_date = min_date
        self.trading_calender = None
        self.load_market()

    def load_market(self, is_real_time=False):
        self.trading_calender = None
        market_data = self.get_all_data('sh000001', is_real_time)
        if market_data is None:
            return
        market_data = market_data[np.where(market_data['volume'] > 0)]
        self._data_source.init_trading_dates(market_data['date'])
        trading_calendar = self.get_trading_dates(self.min_date, datetime.date.today())
        trading_calender_int = np.array(
            [int(t.strftime("%Y%m%d")) for t in trading_calendar], dtype="<u8")
        self.trading_calender = trading_calender_int[
            trading_calender_int <= convert_date_to_int(datetime.date.today())]

    def _load_bars(self, path):
        df = pd.read_csv(path)

        data = np.array([df['date'].values, df['open'].values, df['high'].values, df['low'].values, df['close'].values,
                         df['price'].values, df['volume'].values, df['turnover'].values]).T
        data = [tuple(d.tolist()) for d in data]

        stocktype = np.dtype([
            ('date', 'uint64'), ('open', 'float32'),
            ('high', 'float32'), ('low', 'float32'),
            ('close', 'float32'), ('price', 'float32'),
            ('volume', 'uint64'), ('turnover', 'float32')
        ])
        bars = np.array(data, dtype=stocktype)
        return bars

    def get_all_data(self, order_book_id, is_real_time=False):
        if order_book_id in self._cache_alignment and not is_real_time:
            bars = self._cache_alignment[order_book_id]
        else:
            cache_path = self._cache_path[order_book_id] if isinstance(self._cache_path, dict) else self._cache_path
            path = '%s/%s.csv' % (cache_path, order_book_id)

            # if order_book_id in self._data_source:
            #     bars = self._cache_source[order_book_id]
            # else:
            #     if os.path.exists(path) is False or self.trading_calender is None:
            #         bars = self._data_source.get_all_bars(order_book_id, self.trading_calender,
            #                                               min_date=self.min_date.replace('-', ''),
            #                                               is_real_time=is_real_time)
            #     else:
            #         bars = self._load_bars(path)
            #         if not self._is_offline:
            #             bars = self._data_source.insert_near_2_bar(bars, order_book_id, self.trading_calender)



            if self._is_offline:
                # 离线数据，除了实时更新，不会读取任何网络数据
                if order_book_id in self._cache_source:
                    bars = self._cache_source[order_book_id]
                else:
                    if os.path.exists(path) is False:
                        return None
                    bars = self._load_bars(path)
                    self._cache_source[order_book_id] = bars
            else:
                # 在线数据，如果本地有，优先读取本地的
                if order_book_id in self._cache_source:
                    bars = self._cache_source[order_book_id]
                else:
                    if os.path.exists(path) is False or self.trading_calender is None:
                        bars = self._data_source.get_all_bars(order_book_id, self.trading_calender, min_date=self.min_date.replace('-',''), is_real_time=is_real_time)
                    else:
                        bars = self._load_bars(path)
                        bars = self._data_source.insert_near_2_bar(bars, order_book_id, self.trading_calender)
                    if bars is None:
                        return None
                    self._cache_source[order_book_id] = bars

                    if cache_path:
                        if os.path.exists(cache_path) is False:
                            os.makedirs(cache_path)
                        df = pd.DataFrame({"date":bars["date"],"open":bars["open"],"high":bars["high"],"low":bars["low"],"close":bars["close"],"price":bars["price"],"volume":bars["volume"], "turnover":bars["turnover"]},
                                          columns=["date","open","high","low","close","price","volume","turnover"])
                        df.to_csv(path, index=False)

            if is_real_time:
                bars = self._data_source.insert_current_2_bar(bars, order_book_id, self.trading_calender)

            min_date_int = date2long(self.min_date)
            self._trading_days[order_book_id] = len(bars['date'])
            bars = bars[np.where(bars['date'] >= min_date_int)]
            bars = self._fill_all_bars(bars)
            self._cache_alignment[order_book_id] = bars

        return bars

    @classmethod
    def merge_data(cls, bars, days=5):
        reversed_bar = bars[::-1]
        date = []
        open = []
        high = []
        low = []
        close = []
        price = []
        volume = []
        turnover = []

        for i in range(0, len(reversed_bar) - days, days):
            date.append(reversed_bar["date"][i])
            open.append(reversed_bar["open"][i + days - 1])
            h = reversed_bar["high"][i]
            l = reversed_bar["low"][i]
            v = reversed_bar["volume"][i]
            t = reversed_bar["turnover"]
            for j in range(1, days):
                h = max(h, reversed_bar["high"][i + j])
                l = min(l, reversed_bar["low"][i + j])
                v += reversed_bar["volume"][i + j]
                t += reversed_bar["turnover"][i + j]
            high.append(h)
            low.append(l)
            close.append(reversed_bar["close"][i])
            price.append(reversed_bar["price"][i])
            volume.append(v)
            turnover.append(t)
        data = np.array([date, open, high, low, close,
                         price, volume, turnover]).T
        data = [tuple(d.tolist()) for d in data]

        stocktype = np.dtype([
            ('date', 'uint64'), ('open', 'float32'),
            ('high', 'float32'), ('low', 'float32'),
            ('close', 'float32'), ('price', 'float32'),
            ('volume', 'uint64'), ('turnover', 'float32')
        ])
        return np.array(data, dtype=stocktype)[::-1]

    def get_trading_days(self, order_book_id):
        try:
            days = self._trading_days[order_book_id]
            return days
        except KeyError:
            self.get_all_data(order_book_id)
            return self.get_trading_days(order_book_id)

    def get_table(self, order_book_id):
        bars = self.get_all_data(order_book_id).copy()
        def int2date(date):
            from .data_reader import _2str
            year = int(date / 10000)
            month = int((date - year * 10000) / 100)
            day = int(date - year * 10000 - month * 100)
            return '%s-%s-%s'%('%d'%year,_2str(month),_2str(day))
        date_col = bars["date"]
        index = [pd.Timestamp(int2date(date)) for date in date_col]
        data = [[bars["open"][i],bars["high"][i],bars["low"][i],bars["close"][i],bars["price"][i],bars['volume'][i], bars['turnover'][i]] for i in range(len(index))]
        return pd.DataFrame(np.array(data),index,columns=["Open","High","Low","Close","Price",'Volume','Turnover'])

    def get_bar(self, order_book_id, dt):
        bars = self.get_all_data(order_book_id)

        if isinstance(dt, string_types):
            dt = pd.Timestamp(dt)

        dt = convert_date_to_int(dt)
        return BarObject(bars[bars["date"].searchsorted(dt)])

    def history(self, order_book_id, dt, bar_count, frequency, field):
        if frequency == '1m':
            raise RuntimeError('Minute bar not supported yet!')

        bars = self.get_all_data(order_book_id)

        dt = convert_date_to_int(dt)

        i = bars["date"].searchsorted(dt)
        if i == len(bars["date"]) or bars["date"][i] != dt:
            i -= 1
        left = i - bar_count + 1 if i >= bar_count else 0
        bars = bars[left:i + 1]

        series = pd.Series(bars[field], index=[convert_int_to_date(t) for t in bars["date"]])

        return series

    def last(self, order_book_id, dt, bar_count, frequency, field):
        if frequency == '1m':
            raise RuntimeError('Minute bar not supported yet!')

        try:
            bars = self._origin_cache[order_book_id]
        except KeyError:
            bars = self._data_source.get_all_bars(order_book_id)
            bars = bars[bars["volume"] > 0]
            self._origin_cache[order_book_id] = bars

        dt = convert_date_to_int(dt)

        i = bars["date"].searchsorted(dt)
        left = i - bar_count + 1 if i >= bar_count else 0
        hist = bars[left:i + 1][field]

        return hist

    def get_dividends_by_book_date(self, order_book_id, date):
        #暂时不考虑股息和分红
        return None

    def get_trading_dates(self, start_date, end_date):
        return self._data_source.get_trading_dates(start_date, str(end_date))

    def _fill_all_bars(self, bars):
        if self.trading_calender is None:
            return bars
        trading_calender_int = self.trading_calender

        # prepend
        start_index = trading_calender_int.searchsorted(bars[0]["date"])
        prepend_date = trading_calender_int[:start_index]
        prepend_bars = np.zeros(len(prepend_date), dtype=bars.dtype)
        dates = prepend_bars["date"]
        dates[:] = prepend_date
        prepend_bars["open"].fill(bars[0]["open"])
        prepend_bars["close"].fill(bars[0]["open"])
        prepend_bars["high"].fill(bars[0]["open"])
        prepend_bars["low"].fill(bars[0]["open"])
        prepend_bars["price"].fill(bars[0]["open"])

        # midpend
        last_index = trading_calender_int.searchsorted(bars[-1]["date"])
        midpend_date = trading_calender_int[start_index: last_index + 1]

        midpend_bars = np.zeros(len(midpend_date), dtype=bars.dtype)
        bars_index = bars["date"].searchsorted(midpend_date[0])
        for i in range(len(midpend_bars)):
            if bars[bars_index]["date"] == midpend_date[i]:
                midpend_bars[i] = bars[bars_index]
                bars_index += 1
            else:
                data = (midpend_date[i], bars[bars_index - 1]["close"], bars[bars_index - 1]["close"], bars[bars_index - 1]["close"], bars[bars_index - 1]["close"], bars[bars_index - 1]["close"], 0, 0)
                midpend_bars[i] = data

        # append
        append_date = trading_calender_int[last_index + 1:]
        append_bars = np.zeros(len(append_date), dtype=bars.dtype)
        dates = append_bars["date"]
        dates[:] = append_date
        append_bars["open"].fill(bars[-1]["close"])
        append_bars["close"].fill(bars[-1]["close"])
        append_bars["high"].fill(bars[-1]["close"])
        append_bars["low"].fill(bars[-1]["close"])
        append_bars["price"].fill(bars[-1]["close"])

        # fill bars
        new_bars = np.concatenate([prepend_bars, midpend_bars, append_bars])
        return new_bars
