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
    def __init__(self, cache_path=None, is_offline=False, min_date = "2005-01-01"):
        self._cache_path = cache_path
        self._is_offline = False if cache_path is None else is_offline
        self._cache = {}
        self._data_source = LocalDataSource()
        self.trading_calender_int = None
        self.min_date = min_date
        market_data = self.get_all_Data('0000001')
        market_data = market_data[np.where(market_data['volume'] > 0)]
        self._data_source.init_trading_dates(market_data['date'])
        self.trading_calendar = self.get_trading_dates(min_date, datetime.date.today())
        trading_calender_int = np.array(
            [int(t.strftime("%Y%m%d000000")) for t in self.trading_calendar], dtype="<u8")
        self.trading_calender_int = trading_calender_int[
            trading_calender_int <= convert_date_to_int(datetime.date.today())]


    def get_all_Data(self, order_book_id):
        try:
            bars = self._cache[order_book_id]
        except KeyError:
            cache_path = self._cache_path[order_book_id] if isinstance(self._cache_path, dict) else self._cache_path
            path = '%s/%s.csv' % (cache_path, order_book_id)
            if self._is_offline :
                if os.path.exists(path) is False:
                    return None
                df = pd.read_csv(path)
                data = [(row["date"],row["open"],row["high"],row["low"],row["close"],
                         row["volume"],row["vwap"],row["returns"],row["amount"]) for index, row in df.iterrows()]

                stocktype = np.dtype([
                    ('date', 'uint64'), ('open', 'float64'),
                    ('high', 'float64'), ('low', 'float64'),
                    ('close', 'float64'), ('volume', 'float64'),
                    ('vwap', 'float64'), ('returns', 'float64'),
                    ('amount','float64')
                ])
                bars = np.array(data, dtype=stocktype)
            else:
                bars = self._data_source.get_all_bars(order_book_id, self.trading_calender_int)
                if bars is None:
                    return None
                if cache_path:
                    if os.path.exists(cache_path) is False:
                        os.makedirs(cache_path)
                    df = pd.DataFrame({"date":bars["date"],"open":bars["open"],"high":bars["high"],"low":bars["low"],"close":bars["close"],"volume":bars["volume"],"vwap":bars["vwap"],"returns":bars["returns"],"amount":bars["amount"]},
                                      columns=["date","open","high","low","close","volume","vwap","returns","amount"])
                    df.to_csv(path, index=False)

            min_date_int = date2long(self.min_date)*1000000
            bars = bars[np.where(bars['date'] > min_date_int)]
            bars = self._fill_all_bars(bars)
            self._cache[order_book_id] = bars

        return bars

    def get_table(self, order_book_id):
        bars = self.get_all_Data(order_book_id).copy()
        def int2date(date):
            from .data_reader import _2str
            year = int(date / 10000)
            month = int((date - year * 10000) / 100)
            day = int(date - year * 10000 - month * 100)
            return '%s-%s-%s'%('%d'%year,_2str(month),_2str(day))
        date_col = bars["date"]
        index = [pd.Timestamp(int2date(data / 1000000)) for data in date_col]
        data = [[bars["open"][i],bars["high"][i],bars["low"][i],bars["close"][i],bars["volume"][i],bars["vwap"][i],bars['returns'][i],bars['amount'][i]] for i in range(len(index))]
        return pd.DataFrame(np.array(data),index,columns=["Open","High","Low","Close","Volume","Vwap",'Returns','Amount'])


    def get_bar(self, order_book_id, dt):
        bars = self.get_all_Data(order_book_id)

        if isinstance(dt, string_types):
            dt = pd.Timestamp(dt)

        dt = convert_date_to_int(dt)
        return BarObject(bars[bars["date"].searchsorted(dt)])

    def history(self, order_book_id, dt, bar_count, frequency, field):
        if frequency == '1m':
            raise RuntimeError('Minute bar not supported yet!')

        bars = self.get_all_Data(order_book_id)

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
        return self._data_source.get_trading_dates(start_date, end_date)

    def _fill_all_bars(self, bars):
        if self.trading_calender_int is None:
            return bars
        trading_calender_int = self.trading_calender_int

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
        prepend_bars["vwap"].fill(bars[0]["vwap"])

        # midpend
        last_index = trading_calender_int.searchsorted(bars[-1]["date"])
        midpend_date = trading_calender_int[start_index: last_index + 1]

        midpend_bars = np.zeros(len(midpend_date), dtype=bars.dtype)
        bars_index = bars["date"].searchsorted(midpend_date[0])
        for i in xrange(len(midpend_bars)):
            if bars[bars_index]["date"] == midpend_date[i]:
                midpend_bars[i] = bars[bars_index]
                bars_index += 1
            else:
                data = (midpend_date[i], bars[bars_index - 1]["close"], bars[bars_index - 1]["close"], bars[bars_index - 1]["close"], bars[bars_index - 1]["close"], 0, bars[bars_index - 1]["vwap"], 0, 0)
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
        append_bars["vwap"].fill(bars[-1]["vwap"])

        # fill bars
        new_bars = np.concatenate([prepend_bars, midpend_bars, append_bars])
        return new_bars
