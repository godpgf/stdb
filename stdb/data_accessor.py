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
    def __init__(self, cache_path=None, is_offline=False):
        self._cache_path = cache_path
        self._is_offline = False if cache_path is None else is_offline
        self._cache = {}
        self._data_source = LocalDataSource()
        self.trading_calender_int = None
        self._data_source.init_trading_dates(self.get_all_Data('0000001')['date'])
        self.trading_calendar = self.get_trading_dates("2005-01-01", datetime.date.today())
        trading_calender_int = np.array(
            [int(t.strftime("%Y%m%d000000")) for t in self.trading_calendar], dtype="<u8")
        self.trading_calender_int = trading_calender_int[
            trading_calender_int <= convert_date_to_int(datetime.date.today())]

    def get_all_Data(self, order_book_id):
        try:
            bars = self._cache[order_book_id]
        except KeyError:
            bars = None
            if self._is_offline :
                path = '%s/%s.bin'%(self._cache_path,order_book_id)
                if os.path.exists(path) is False:
                    return None
                stocktype = np.dtype([
                    ('date', 'uint64'), ('open', 'float64'),
                    ('high', 'float64'), ('low', 'float64'),
                    ('close', 'float64'), ('volume', 'float64'),
                    ('vwap', 'float64'), ('rise', 'float64'),
                    #('rf','float64')
                ])
                bars = np.fromfile(path,stocktype)
            else:
                bars = self._data_source.get_all_bars(order_book_id)
                if bars is None:
                    return None
                if self._cache_path:
                    if os.path.exists(self._cache_path) is False:
                        os.makedirs(self._cache_path)
                    bars.tofile('%s/%s.bin'%(self._cache_path,order_book_id))
            #bars = self._fill_all_bars(bars)
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
        data = [[bars["open"][i],bars["high"][i],bars["low"][i],bars["close"][i],bars["volume"][i],bars["vwap"][i],bars["rise"][i]] for i in range(len(index))]
        return pd.DataFrame(np.array(data),index,columns=["Open","High","Low","Close","Volume","Vwap","Rise"])


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
        if self.trading_calender_int == None:
            return bars
        trading_calender_int = self.trading_calender_int

        # prepend
        prepend_date = trading_calender_int[:trading_calender_int.searchsorted(bars[0]["date"])]
        prepend_bars = np.zeros(len(prepend_date), dtype=bars.dtype)
        dates = prepend_bars["date"]
        dates[:] = prepend_date

        # append
        append_date = trading_calender_int[trading_calender_int.searchsorted(bars[-1]["date"]) + 1:]
        append_bars = np.zeros(len(append_date), dtype=bars.dtype)
        dates = append_bars["date"]
        dates[:] = append_date

        for key in ["open", "high", "low", "close"]:
            col = append_bars[key]
            col[:] = bars[-1][key]  # fill with bars's last bar

        # fill bars
        new_bars = np.concatenate([prepend_bars, bars, append_bars])
        return new_bars
