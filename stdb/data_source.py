#-*- coding:utf8 -*-
import numpy as np
import pandas as pd
import six
from data_reader import *

class LocalDataSource(object):
    PRICE_SCALE = 1000.
    RISE_SCALE = 10000.

    def init_trading_dates(self,date):
        date_col = date[:] / 1000000
        self._trading_dates = pd.Index(pd.Timestamp(int2date(d)) for d in date_col)

    def get_trading_dates(self, start_date, end_date):
        left = self._trading_dates.searchsorted(start_date)
        right = self._trading_dates.searchsorted(end_date, side='right')
        return self._trading_dates[left:right]

    def get_all_bars(self, order_book_id):
        history_data = None
        cur_data = None
        if len(order_book_id) == 7:
            history_data = get_history_data(order_book_id)
            cur_data = get_current_data(order_book_id)

        else:
            #history_data = get_ts_history_data(order_book_id)
            #cur_data = get_ts_current_data(order_book_id)
            raise NotImplementedError()

        if cur_data[0] != history_data[0][0]:
            history_data.insert(0,cur_data)
        stocktype = np.dtype([
            ('date', 'uint64'), ('open', 'float64'),
            ('high', 'float64'), ('low', 'float64'),
            ('close', 'float64'), ('volume', 'float64'),
            ('vwap', 'float64'), ('rise', 'float64'),
            #('rf','float64')
        ])
        bars = np.array(history_data,dtype=stocktype)
        bars = bars[::-1]#转向
        #bars = bars.astype([
        #    ('date', 'uint64'), ('open', 'float64'),
        #    ('high', 'float64'), ('low', 'float64'),
        #    ('close', 'float64'), ('volume', 'float64'),
        #])
        date_col = bars["date"]
        date_col[:] = 1000000 * date_col

        for key in ["open", "high", "low", "close", "vwap"]:
            col = bars[key]
            col[:] = np.round(1 / self.PRICE_SCALE * col, 2)
        rice_col = bars["rise"]
        #rice_col[:-1] = bars["close"][1:]
        #rice_col[:] = (rice_col - bars["close"]) / bars["close"]
        #rice_col[-1] = 0
        rice_col[-1] = (bars["close"][-1]-bars["close"][-2]) * self.RISE_SCALE / bars["close"][-2] * 100
        rice_col[:] = 1 / (self.RISE_SCALE*100) * rice_col

        #rf_col = bars["rf"]
        #rf_col[0] = bars["close"][0]
        #for i in range(1,len(rf_col)):
        #    rf_col[i]= (rice_col[i] + 1) * rf_col[i-1]
        #rf_col[:] = rf_col / bars["close"]
        return bars

    def get_dividends(self, order_book_id):
        # 暂时不考虑股息和分红
        return None