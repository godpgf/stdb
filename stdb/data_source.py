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
        self._trading_dates = pd.Index(pd.Timestamp(long2date(d)) for d in date_col)

    def get_trading_dates(self, start_date, end_date):
        left = self._trading_dates.searchsorted(start_date)
        right = self._trading_dates.searchsorted(end_date, side='right')
        return self._trading_dates[left:right]

    def get_all_bars(self, order_book_id, trading_calender_int = None):
        history_data = None
        cur_data = None
        if len(order_book_id) == 7:
            history_data = get_history_data(order_book_id, trading_calender_int)
            if history_data and trading_calender_int:
                cid = trading_calender_int.searchsorted(1000000 * history_data[0][0])
                if cid < len(trading_calender_int) -1:
                    next_date = trading_calender_int[cid+1] / 1000000
                    cur_data = get_current_data(order_book_id)
                    if cur_data:
                        close = history_data[0][4]
                        #打上下一天数据为空标记
                        if cur_data[0] > next_date:
                            history_data.insert(0,(
                                next_date,close,close,close,close,0,0,0,0
                            ))


            else:
                cur_data = get_current_data(order_book_id)

        else:
            #history_data = get_ts_history_data(order_book_id)
            #cur_data = get_ts_current_data(order_book_id)
            raise NotImplementedError()

        if history_data is None or len(history_data) == 0:
            return None

        if cur_data is not None and cur_data[0] != history_data[0][0]:
            history_data.insert(0,cur_data)
        stocktype = np.dtype([
            ('date', 'uint64'), ('open', 'float64'),
            ('high', 'float64'), ('low', 'float64'),
            ('close', 'float64'), ('volume', 'float64'),
            ('vwap', 'float64'), ('returns', 'float64'),
            ('amount','float64')
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
        rise_col = bars['returns']
        rise_col[:] = rise_col / (self.RISE_SCALE * 100.)
        # if len(bars["close"]) >= 2:
        #     rice_col[-1] = (bars["close"][-1]-bars["close"][-2]) * self.RISE_SCALE / bars["close"][-2] * 100
        # rice_col[:] = 1 / (self.RISE_SCALE*100) * rice_col

        return bars

    def get_dividends(self, order_book_id):
        # 暂时不考虑股息和分红
        return None