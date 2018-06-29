#-*- coding:utf8 -*-
import numpy as np
import pandas as pd
import six
from .data_reader import *


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

    def insert_current_2_history(self, history_data, order_book_id, trading_calender_int = None, is_update_cur_data = False):
        cur_data = None
        if history_data and len(history_data) > 0:
            if trading_calender_int is not None:
                cid = trading_calender_int.searchsorted(1000000 * history_data[0][0])
                if cid < len(trading_calender_int) -1:
                    next_date = trading_calender_int[cid+1] / 1000000
                    cur_data = get_current_data(order_book_id)
                    close = history_data[0][4]
                    #turn = history_data[0][9]
                    tcap = history_data[0][10]
                    mcap = history_data[0][11]
                    if cur_data and cur_data[0] != history_data[0][0]:
                        #打上下一天数据为空标记
                        if cur_data[0] > next_date:
                            history_data.insert(0,(
                                int(next_date), close, close, close, close,0,0,0,0,0,tcap,mcap
                            ))
                    else:
                        history_data.insert(0, (
                            int(next_date), close, close, close, close, 0, 0, 0, 0,0,tcap,mcap
                        ))
                elif is_update_cur_data:
                    cur_data = get_current_data(order_book_id)
                    if cur_data and cur_data[5] > history_data[0][5]:
                        history_data.pop(0)
                    else:
                        cur_data =  None
            else:
                cur_data = get_current_data(order_book_id)
        else:
            return

        if cur_data is not None and cur_data[0] > history_data[0][0]:
            close = history_data[0][4]
            volume = history_data[0][5]
            turn = history_data[0][9] * (0 if volume == 0 else cur_data[5] / volume)
            tcap = history_data[0][10] * (cur_data[4] / close)
            mcap = history_data[0][11] * (cur_data[4] / close)
            cur_data = (cur_data[0],cur_data[1],cur_data[2],cur_data[3],cur_data[4],cur_data[5],
                        cur_data[6],cur_data[7],cur_data[8],turn,tcap,mcap)
            history_data.insert(0, cur_data)

    def get_all_bars(self, order_book_id, trading_calender_int = None, is_update_cur_data = False, min_date = '19910403'):
        if len(order_book_id) == 7:
            history_data = get_history_data(order_book_id, trading_calender_int, min_date=min_date)
            if history_data is None:
                return None
            self.insert_current_2_history(history_data, order_book_id, trading_calender_int, is_update_cur_data)
        else:
            #history_data = get_ts_history_data(order_book_id)
            #cur_data = get_ts_current_data(order_book_id)
            raise NotImplementedError()
        return self.history_2_bars(history_data)


    def history_2_bars(self, history_data):
        stocktype = np.dtype([
            ('date', 'uint64'), ('open', 'float64'),
            ('high', 'float64'), ('low', 'float64'),
            ('close', 'float64'), ('volume', 'float64'),
            ('vwap', 'float64'), ('returns', 'float64'),
            ('amount','float64'), ('turn', 'float64'),
            ('tcap', 'float64'), ('mcap', 'float64')
        ])
        bars = np.array(history_data,dtype=stocktype)
        bars = bars[::-1]#转向
        date_col = bars["date"]
        date_col[:] = 1000000 * date_col

        for key in ["open", "high", "low", "close", "vwap"]:
            col = bars[key]
            col[:] = np.round(1 / self.PRICE_SCALE * col, 2)
        rise_col = bars['returns']
        rise_col[:] = rise_col / (self.RISE_SCALE * 100.)
        turn_col = bars['turn']
        turn_col[:] = turn_col / (self.RISE_SCALE * 100.)

        return bars

    def get_dividends(self, order_book_id):
        # 暂时不考虑股息和分红
        return None