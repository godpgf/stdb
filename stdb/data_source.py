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

    def insert_current_2_history(self, history_data, order_book_id, price_scale, trading_calender_int=None, is_real_time=False):
        if history_data is not None and len(history_data) > 0:
            fill_data = []
            cur_data = get_current_data(order_book_id) if is_real_time else None
            if cur_data and cur_data[0] > history_data[0][0]:
                cur_data = (cur_data[0],cur_data[1] * price_scale,cur_data[2] * price_scale,cur_data[3]*price_scale,cur_data[4]*price_scale,cur_data[5],0)
            else:
                cur_data = None
                # insert_stocks(fill_data, cur_data, trading_calender_int)

            near_data = get_ifeng_data(order_book_id)
            if cur_data is not None and near_data is not None and cur_data[0] == near_data[0][0]:
                # near_data.pop(0)
                cur_data = None

            if cur_data is not None:
                insert_stocks(fill_data, cur_data, trading_calender_int)

            if near_data is not None and len(near_data) > 0:
                for i in range(len(near_data)):
                    if near_data[i][0] <= history_data[0][0]:
                        near_data = near_data[:i]
                        break
                for data in near_data:
                    # fill_data.append((data[0], data[1] * price_scale, data[2] * price_scale, data[3] * price_scale, data[4] * price_scale, data[5], data[6]))
                    insert_stocks(fill_data, (data[0], data[1] * price_scale, data[2] * price_scale, data[3] * price_scale, data[4] * price_scale, data[5], data[6]), trading_calender_int)

            if len(fill_data) > 0:
                if trading_calender_int is not None:
                    cid = trading_calender_int.searchsorted(1000000 * history_data[0][0])
                    next_date = trading_calender_int[cid + 1] / 1000000
                    if next_date < fill_data[-1][0]:
                        close = history_data[0][4]
                        fill_data.append((int(next_date), close, close, close, close, 0, 0))
                fill_data.extend(history_data)
                history_data = fill_data

            if trading_calender_int is not None:
                cid = trading_calender_int.searchsorted(1000000 * history_data[0][0])
                if cid < len(trading_calender_int) -1:
                    next_date = int(trading_calender_int[cid+1] / 1000000)
                    close = history_data[0][4]
                    history_data.insert(0, (int(next_date), close, close, close, close, 0, 0))

        return history_data

    def get_all_bars(self, order_book_id, trading_calender_int=None, min_date='19910403', is_real_time=False):
        if len(order_book_id) == 6 or len(order_book_id) == 8:
            history_data = get_163_data(order_book_id, trading_calender_int, min_date=min_date)
            #将价格变成复权价
            fuquan_price = get_sina_fuquan_price(order_book_id)
            scale = 1
            if fuquan_price is not None and history_data is not None:
                fuquan_price.reverse()
                history_data.reverse()
                price_index = 0
                is_scale_pre = False
                for i in range(len(history_data)):
                    data = history_data[i]
                    while price_index < len(fuquan_price) and fuquan_price[price_index][0] < data[0]:
                        price_index += 1
                    if price_index < len(fuquan_price) and data[0] == fuquan_price[price_index][0]:
                        scale = fuquan_price[price_index][1] / float(data[4])
                        if is_scale_pre is False:
                            is_scale_pre = True
                            for j in range(i):
                                pre_data = history_data[j]
                                history_data[j] = (pre_data[0],int(pre_data[1] * scale),int(pre_data[2] * scale),int(pre_data[3] * scale),int(pre_data[4] * scale),pre_data[5],pre_data[6])
                    history_data[i] = (data[0],int(data[1] * scale),int(data[2] * scale),int(data[3] * scale),int(data[4] * scale),data[5],data[6])
                history_data.reverse()
            if history_data is None:
                return None
            history_data = self.insert_current_2_history(history_data, order_book_id, scale, trading_calender_int, is_real_time)
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
            ('turn', 'float64')
        ])
        bars = np.array(history_data,dtype=stocktype)
        bars = bars[::-1]#转向
        date_col = bars["date"]
        date_col[:] = 1000000 * date_col

        for key in ["open", "high", "low", "close"]:
            col = bars[key]
            col[:] = np.round(1 / self.PRICE_SCALE * col, 2)
        turn_col = bars['turn']
        turn_col[:] = turn_col / (self.RISE_SCALE * 100.)

        return bars

    def get_dividends(self, order_book_id):
        # 暂时不考虑股息和分红
        return None