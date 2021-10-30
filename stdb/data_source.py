# -*- coding:utf8 -*-
import numpy as np
import pandas as pd
import six
from .data_reader import *


def is_trading(data):
    return data[6] > 0


class LocalDataSource(object):

    def init_trading_dates(self, date):
        date_col = date[:]
        self._trading_dates = pd.Index(pd.Timestamp(long2date(d)) for d in date_col)

    def get_trading_dates(self, start_date, end_date):
        start_date = pd.Timestamp(long2date(date2long(start_date)))
        end_date = pd.Timestamp(long2date(date2long(end_date)))
        left = self._trading_dates.searchsorted(start_date)
        right = self._trading_dates.searchsorted(end_date, side='right')
        return self._trading_dates[left:right]

    def insert_near_2_history(self, history_data, order_book_id, trading_calender_int=None):
        # 将最近几天的数据补上，因为所有历史数据可能最近几天的数据缺失
        if history_data is not None and len(history_data) > 0:
            near_data = get_ifeng_data(order_book_id)
            if near_data is not None and len(near_data) > 0:
                fill_data = []
                if not is_trading(history_data[0]):
                    history_data.pop(0)
                for i in range(len(near_data)):
                    if near_data[i][0] <= history_data[0][0]:
                        near_data = near_data[:i]
                        break
                if len(near_data) > 0:
                    price_scale = history_data[0][4] / history_data[0][5]
                    for data in near_data:
                        # fill_data.append((data[0], data[1] * price_scale, data[2] * price_scale, data[3] * price_scale, data[4] * price_scale, data[5], data[6]))
                        insert_stocks(fill_data, (
                            data[0], data[1] * price_scale, data[2] * price_scale, data[3] * price_scale,
                            data[4] * price_scale,
                            data[5], data[6], data[7]), trading_calender_int)
                    insert_stocks(fill_data, history_data[0], trading_calender_int)
                    history_data.pop(0)
                    fill_data.extend(history_data)
                    history_data = fill_data
        return history_data

    def insert_current_2_history(self, history_data, order_book_id, trading_calender_int=None):
        # 将当前时刻的数据补上
        if history_data is not None and len(history_data) > 0:
            cur_data = get_current_data(order_book_id)
            if cur_data:
                fill_data = []
                if not is_trading(history_data[0]):
                    history_data.pop(0)
                if cur_data[0] == history_data[0][0]:
                    history_data.pop(0)
                price_scale = history_data[0][4] / history_data[0][5]
                cur_data = (
                    cur_data[0], cur_data[1] * price_scale, cur_data[2] * price_scale, cur_data[3] * price_scale,
                    cur_data[4] * price_scale, cur_data[5], cur_data[6], cur_data[7])
                insert_stocks(fill_data, cur_data, trading_calender_int)
                insert_stocks(fill_data, history_data[0], trading_calender_int)
                history_data.pop(0)
                fill_data.extend(history_data)
                history_data = fill_data
        return history_data

    def insert_current_2_bar(self, bars, order_book_id, trading_calender=None):
        history_data = self.bars_2_history(bars)
        history_data = self.insert_current_2_history(history_data, order_book_id, trading_calender)
        start_stocks(history_data, trading_calender)
        return self.history_2_bars(history_data)

    def insert_near_2_bar(self, bars, order_book_id, trading_calender=None):
        history_data = self.bars_2_history(bars)
        history_data = self.insert_near_2_history(history_data, order_book_id, trading_calender)
        start_stocks(history_data, trading_calender)
        return self.history_2_bars(history_data)

    def get_all_bars(self, order_book_id, trading_calender=None, min_date='19910403', is_real_time=False):
        if len(order_book_id) == 6 or len(order_book_id) == 8:
            history_data = get_163_data(order_book_id, trading_calender, min_date=min_date)
            if history_data is None:
                return None
            # 将价格变成复权价
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
                                history_data[j] = (pre_data[0], (pre_data[1] * scale), (pre_data[2] * scale),
                                                   (pre_data[3] * scale), (pre_data[4] * scale), pre_data[5],
                                                   pre_data[6], pre_data[7])
                    history_data[i] = (
                        data[0], (data[1] * scale), (data[2] * scale), (data[3] * scale), (data[4] * scale),
                        data[5], data[6], data[7])
                history_data.reverse()

            history_data = self.insert_near_2_history(history_data, order_book_id, trading_calender)
            if is_real_time:
                history_data = self.insert_current_2_history(history_data, order_book_id, trading_calender)
            start_stocks(history_data, trading_calender)
        else:
            # history_data = get_ts_history_data(order_book_id)
            # cur_data = get_ts_current_data(order_book_id)
            raise NotImplementedError()
        return self.history_2_bars(history_data)

    def history_2_bars(self, history_data):
        stocktype = np.dtype([
            ('date', 'uint64'), ('open', 'float32'),
            ('high', 'float32'), ('low', 'float32'),
            ('close', 'float32'), ('price', 'float32'),
            ('volume', 'uint64'), ('turnover', 'float32')
        ])
        bars = np.array(history_data, dtype=stocktype)
        bars = bars[::-1]  # 转向

        return bars

    def bars_2_history(self, bars):
        # history_data = []
        # for i in range(len(bars) - 1, -1, -1):
        #     history_data.append((int(bars['date'][i]), bars["open"][i], bars["high"][i], bars["low"][i],
        #                          bars["close"][i], int(bars["volume"][i]),
        #                          bars["turn"][i], bars["price"][i]))
        history_data = bars.tolist()
        history_data.reverse()
        return history_data

    def get_dividends(self, order_book_id):
        # 暂时不考虑股息和分红
        return None
