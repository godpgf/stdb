# -*- coding: utf-8 -*-
#
# Copyright 2016 Ricequant, Inc
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

import datetime

def convert_date_to_int(dt):
    t = dt.year * 10000 + dt.month * 100 + dt.day
    t *= 1000000
    return t


def convert_dt_to_int(dt):
    t = convert_date_to_int(dt)
    t += dt.hour * 10000 + dt.minute * 100 + dt.second
    return t


def convert_int_to_date(dt_int):
    dt_int = int(dt_int)
    year = dt_int // 10000000000
    month = (dt_int // 100000000) % 100
    day = (dt_int // 1000000) % 100
    return datetime.datetime(year, month, day)


class BarObject(object):
    def __init__(self, data):
        self._data = data

    @property
    def open(self):
        return self._data["open"]

    @property
    def close(self):
        return self._data["close"]

    @property
    def low(self):
        return self._data["low"]

    @property
    def high(self):
        return self._data["high"]

    @property
    def last(self):
        return self.close

    @property
    def volume(self):
        return self._data["volume"]

    @property
    def datetime(self):
        return datetime.datetime.strptime(str(self._data["date"]), "%Y%m%d%H%M%S")

    @property
    def is_trading(self):
        return self.volume > 0

    def mavg(self, intervals, frequency="day"):
        """
        Returns moving average price for the given security for a give number
            of intervals for a frequency, by default to `"day"`.
        :param int intervals: a given number of intervals, e.g. given number
            of days
        :param str frequency: frequency of the give number of intervals, by
            default as ‘day’.
        """
        raise NotImplementedError

    def vwap(self, intervals, frequency="day"):
        raise NotImplementedError

    def history(self, bar_count, frequency, field):
        raise NotImplementedError

    def __repr__(self):
        return "BarObject({0})".format(self.__dict__)

    def __getitem__(self, key):
        return self.__dict__[key]


class BarMap(object):
    def __init__(self, dt, data_proxy):
        self.dt = dt
        #self.universe = universe
        self.data_proxy = data_proxy

    def update_dt(self, dt):
        self.dt = dt

    def __getitem__(self, key):
        return self.data_proxy.get_bar(key, self.dt)

    def __repr__(self):
        return "{0}()".format(type(self).__name__)
