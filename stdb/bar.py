import datetime


def convert_date_to_int(dt):
    t = dt.year * 10000 + dt.month * 100 + dt.day
    return t


def convert_int_to_date(dt_int):
    dt_int = int(dt_int)
    year = dt_int // 10000
    month = (dt_int // 100) % 100
    day = dt_int % 100
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
    def turnover(self):
        return self._data["turnover"]

    @property
    def price(self):
        return self._data["price"]

    @property
    def datetime(self):
        return datetime.datetime.strptime(str(self._data["date"]), "%Y%m%d%H%M%S")

    @property
    def is_trading(self):
        return self.volume > 0

    def history(self, bar_count, frequency, field):
        raise NotImplementedError

    def __repr__(self):
        return "BarObject({0})".format(self.__dict__)

    def __getitem__(self, key):
        return self.__dict__[key]



