#coding=utf-8
#author=godpgf

import os
import abc
from six import with_metaclass, string_types
try:
  import cPickle as pickle
except ImportError:
  import pickle

from .data_reader import *


class FundamentalProxy(with_metaclass(abc.ABCMeta)):

    @abc.abstractmethod
    def base_info(self, order_book_id):
        """得到股票基本信息

        :param str order_book_id:
        :returns: result instrument
        :rtype: Instrument

        """
        raise NotImplementedError

    @abc.abstractmethod
    def report_info(self, order_book_id, year, quarter):
        """得到股票业绩报表数据

        :param str order_book_id:
        :returns: result instrument
        :rtype: Instrument

        """
        raise NotImplementedError

    @abc.abstractmethod
    def profit_info(self, order_book_id, year, quarter):
        """获取盈利能力数据

        :param str order_book_id:
        :returns: result instrument
        :rtype: Instrument

        """
        raise NotImplementedError

    @abc.abstractmethod
    def operation_info(self, order_book_id, year, quarter):
        """获取营运能力数据

        :param str order_book_id:
        :returns: result instrument
        :rtype: Instrument

        """
        raise NotImplementedError

    @abc.abstractmethod
    def growth_info(self, order_book_id, year, quarter):
        """获取成长能力数据

        :param str order_book_id:
        :returns: result instrument
        :rtype: Instrument

        """
        raise NotImplementedError

    @abc.abstractmethod
    def debtpaying_info(self, order_book_id, year, quarter):
        """获取偿债能力数据

        :param str order_book_id:
        :returns: result instrument
        :rtype: Instrument

        """
        ts.get_industry_classified()
        raise NotImplementedError

    @abc.abstractmethod
    def cashflow_info(self, order_book_id, year, quarter):
        """获取现金流量数据

        :param str order_book_id:
        :returns: result instrument
        :rtype: Instrument

        """
        raise NotImplementedError


class LocalFundamentalProxy(FundamentalProxy):

    def __init__(self, cache_path=None, is_offline=False):
        self._cache_path = cache_path
        self._is_offline = False if cache_path is None else is_offline
        self._cache_base_info = None
        self._cache_report_info = {}
        self._cache_profit_info = {}
        self._cache_operation_info = {}
        self._cache_growth_info = {}
        self._cache_debtpaying_info = {}
        self._cache_cashflow_info = {}

    def base_info(self, order_book_id):
        if len(order_book_id) == 7:
            order_book_id = order_book_id[1:]
        if self._cache_base_info is None:
            if self._is_offline:
                path = '%s/%s.bin'%(self._cache_path,'base')
                if os.path.exists(path) is False:
                    return None
                f = open(path, 'rb')
                self._cache_base_info = pickle.load(f)
                f.close()
            else:
                self._cache_base_info = get_ts_base_info()
                if self._cache_path is not None:
                    if os.path.exists(self._cache_path) is False:
                        os.makedirs(self._cache_path)
                    f = open('%s/%s.bin'%(self._cache_path,'base'), 'wb')
                    pickle.dump(self._cache_base_info, f)
                    f.close()
        try:
            return self._cache_base_info.loc[order_book_id]
        except KeyError:
            return None

    def report_info(self, order_book_id, year, quarter):
        if len(order_book_id) == 7:
            order_book_id = order_book_id[1:]
        data = self.get_cache_data(year, quarter, self._cache_report_info, get_ts_report_info, 'report')
        if data is None:
            return None
        return self.get_book_data(data, order_book_id)


    def profit_info(self, order_book_id, year, quarter):
        if len(order_book_id) == 7:
            order_book_id = order_book_id[1:]
        data = self.get_cache_data(year, quarter, self._cache_profit_info, get_ts_profit_info, 'profit')
        if data is None:
            return None
        return self.get_book_data(data, order_book_id)

    def operation_info(self, order_book_id, year, quarter):
        if len(order_book_id) == 7:
            order_book_id = order_book_id[1:]
        data = self.get_cache_data(year, quarter, self._cache_operation_info, get_ts_operation_info, 'operation')
        if data is None:
            return None
        return self.get_book_data(data, order_book_id)

    def growth_info(self, order_book_id, year, quarter):
        if len(order_book_id) == 7:
            order_book_id = order_book_id[1:]
        data = self.get_cache_data(year, quarter, self._cache_growth_info, get_ts_growth_info, 'growth')
        if data is None:
            return None
        return self.get_book_data(data, order_book_id)

    def debtpaying_info(self, order_book_id, year, quarter):
        if len(order_book_id) == 7:
            order_book_id = order_book_id[1:]
        data = self.get_cache_data(year, quarter, self._cache_debtpaying_info, get_ts_debtpaying_info, 'debtpaying')
        if data is None:
            return None
        return self.get_book_data(data, order_book_id)

    def cashflow_info(self, order_book_id, year, quarter):
        if len(order_book_id) == 7:
            order_book_id = order_book_id[1:]
        data = self.get_cache_data(year, quarter, self._cache_cashflow_info, get_ts_cashflow_info, 'cashflow')
        if data is None:
            return None
        return self.get_book_data(data, order_book_id)

    def get_book_data(self, data, order_book_id):
        index = list(data['code']).index(order_book_id)
        return data.loc[index]

    def get_cache_data(self, year, quarter, cache, reader, pre_file_name):
        key_str = '%d_%d'%(year,quarter)
        c_data = None
        if cache.has_key(key_str):
            c_data = cache[key_str]

        if c_data is None:
            path = '%s/%s_%s.bin'%(self._cache_path,pre_file_name,key_str)
            if self._is_offline:
                if os.path.exists(path) is False:
                    return None
                f = open(path, 'rb')
                c_data = pickle.load(f)
                f.close()
            else:
                if self._cache_path is not None:
                    if os.path.exists(path):
                        f = open(path, 'rb')
                        c_data = pickle.load(f)
                        f.close()
                    else:
                        c_data = reader(year, quarter)
                        if os.path.exists(self._cache_path) is False:
                            os.makedirs(self._cache_path)
                        f = open(path, 'wb')
                        pickle.dump(c_data, f)
                        f.close()
                else:
                    c_data = reader(year, quarter)
            cache[key_str] = c_data
            return c_data




