#coding=utf-8
#author=godpgf

import os
import abc
from six import with_metaclass
import pandas as pd
import numpy as np
from .data_reader import *


class CodeProxy(with_metaclass(abc.ABCMeta)):
    #返回所有股票码，0开头是上证，1开头是深证
    @abc.abstractmethod
    def get_codes(self):
        raise NotImplementedError


class LocalCodeProxy(CodeProxy):
    """初始化股票信息读取代理
    :param str cache_path:缓存地址
    :param is_offline:是否离线
    """
    def __init__(self, cache_path=None, is_offline=False):
        self._cache_path = cache_path
        self._is_offline = False if cache_path is None else is_offline
        self._cache = None

    def get_codes(self, market_code = 'sz399005'):
        if self._cache is None:
            if self._is_offline:
                path = '%s/%s.csv'%(self._cache_path,'codes')
                if os.path.exists(path) is False:
                    return None
                #self._cache = np.fromfile(path, np.dtype('|S7'))
                self._cache = pd.read_csv(path, dtype=str)
            else:
                # codes, price= get_163_stock_code()
                code_table = get_vip_sina_stock_code()

                # code_table['code'] = code_table['code'].apply(lambda v:v[2:])
                codes = code_table['code'].values
                price = code_table['trade'].values
                name = code_table['name'].values
                mktcap = code_table['mktcap'].values
                per = code_table['per'].values

                self._cache = pd.DataFrame({"code": codes, "price": price, "name": name, 'mktcap': mktcap, 'per': per})
                if self._cache_path is not None:
                    if os.path.exists(self._cache_path) is False:
                        os.makedirs(self._cache_path)
                    self._cache.to_csv('%s/%s.csv'%(self._cache_path,'codes'), index = False)
        return self._cache
