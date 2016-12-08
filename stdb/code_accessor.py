#coding=utf-8
#author=godpgf

import os
import abc
from six import with_metaclass, string_types
import numpy as np

from .data_reader import get_all_stock_code


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

    def get_codes(self):
        if self._cache is None:
            code_type = np.dtype([('code', '|S7'), ('price', 'float64')])
            if self._is_offline:
                path = '%s/%s.bin'%(self._cache_path,'codes')
                if os.path.exists(path) is False:
                    return None
                #self._cache = np.fromfile(path, np.dtype('|S7'))
                self._cache = np.fromfile(path, code_type)
            else:
                self._cache = np.array(get_all_stock_code(),code_type)

                if self._cache_path is not None:
                    if os.path.exists(self._cache_path) is False:
                        os.makedirs(self._cache_path)
                    self._cache.tofile('%s/%s.bin'%(self._cache_path,'codes'))
        return self._cache