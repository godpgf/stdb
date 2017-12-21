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


class ClassifiedProxy(with_metaclass(abc.ABCMeta)):

    @abc.abstractmethod
    def industry_info(self, order_book_id):
        """得到股票行业信息

        :param str order_book_id:
        :returns: result instrument
        :rtype: Instrument

        """
        raise NotImplementedError

    @abc.abstractmethod
    def concept_info(self, order_book_id):
        """得到股票概念信息

        :param str order_book_id:
        :returns: result instrument
        :rtype: Instrument

        """
        raise NotImplementedError

class LocalClassifiedProxy(ClassifiedProxy):

    def __init__(self, cache_path=None, is_offline=False):
        self._cache_path = cache_path
        self._is_offline = False if cache_path is None else is_offline
        self._cache_industry_info = None
        self._cache_concept_info = None

    def industry_info(self, order_book_id):
        if len(order_book_id) == 7:
            order_book_id = order_book_id[1:]
        if self._cache_industry_info is None:
            if self._is_offline:
                path = '%s/%s.bin'%(self._cache_path,'industry')
                if os.path.exists(path) is False:
                    return None
                f = open(path, 'rb')
                self._cache_industry_info = pickle.load(f)
                f.close()
            else:
                self._cache_industry_info = get_industry()
                if self._cache_path is not None:
                    if os.path.exists(self._cache_path) is False:
                        os.makedirs(self._cache_path)
                    f = open('%s/%s.bin'%(self._cache_path,'industry'), 'wb')
                    pickle.dump(self._cache_industry_info, f)
                    f.close()
        try:
            return self._cache_industry_info[order_book_id]
        except KeyError:
            return None

    def concept_info(self, order_book_id):
        if len(order_book_id) == 7:
            order_book_id = order_book_id[1:]
        if self._cache_concept_info is None:
            if self._is_offline:
                path = '%s/%s.bin'%(self._cache_path,'concept')
                if os.path.exists(path) is False:
                    return None
                f = open(path, 'rb')
                self._cache_concept_info = pickle.load(f)
                f.close()
            else:
                self._cache_concept_info = get_concept()
                if self._cache_path is not None:
                    if os.path.exists(self._cache_path) is False:
                        os.makedirs(self._cache_path)
                    f = open('%s/%s.bin'%(self._cache_path,'concept'), 'wb')
                    pickle.dump(self._cache_concept_info, f)
                    f.close()
        try:
            return self._cache_concept_info[order_book_id]
        except KeyError:
            return None