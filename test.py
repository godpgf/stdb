#coding=utf-8
#author=godpgf
import time
from stdb import *
print(time.strftime('%Y-%m-%d %H:%M:%S',time.localtime(time.time())))
dp = LocalDataProxy('data')
d = dp.get_all_data('600703')
#refresh_stock_data()
download_stock_data()
print(time.strftime('%Y-%m-%d %H:%M:%S',time.localtime(time.time())))