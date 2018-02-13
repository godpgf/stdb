#coding=utf-8
#author=godpgf

from stdb import *
cp = LocalCodeProxy('test',True)
cp.get_codes()
fun_accesser = LocalFundamentalProxy('test',False)
info = fun_accesser.report_info('000001',2015,1)
print(info)