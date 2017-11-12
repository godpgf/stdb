# 股票数据访问

## 股票基本数据

####创建数据访问对象

```python
dataProxy = LocalDataProxy()
```

####得到所有历史数据
```python
#第一个0表示上证指数
dataProxy.get_table('0000001')
```
####得到2016-5-5往前600个交易日的所有收盘数据
```python
#close、open、low、high、volume、vwap、returns
dataProxy.history('0000001',pd.Timestamp('2016-5-5'),600,'1d','close').values
```

####取得数据时顺带保存到本地
```python
dataProxy = LocalDataProxy(cache_path)
```

####读取本地缓存的数据
```python
dataProxy = LocalDataProxy(cache_path, True)
```

## 所有股票代码数据

####创建数据访问对象
```python
codeProxy = LocalCodeProxy()
```
####得到股票码和当前最新价格
```python
#得到第一只股票的股票码
codeProxy.get_codes()[0][0]
#得到第三只股票的当前价
codeProxy.get_codes()[3][1]
```

## 股票公司金融数据

####创建数据访问对象
```python
fundamentalProxy = LocalFundamentalProxy()
```
####得到基本金融数据
```python
#京东方A的金融数据
bi = fundamentalProxy.base_info('1000725')
#上市时间
bi.loc['timeToMarket']
"""
code,代码
name,名称
industry,所属行业
area,地区
pe,市盈率
outstanding,流通股本(亿)
totals,总股本(亿)
totalAssets,总资产(万)
liquidAssets,流动资产
fixedAssets,固定资产
reserved,公积金
reservedPerShare,每股公积金
esp,每股收益
bvps,每股净资
pb,市净率
timeToMarket,上市日期
undp,未分利润
perundp, 每股未分配
rev,收入同比(%)
profit,利润同比(%)
gpr,毛利率(%)
npr,净利润率(%)
holders,股东人数
"""
```

