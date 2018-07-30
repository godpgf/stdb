# 股票数据访问

## 股票基本数据

####创建数据访问对象

```python
dataProxy = LocalDataProxy()
```

####得到所有历史数据
```python
#第一个0表示上证指数
dataProxy.get_table('1399005')
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
#get all stock
codes = codeProxy.get_codes()
#enum all stock
for index, row in codes.iterrows():
    print(row["code"])
    print(row["price"])
```

