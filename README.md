# 股票数据访问

## 股票基本数据

### 创建数据访问对象

```python
dataProxy = LocalDataProxy()
```

### 得到所有历史数据
```python
dataProxy.get_table('399005')
```
### 得到2016-5-5往前600个交易日的所有收盘数据
```python
#close、open、low、high、volume、vwap、returns
dataProxy.history('000001',pd.Timestamp('2016-5-5'),600,'1d','close').values
```

### 取得数据时顺带保存到本地
```python
dataProxy = LocalDataProxy(cache_path)
```

### 读取本地缓存的数据
```python
dataProxy = LocalDataProxy(cache_path, True)
```

### 缓存csv文件补数据规则

```
注意，某只股票可能在某段时间停牌，需要补数据，在csv文件中补数据的方法如下：
1、第一天上市时，之前数据全部缺失，所以需要在上市首日的前一天补一条数据，高开低收都等于上市首日的开盘，其他数据都是0
2、中间某一段连续时间出现缺失，在缺失前的最后一个交易日后补一天的数据，高开低收等于上个交易日的收盘，其他数据都是0
这样，虽然缺失一段时间只补了一条数据，但其他数据只要和它一样就可以，方便未来的程序的补数据流程
```

## 所有股票代码数据

### 创建数据访问对象
```python
codeProxy = LocalCodeProxy()
```
### 得到股票码和当前最新价格
```python
#get all stock
codes = codeProxy.get_codes()
#enum all stock
for index, row in codes.iterrows():
    print(row["code"])
    print(row["price"])
```

