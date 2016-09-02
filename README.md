# 推荐算法实验
* 使用SparkML中的ALS进行训练

##主要思路
* 数据集中有脏数据，先清理一遍
* 自己查看了一下收听次数的分布情况（见tips.pdf），自己将星级和次数挂钩（其实可以用ALS的隐式训练方法，而不必自己定义星级）
* 然后使用ALS进行训练，反复迭代出一个比较好的模型
* 然后测试结果

##主要技术
* 主要使用了`SparkML` 、`SparkSql` 和`SparkCore`库

##关于
* 使用的数据集 [Music Listening Dataset](http://audioscrobbler.com)
* [click](http://www-etud.iro.umontreal.ca/~bergstrj/audioscrobbler_data.html) 下载数据集
* AudioscrobblerALS.scala 是一个不知道从哪里弄来的参考，我自己做完实验了才发现的。


