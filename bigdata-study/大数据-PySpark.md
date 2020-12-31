
学习资料

> https://www.tutorialspoint.com/pyspark/pyspark_environment_setup.htm

> https://realpython.com/pyspark-intro/

Python 3.6

Python 3.7.3

PyCharm+PySpark远程调试的环境配置的方法

https://www.jianshu.com/p/06b40a77b6ee

https://www.jianshu.com/p/8369f4312b23

Window下安装Spark
https://yxnchen.github.io/technique/Windows%E5%B9%B3%E5%8F%B0%E4%B8%8B%E5%8D%95%E6%9C%BASpark%E7%8E%AF%E5%A2%83%E6%90%AD%E5%BB%BA/#Python

PySpark Course
> https://intellipaat.com/blog/tutorial/spark-tutorial/pyspark-tutorial/


> https://www.edureka.co/blog/pyspark-programming/


> http://codingdict.com/article/8883

PySpark 学习资料

> https://www.tutorialspoint.com/pyspark/index.htm



> pip install --upgrade pyspark

That will update the package, if one is available. If this doesn't help then you might have to downgrade to a compatible version of python.

## 使用数组创建RDD


```
from pyspark import SparkContext
import os

os.environ['HADOOP_HOME'] = r'C:\bigdata\hadoop'
os.environ['SPARK_HOME'] = r'C:\bigdata\spark'
os.environ['PYTHON_HOME'] = r'C:\mysoft\python36\python.exe'
os.environ['PYSPARK_PYTHON'] = r'C:\mysoft\python36\python.exe'

sc = SparkContext()
rdd = sc.parallelize([1, 2, 3, 4, 5, 6], 3)
print(rdd)
print(rdd.getNumPartitions())
print(rdd.count())

```

## 转换操作

### 使用map(func)转换数据

```
from pyspark import SparkContext
import os

os.environ['HADOOP_HOME'] = r'C:\bigdata\hadoop'
os.environ['SPARK_HOME'] = r'C:\bigdata\spark'
os.environ['PYTHON_HOME'] = r'C:\mysoft\python36\python.exe'
os.environ['PYSPARK_PYTHON'] = r'C:\mysoft\python36\python.exe'

sc = SparkContext()
rdd1 = sc.parallelize([0, 1, 2, 3, 4, 6])
rdd2 = rdd1.map(lambda x: x * 2)
local_data = rdd2.collect()
[print("当前元素是: ", item ) for item in local_data]

```

### PySpark学习笔记

> https://gitee.com/cucy/pyspark_project




