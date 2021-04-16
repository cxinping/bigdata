
# 装饰器

装饰器用于在源码中“标记”函数，以增强函数的行为。

我们先来看下面的例子，现有一个求和函数add，现在要求统计函数执行的时长

```
def add(a, b):
    print(a+b)
```

常用做法
```
def add(a, b):
    start = time.time()
    print(a + b)
    time.sleep(2)#模拟耗时操作
    long = time.time() - start
    print(f'共耗时{long}秒。')
```



