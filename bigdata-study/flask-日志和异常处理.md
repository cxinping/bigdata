https://www.jianshu.com/p/9f08c72a148b


```
a = [1, 2, 3]
try:
    print a[3]
except Exception, e:
    logging.exception(e)
```

https://segmentfault.com/a/1190000018087099?utm_source=tag-newest


### Flask中实现统一异常处理
https://my.oschina.net/OHC1U9jZt/blog/3027727

### flask项目统一捕获异常并自定义异常信息
https://www.lagou.com/lgeduarticle/82443.html


### 自定义异常类

```
class CustomError(Exception):
    def __init__(self,ErrorInfo):
        super().__init__(self) #初始化父类
        self.errorinfo=ErrorInfo
    def __str__(self):
        return self.errorinfo
    
if __name__ == '__main__':
    try:
        raise CustomError('客户异常')
    except CustomError as e:
        print(e)
```
