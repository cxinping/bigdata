
官网
> https://openresty.org/en/download.html

参考资料
```
https://www.jianshu.com/p/a20ff673d483
```


# 源码安装 OpenResty

选择合适的版本下载
```
wget https://openresty.org/download/openresty-1.19.3.1.tar.gz
```

安装依赖
```
yum install pcre-devel openssl-devel gcc curl -y
```

编译
```
tar -zxvf openresty-1.19.3.1.tar.gz && cd openresty-1.19.3.1

cd openresty-1.19.3.1

./configure

make && make install
```

检查
```
/usr/local/openresty/nginx/sbin/nginx -c /usr/local/openresty/nginx/conf/nginx.conf
```
然后输入
```
curl 127.0.0.1:80
```

























