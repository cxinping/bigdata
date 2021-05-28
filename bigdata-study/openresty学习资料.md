
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
得到以下结果
```
<!DOCTYPE html>
<html>
<head>
<meta content="text/html;charset=utf-8" http-equiv="Content-Type">
<meta content="utf-8" http-equiv="encoding">
<title>Welcome to OpenResty!</title>
<style>
    body {
        width: 35em;
        margin: 0 auto;
        font-family: Tahoma, Verdana, Arial, sans-serif;
    }
</style>
</head>
<body>
<h1>Welcome to OpenResty!</h1>
<p>If you see this page, the OpenResty web platform is successfully installed and
working. Further configuration is required.</p>

<p>For online documentation and support please refer to our
<a href="https://openresty.org/">openresty.org</a> site<br/>
Commercial support is available at
<a href="https://openresty.com/">openresty.com</a>.</p>
<p>We have articles on troubleshooting issues like <a href="https://blog.openresty.com/en/lua-cpu-flame-graph/?src=wb">high CPU usage</a> and
<a href="https://blog.openresty.com/en/how-or-alloc-mem/">large memory usage</a> on <a href="https://blog.openresty.com/">our official blog site</a>.
<p><em>Thank you for flying <a href="https://openresty.org/">OpenResty</a>.</em></p>
</body>
</html>
```
























