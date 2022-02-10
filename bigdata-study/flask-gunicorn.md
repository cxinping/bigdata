# Gunicorn

Gunicorn ‘Green Unicorn’ 是一个 UNIX 下的 WSGI HTTP 服务器，它是一个 移植自 Ruby 的 Unicorn 项目的 pre-fork worker 模型。它既支持 eventlet ， 也支持 greenlet

在管理 worker 上，使用了 pre-fork 模型，即一个 master 进程管理多个 worker 进程，所有请求和响应均由 Worker 处理。Master 进程是一个简单的 loop, 监听 worker 不同进程信号并且作出响应。比如接受到 TTIN 提升 worker 数量，TTOU 降低运行 Worker 数量。如果 worker 挂了，发出 CHLD, 则重启失败的 worker, 同步的 Worker 一次处理一个请求。


## 安装Gunicorn
目前Gunicorn只能运行在Linux环境中，不支持windows平台

> pip3 install gunicorn

gunicorn安装成功后，使用 pip3 show gunicorn命令查看安装的gunicorn的模块信息
> pip3 status gunicorn


# Flask项目打包和注册服务



### 安装wheel模块

安装 wheel模块

>  pip3 install wheel



> pip install -i https://pypi.tuna.tsinghua.edu.cn/simple wheel



使用pycharm新建一个Flask项目 gunicorndemo，项目结构如下图所示：

![gunicorn1](.\images\gunicorn1.jpg)




项目的setup.py的内容如下

```	
from setuptools import find_packages, setup


with open("README.md", "r") as fh:
    long_description = fh.read()

setup(
    name='gunicorndemo',
    version='0.0.1',
    packages=find_packages(),
    description='gunicorndemo Flask Service',
    long_description=long_description,
    long_description_content_type="text/markdown",
    classifiers=[
        'Programming Language :: Python :: 3',
        'Operating System :: OS Independent',
    ],
    include_package_data=True,
    zip_safe=False,
    install_requires=[
        'flask'
    ],
    python_requires='>=3.6',
)
```

项目的MANIFEST.in的内容如下

```	
include gunicorndemo/*/*.*
include gunicorndemo/static/css/*.css
include gunicorndemo/static/images/*.*
include gunicorndemo/static/js/*.js
include config/*.py
include tests/*.py
```

在项目目录下使用以下命令打包项目

> python setup.py sdist bdist_wheel



在 %/gunicorndemo/dist新生成 gunicorndemo-0.0.1-py3-none-any.whl 和 gunicorndemo-0.0.1.tar.gz

![gunicorn2](.\images\gunicorn2.jpg)



### 在Linux上安装Flask App应用



把gunicorndemo-0.0.1-py3-none-any.whl 上传到Linux服务器 192.168.11.10，需要在LInux服务器 192.168.11.10先安装好Python3的环境，还需要安装Flask app依赖的第三方模块。

```
pip3 install flask 

pip3 install flask-sqlalchemy
```



#### 方法一  

然后在Linux上安装Flask App应用

> pip3 install gunicorndemo-0.0.1-py3-none-any.whl



安装好Flask App后在 服务器 192.168.11.10的/usr/local/python/lib/python3.9/site-packages/gunicorndemo目录会看到上传的Flask APP源码,如下图所示。

![gunicorn3](.\images\gunicorn3.jpg)



使用gunicorn启动Flask App，Flask App本身的web service是可以开发使用，在生产环境需要启动 gunicorn服务

> gunicorn -w 4 "bmolre:init_app(config_object='config.development')"



启动gunicorn服务，使用了4个线程，调用了Flask APP了，这种方式关闭Linux客户端就关闭了gunicorn服务



#### 方法二

在Linux下注册服务, 使用Systemd确保引导时启动Gunicorn

在/etc/systemd/system/下创建gunicorndemo.service配置文件

```
[Unit]
Description=gunicorn.service
After=syslog.target network.target

[Service]
Type=simple
# 项目的工作目录，manage.py所在的目录
WorkingDirectory='/usr/local/python/lib/python3.9/site-packages/gunicorndemo'
ExecStart= /usr/local/python/bin/gunicorn --config /soft/demo/gunicorn_config.py 'gunicorndemo:init_app(config_object="config.development")'
# #指明在进程崩溃时自动重启进程
Restart=on-failure

[Install]
# 让systemd在引导时启动这个服务
WantedBy=multi-user.target
```



/soft/demo下创建gunicorn-config.py

```
bind = "0.0.0.0:8000"
workers = 4
```



启动gunicorndemo服务

> systemctl restart gunicorndemo

查看gunicorndemo服务

> systemctl status gunicorndemo

```
[root@localhost system]# systemctl status gunicorndemo
● gunicorndemo.service - gunicorn.service
   Loaded: loaded (/etc/systemd/system/gunicorndemo.service; disabled; vendor preset: disabled)
   Active: active (running) since 六 2020-12-12 17:57:54 CST; 48min ago
 Main PID: 78204 (gunicorn)
    Tasks: 5
   CGroup: /system.slice/gunicorndemo.service
           ├─78204 /usr/local/python/bin/python3.9 /usr/local/python/bin/gunicorn --config /soft/demo/gunicorn_config.py gunicorndemo:init_app(config_object="config.development")
           ├─78205 /usr/local/python/bin/python3.9 /usr/local/python/bin/gunicorn --config /soft/demo/gunicorn_config.py gunicorndemo:init_app(config_object="config.development")
           ├─78206 /usr/local/python/bin/python3.9 /usr/local/python/bin/gunicorn --config /soft/demo/gunicorn_config.py gunicorndemo:init_app(config_object="config.development")
           ├─78207 /usr/local/python/bin/python3.9 /usr/local/python/bin/gunicorn --config /soft/demo/gunicorn_config.py gunicorndemo:init_app(config_object="config.development")
           └─78208 /usr/local/python/bin/python3.9 /usr/local/python/bin/gunicorn --config /soft/demo/gunicorn_config.py gunicorndemo:init_app(config_object="config.development")

12月 12 17:57:54 localhost.localdomain systemd[1]: Started gunicorn.service.
12月 12 17:57:54 localhost.localdomain gunicorn[78204]: [2020-12-12 17:57:54 +0800] [78204] [INFO] Starting gunicorn 20.0.4
12月 12 17:57:54 localhost.localdomain gunicorn[78204]: [2020-12-12 17:57:54 +0800] [78204] [INFO] Listening at: http://0.0.0.0:8000 (78204)
12月 12 17:57:54 localhost.localdomain gunicorn[78204]: [2020-12-12 17:57:54 +0800] [78204] [INFO] Using worker: sync
12月 12 17:57:54 localhost.localdomain gunicorn[78204]: [2020-12-12 17:57:54 +0800] [78205] [INFO] Booting worker with pid: 78205
12月 12 17:57:54 localhost.localdomain gunicorn[78204]: [2020-12-12 17:57:54 +0800] [78206] [INFO] Booting worker with pid: 78206
12月 12 17:57:54 localhost.localdomain gunicorn[78204]: [2020-12-12 17:57:54 +0800] [78207] [INFO] Booting worker with pid: 78207
12月 12 17:57:54 localhost.localdomain gunicorn[78204]: [2020-12-12 17:57:54 +0800] [78208] [INFO] Booting worker with pid: 78208
```

可以看到gunicorndemo服务已经启动了，访问以下网址

```
http://192.168.11.10:8000/report/hello2

http://192.168.11.10:8000/report/line
```



# 参考资料

How To Serve Flask Applications with Gunicorn and Nginx on Ubuntu 18.04

> https://www.digitalocean.com/community/tutorials/how-to-serve-flask-applications-with-gunicorn-and-nginx-on-ubuntu-18-04


MANIFEST.in ignored on “python setup.py install” - no data files installed?

> https://stackoverflow.com/questions/3596979/manifest-in-ignored-on-python-setup-py-install-no-data-files-installed/3597263#3597263


Flask Is Not Your Production Server
> https://vsupalov.com/flask-web-server-in-production/




