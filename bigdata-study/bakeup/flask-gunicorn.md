

### 项目打包



安装 wheel模块

>  pip install wheel



setup.py

```	
from setuptools import find_packages, setup


with open("README.md", "r") as fh:
    long_description = fh.read()

setup(
    name='flask-service',
    version='0.0.1',
    packages=find_packages(),
    description='My Flask Service',
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



编写 MANIFEST.in

```	
include demo/static/*.*
include demo/templates/*.*

```



同时发布源码包和 whl 二进制包

> python setup.py sdist bdist_wheel upload

打包项目
> python setup.py sdist bdist_wheel



使用gunicorn启动Flask App

启动 gunicorn服务

FlaskApp本身的web service是一个测试用的

在自己的PC上面启动Flask的时候会看到这样一句：WARNING: This is a development server. Do not use it in a production deployment.


> https://vsupalov.com/flask-web-server-in-production/

Flask Is Not Your Production Server
> https://vsupalov.com/flask-web-server-in-production/

在Linux下注册服务

> cd /etc/systemd/system/

## 配置文件

/etc/systemd/system/下的bmo-lre.service配置文件

```
[Unit]
Description=bmo-lre.service
After=syslog.target network.target

[Service]
Type=simple
WorkingDirectory='/etc/bmo-lre'
ExecStart=/usr/local/bin/gunicorn --config /etc/bmo-lre/gunicorn-config.py 'bmolre:init_app(config_object="config.development")'
Restart=on-failure

[Install]
WantedBy=multi-user.target
```

gunicorn-config.py
```
keyfile = '/etc/bmo-lre/bmo-lre.key'
ssl_version = 2
ciphers = 'TLSv1.2'

logconfig_dict = {
    'version': 1,
    'disable_existing_loggers': False,
    'formatters': {
        'standard': {
            'format': '%(asctime)s [%(levelname)s] %(name)s.%(module)s.%(funcName)s (%(lineno)d): %(message)s'
        },
        'access': {
            'format': '%(message)s'
        },
    },
    'handlers': {
        'console': {
            'formatter': 'standard',
            'class': 'logging.StreamHandler',
        },
        'access_file': {
            'formatter': 'access',
            'class': 'logging.handlers.TimedRotatingFileHandler',
            'filename': '/var/log/bmo-lre/bmo-lre.access.log',
            'when': 'D',
            'encoding': 'utf-8',
        },
        'error_file': {
            'formatter': 'standard',
            'class': 'logging.handlers.TimedRotatingFileHandler',
            'filename': '/var/log/bmo-lre/bmo-lre.error.log',
            'when': 'D',
            'encoding': 'utf-8',
        },
    },
    'loggers': {
        'gunicorn.access': {
            'handlers': ['console', 'access_file'],
            'level': 'DEBUG',
        },
        'gunicorn.error': {
            'handlers': ['console', 'error_file'],
            'level': 'DEBUG',
        },
    },
}

```



systemctl restart bmo-lre

systemctl restatus bmo-lre

Flask run启动Falsk APP
https://flask.palletsprojects.com/en/1.1.x/quickstart/

$ export FLASK_APP=hello.py
$ flask run

C:\path\to\app>set FLASK_APP=hello.py

Alternatively you can use python -m flask:

$ export FLASK_APP=hello.py
$ python -m flask run
 * Running on http://127.0.0.1:5000/

## gunicorn

https://gunicorn.org/


> pip3 install gunicorn 

gunicorn安装成功后，使用 pip3 show gunicorn命令查看安装的gunicorn的模块信息

> pip3 show gunicorn



gunicorn -w 4 "bmolre:init_app(config_object='config.development')"





在/soft/demo下新建 gunicorn_config.py

```	
bind = "0.0.0.0:8000"
workers = 4
```

/usr/local/python/bin/gunicorn -c /soft/demo/gunicorn.config -w 4  'gunicorndemo:init_app(config_object="config.development")'

/etc/systemd/system/下的 gunicorndemo.service配置文件，添加以下内容

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

> systemctl restart gunicorndemo

> systemctl restatus gunicorndemo


# 参考资料

How To Serve Flask Applications with Gunicorn and Nginx on Ubuntu 18.04

> https://www.digitalocean.com/community/tutorials/how-to-serve-flask-applications-with-gunicorn-and-nginx-on-ubuntu-18-04


MANIFEST.in ignored on “python setup.py install” - no data files installed?

> https://stackoverflow.com/questions/3596979/manifest-in-ignored-on-python-setup-py-install-no-data-files-installed/3597263#3597263





