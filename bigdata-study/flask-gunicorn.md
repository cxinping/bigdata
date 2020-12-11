
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

> pip install gunicorn flask













