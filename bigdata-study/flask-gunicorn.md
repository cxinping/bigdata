
使用gunicorn启动Flask App

启动 gunicorn服务

FlaskApp本身的web service是一个测试用的

在自己的PC上面启动Flask的时候会看到这样一句：WARNING: This is a development server. Do not use it in a production deployment.


> https://vsupalov.com/flask-web-server-in-production/

Flask Is Not Your Production Server
> https://vsupalov.com/flask-web-server-in-production/

在Linux下注册服务

> cd /etc/systemd/system/


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













