# -*- coding: utf-8 -*-
"""
Created on 2021-08-03

@author: WangShuo

首先登录到服务器 10.5.138.11, 然后进入到容器 report
docker exec -it report /bin/bash

进入report容器，进入到指定目录下
cd /you_filed_algos/app

启动 report 项目
PYTHONIOENCODING=utf-8 /root/anaconda3/bin/python /you_filed_algos/app/run.py

后台启动 report 项目
PYTHONIOENCODING=utf-8 nohup /root/anaconda3/bin/python /you_filed_algos/app/run.py &



"""


from report import init_app
app = init_app(config_object='config.default')


if __name__ == '__main__':
    # from gevent import monkey
    # monkey.patch_all()

    app.run(host='0.0.0.0', port=8004)
