# -*- coding: utf-8 -*-
"""
Created on 2021-08-03

@author: WangShuo

cd /you_filed_algos/app

PYTHONIOENCODING=utf-8 /root/anaconda3/bin/python /you_filed_algos/app/run.py

PYTHONIOENCODING=utf-8 nohup /root/anaconda3/bin/python /you_filed_algos/app/run.py &


"""


from report import init_app
app = init_app(config_object='config.default')


if __name__ == '__main__':
    # from gevent import monkey
    # monkey.patch_all()

    app.run(host='0.0.0.0', port=8004)

