# -*- coding: utf-8 -*-
"""
Created on 2021-08-03

@author: WangShuo

"""


from report import init_app
app = init_app(config_object='config.default')


if __name__ == '__main__':
    app.run(host='0.0.0.0', port=8004)