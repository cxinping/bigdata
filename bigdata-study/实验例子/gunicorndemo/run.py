# -*- coding: utf-8 -*-

from flask import Flask

from gunicorndemo import init_app

app = init_app(config_object='config.development')

if __name__ == "__main__":
    app.run(host='0.0.0.0',port=8888)



