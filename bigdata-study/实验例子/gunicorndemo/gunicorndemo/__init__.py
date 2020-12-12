# -*- coding: utf-8 -*-

'''
Created on

@author:
'''

import os
import logging as python_logging
from flask import Flask
from .views.report import report_bp
from .commons.constant import CFG_DEBUG, LRE_REPORT_DB_URL
from .commons.logging import get_logger, log_entry_exit

from .exts import db


GUNICORN_ERROR_LOGGER_NAME = 'gunicorn.error'

def create_app(config_object='config.default', config_map=None):
    # Create and configure the app
    app = Flask(__name__)

    app.logger.debug('loggers=' + str(python_logging.Logger.manager.loggerDict))

    # Use gunicorn log handler if this application is initiated by gunicorn
    if GUNICORN_ERROR_LOGGER_NAME in python_logging.Logger.manager.loggerDict:
        gunicorn_logger = python_logging.getLogger(GUNICORN_ERROR_LOGGER_NAME)

        app.logger.debug('gunicorn_logger.level=' + str(gunicorn_logger.level))
        app.logger.debug('gunicorn_logger.handlers=' + str(gunicorn_logger.handlers))

        app.logger.handlers = gunicorn_logger.handlers
        app.logger.setLevel(gunicorn_logger.level)

    # configure blue print
    app.register_blueprint(report_bp, url_prefix='/report')

    # Overwrite default config by input parameter
    app.config.from_object(config_object)

    if config_map is not None:
        # load the test config if passed in
        app.config.from_mapping(config_map)

    app.logger.debug('app.config=%s', app.config)

    # Put debug flag into os environment for reference by logger, etc.
    os.environ[CFG_DEBUG] = str(app.config[CFG_DEBUG])

    # * Put lre report db url into os environment fore reference by Pandas, etc.
    os.environ[LRE_REPORT_DB_URL] = str(app.config[LRE_REPORT_DB_URL])

    # Create logger after app initiated to avoid recursive dependency
    app.logger.debug('os.environ=%s', os.environ)

    # a simple page that says hello
    @app.route('/hello')
    def hello():
        return 'Hello, World!'

    return app

def init_app(config_object='config.development'):
    app = create_app(config_object)
    db.init_app(app)
    return app
