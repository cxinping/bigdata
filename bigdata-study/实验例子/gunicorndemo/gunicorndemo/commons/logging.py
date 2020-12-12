# -*- coding: utf-8 -*-


'''
Created on 2020-11-06

@author: Wang Shuo
'''

import logging.config
import os

from gunicorndemo import __name__ as app_name
from gunicorndemo.commons.constant import CFG_DEBUG

from flask import current_app as app

DEFAULT_CONFIG = {

    'version': 1,
    'disable_existing_loggers': False,
    'formatters': {
        'standard': {
            'format': '%(asctime)s [%(levelname)s] %(name)s.%(module)s.%(funcName)s (%(lineno)d): %(message)s'
        },
    },
    'handlers': {
        'console': {
            'level': 'DEBUG',
            'formatter': 'standard',
            'class': 'logging.StreamHandler',
        },
        'default_file': {
            'level': 'DEBUG',
            'formatter': 'standard',
            'class': 'logging.handlers.TimedRotatingFileHandler',
            #'filename': '/var/log/gunicorndemo.default.log',
            'filename': 'D:/gunicorndemo.default.log',
            'when': 'D',
            'encoding': 'utf-8',
        },
    },
    'loggers': {
        '': {
            'handlers': ['console', 'default_file'],
            'level': 'INFO',
        },
    }
}


def log_entry_exit(func):
    '''
    Decorator for logging entry and exit
    '''

    def log_entry_exit(*args, **kwargs):
        logger = get_logger()

        logger.debug(func.__module__ + '.' + func.__qualname__ + ' arguments:' + str(args) + ', ' + str(kwargs))

        return_value = func(*args, **kwargs)

        logger.debug(func.__module__ + '.' + func.__qualname__ + ' return: ' + str(return_value))

        return return_value

    return log_entry_exit


def get_logger():
    if app:
        return app.logger
    else:
        logging.config.dictConfig(DEFAULT_CONFIG)
        logger = logging.getLogger(app_name)

        if CFG_DEBUG in os.environ and os.environ[CFG_DEBUG] == 'True':
            logger.setLevel(logging.DEBUG)

        return logger
