# -*- coding: utf-8 -*-


"""
Created on 2021-08-05

@author: Wang Shuo
"""

import logging.config
import os
from flask import current_app as app

import logging

CFG_DEBUG = 'DEBUG'

DEFAULT_CONFIG = {

    'version': 1,
    'disable_existing_loggers': False,
    'formatters': {
        'standard': {
            'format': '%(asctime)s [%(levelname)s] %(name)s.%(funcName)s (%(lineno)d): %(message)s'
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
            'filename': '/report/report.app.default.log',
            'when': 'D',
            'encoding': 'utf-8',
        },
        'error_file': {
            'level': 'ERROR',
            'formatter': 'standard',
            'class': 'logging.handlers.TimedRotatingFileHandler',
            'filename': '/report/report.app.error.log',
            'when': 'D',
            'encoding': 'utf-8',
        },
    },

    'root': {
        'handlers': ['console', 'default_file', 'error_file'],
        'level': 'DEBUG',
    },
}


def log_entry_exit(func):
    '''
    Decorator for logging entry and exit
    '''
    def log_entry_exit(*args, **kwargs):

        logger = get_logger(func.__module__ + '.' + func.__qualname__)
        
        logger.debug('entry arguments:' + str(args) + ', ' + str(kwargs))

        return_values = func(*args, **kwargs)

        logger.debug('exit return_values: ' + str(return_values))

        return return_values

    return log_entry_exit


def get_logger(name=None):
    
    logging.config.dictConfig(DEFAULT_CONFIG)

    if name:
        logger = logging.getLogger(name)
    elif app:
        logger = app.logger
    else:
        logger = logging.getLogger()
    
    if CFG_DEBUG in os.environ and os.environ[CFG_DEBUG] == 'False':
        logger.setLevel(logging.INFO)

    return logger
