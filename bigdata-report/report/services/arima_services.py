# -*- coding: utf-8 -*-

from gevent import monkey;

monkey.patch_all(thread=False)

import gevent
from gevent.pool import Pool
from pyramid.arima import auto_arima
from report.commons.logging import get_logger

import sys
sys.path.append('/you_filed_algos/app')

log = get_logger(__name__)

def demo1():
    pass


if __name__ == '__main__':
    demo1()

    print('--- ok ---')
