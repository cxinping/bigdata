# -*- coding: utf-8 -*-

"""
Created on 2020-11-06

@author: Wang Shuo
"""

import os

# Configure debug flag
CFG_DEBUG = 'DEBUG'
# Jwt access token
ACCESS_TOKEN = 'access_token'

# 报表模板保存地址
COMMONS_PATH = os.path.dirname(__file__)
BMOLRE_PATH = os.path.dirname(COMMONS_PATH)
REPORT_TEMPLATE_PATH = os.path.join(BMOLRE_PATH, 'templates')
CREATED_REPORT_PATH = 'C:/codes2/bmo-lre/bmolre/templates/report'

G14_QUARTERLY_REPORT_TEMPLATE = 'g14_quarterly_report_template.xlsx'
LRE_DAILY_REPORT_TEMPLATE = 'lre_daily_report_template.xlsx'

# LRE DB flag
LRE_REPORT_DB_URL = 'SQLALCHEMY_DATABASE_URI'


