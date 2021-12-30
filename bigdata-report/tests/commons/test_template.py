import uuid
import time, os, re
from datetime import datetime, timezone, timedelta
import psutil
#from report.commons.settings import CONN_TYPE
from report.commons.logging import get_logger
# from report.commons.connect_kudu2 import prod_execute_sql

log = get_logger(__name__)
