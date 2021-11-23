# -*- coding: utf-8 -*-
import os
from pathlib import Path

BASE_DIR = Path(__file__).resolve().parent.parent
CONFIG_DIR = os.path.join(BASE_DIR , 'config')

CONN_TYPE = 'test'  # test ,  prod



