# -*- coding: utf-8 -*-

"""
Created on

@author: W
"""

import unittest

from gunicorndemo.commons.util import get_current_time, convert_digital_precision



class TestReportUtil(unittest.TestCase):
    def setUp(self):
        self.data_date = '20201202'

    def tearDown(self):
        pass


    def test_get_current_time(self):
        print('* run test_get_current_time()')
        curr_time = get_current_time()
        print('curr_time=', curr_time)
        self.assertIsNotNone(curr_time)

    def test_convert_digital_precision(self):
        print('* run test_convert_digital_precision()')
        digital = convert_digital_precision(12.333)
        print('digital=',digital)
        self.assertIsNotNone(digital)



if __name__ == "__main__":
    unittest.main(verbosity=2)
