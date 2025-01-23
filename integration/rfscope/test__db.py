import unittest
from datetime import datetime

import numpy as np

from rfscope.db import Scan, WaveformDB


class TestDB(unittest.TestCase):
    db = WaveformDB(host='localhost', user='scope_rw', password='password')

    @unittest.skip('skip - has data at start for now')
    def test_0scan_insert_query(self):
        dt1 = datetime.strptime("2020-01-01 01:23:45.123456", '%Y-%m-%d %H:%M:%S.%f')
        dt2 = datetime.strptime("2021-01-01 01:23:45.123456", '%Y-%m-%d %H:%M:%S.%f')
        dt3 = datetime.strptime("2022-01-01 01:23:45.123456", '%Y-%m-%d %H:%M:%S.%f')
        x1 = Scan(dt=dt1)
        x2 = Scan(dt=dt2)
        x3 = Scan(dt=dt3)

        t = np.linspace(0, 1638.2, 8192) / 1000.0
        g1 = 0.5 * np.cos(t * 2 * np.pi * 6.103) + 1
        g2 = 0.5 * np.cos(t * 2 * np.pi * 12.206) + 1
        g3 = 0.5 * np.cos(t * 2 * np.pi * 18.309) + 1

        cavity_data1 = {
            'Time': t,
            'GMES': g1,
        }
        cavity_data2 = {
            'Time': t,
            'GMES': g2,
        }
        cavity_data3 = {
            'Time': t,
            'GMES': g3,
        }

        x1.add_cavity_data("c1", data=cavity_data1, sampling_rate=5000)
        x1.add_scan_data(float_data={'a': 1.0, "b": 2.0, "c": 100.0}, str_data={'c': 'on'})
        x2.add_cavity_data("c2", data=cavity_data2, sampling_rate=5000)
        x2.add_scan_data(float_data={'a': 2.0, "b": 3.0, "d": -10.0}, str_data={'c': 'off'})
        x3.add_cavity_data("c3", data=cavity_data3, sampling_rate=5000)
        x3.add_scan_data(float_data={'a': 1.1, "b": 2.1}, str_data={'c': 'on'})

        x1.insert_data(TestDB.db.conn)
        x2.insert_data(TestDB.db.conn)
        x3.insert_data(TestDB.db.conn)

    def test_1query_single(self):
        TestDB.db.test_query_single()

    def test_1query_multi(self):
        TestDB.db.test_query_multi()
