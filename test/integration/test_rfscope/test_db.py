import unittest
from datetime import datetime

import numpy as np

from rfscopedb.db import WaveformDB
from rfscopedb.data_model import Scan


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
            'PMES': g3,
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

    # def test_1query_multi(self):
    #     TestDB.db.test_query_multi()

    def test_query_scans1(self):
        out = TestDB.db.query_scan_rows()
        exp = [{'sid': 1,
                'scan_start_utc': datetime(2020, 1, 1, 6, 23, 45, 123456),
                's_c': 'on', 'f_a': 1.0, 'f_b': 2.0, 'f_c': 100.0},
               {'sid': 2,
                'scan_start_utc': datetime(2021, 1, 1, 6, 23, 45, 123456),
                's_c': 'off', 'f_a': 2.0, 'f_b': 3.0, 'f_d': -10.0},
               {'sid': 3,
                'scan_start_utc': datetime(2022, 1, 1, 6, 23, 45, 123456),
                's_c': 'on', 'f_a': 1.1, 'f_b': 2.1
                }]
        self.assertListEqual(exp, out)

    def test_query_scans2(self):
        out = TestDB.db.query_scan_rows(begin=datetime.strptime("2020-06-01", "%Y-%m-%d"),
                                        end=datetime.strptime("2021-06-01", "%Y-%m-%d"))
        exp = [{'sid': 2,
                'scan_start_utc': datetime(2021, 1, 1, 6, 23, 45, 123456),
                's_c': 'off',
                'f_a': 2.0,
                'f_b': 3.0,
                'f_d': -10.0
                }]
        self.assertListEqual(exp, out)

    def test_query_scans3(self):
        # Test that date filters + single metadata filter work
        out = TestDB.db.query_scan_rows(begin=datetime.strptime("2020-06-01", "%Y-%m-%d"),
                                        end=datetime.strptime("2022-06-01", "%Y-%m-%d"),
                                        filter_params=["c", ], filter_ops=["=", ], filter_values=["off", ])
        exp = [{'sid': 2,
                'scan_start_utc': datetime(2021, 1, 1, 6, 23, 45, 123456),
                's_c': 'off',
                'f_a': 2.0,
                'f_b': 3.0,
                'f_d': -10.0
                }]

        self.assertListEqual(exp, out)

    def test_query_scans4(self):
        # Test that date filters + multiple metadata filters work
        out = TestDB.db.query_scan_rows(begin=datetime.strptime("2019-06-01", "%Y-%m-%d"),
                                        end=datetime.strptime("2023-06-01", "%Y-%m-%d"),
                                        filter_params=["a", "b", "c"], filter_ops=["<", "<", "="],
                                        filter_values=[2, 3, "on"])
        exp = [{'sid': 1,
                'scan_start_utc': datetime(2020, 1, 1, 6, 23, 45, 123456),
                's_c': 'on', 'f_a': 1.0, 'f_b': 2.0, 'f_c': 100.0},
               {'sid': 3,
                'scan_start_utc': datetime(2022, 1, 1, 6, 23, 45, 123456),
                's_c': 'on', 'f_a': 1.1, 'f_b': 2.1
                }]
        self.assertListEqual(exp, out)

    def test_query_scans5(self):
        # Test that date filters + multiple metadata filters work even when the same name exists in both the float
        # and string metadata tables ("c").

        out = TestDB.db.query_scan_rows(begin=datetime.strptime("2019-06-01", "%Y-%m-%d"),
                                        end=datetime.strptime("2023-06-01", "%Y-%m-%d"),
                                        filter_params=["a", "b", "c", "c"], filter_ops=["<", "<", "=", "="],
                                        filter_values=[2, 3, 100, "on"])
        exp = [{'sid': 1,
                'scan_start_utc': datetime(2020, 1, 1, 6, 23, 45, 123456),
                's_c': 'on', 'f_a': 1.0, 'f_b': 2.0, 'f_c': 100.0
              }]
        self.assertListEqual(exp,out)

    def test_insert_delete(self):
        # Pick dates that don't overlap.  On the off chance the test fail to delete these, they shouldn't pollute the
        # other tests.
        dt1 = datetime.strptime("2000-01-01 01:23:45.123456", '%Y-%m-%d %H:%M:%S.%f')
        dt2 = datetime.strptime("2001-01-01 01:23:45.123456", '%Y-%m-%d %H:%M:%S.%f')
        x1 = Scan(dt=dt1)
        x2 = Scan(dt=dt2)

        t = np.linspace(0, 1638.2, 8192) / 1000.0
        g1 = 0.5 * np.cos(t * 2 * np.pi * 6.103) + 1
        g2 = 0.5 * np.cos(t * 2 * np.pi * 12.206) + 1

        p1 = np.cos(t * 2 * np.pi * 100.0) + np.cos(t * 2 * np.pi * 10.0)
        p2 = np.cos(t * 2 * np.pi * 300.0) + np.cos(t * 2 * np.pi * 20.0)

        cavity_data1 = {
            'Time': t,
            'GMES': g1,
            'PMES': p1,
        }
        cavity_data2 = {
            'Time': t,
            'GMES': g2,
            'PMES': p2,
        }

        x1.add_cavity_data("c1", data=cavity_data1, sampling_rate=5000)
        x1.add_scan_data(float_data={'a': 1.0, "b": 2.0}, str_data={'c': 'on'})
        x2.add_cavity_data("c2", data=cavity_data2, sampling_rate=5000)
        x2.add_scan_data(float_data={'a': 2.0, "b": 3.0, "d": -10.0}, str_data={'c': 'off'})

        x1.insert_data(TestDB.db.conn)
        x2.insert_data(TestDB.db.conn)

        scans = TestDB.db.query_scan_rows(begin=dt1, end=dt2)
        sids = [scan['sid'] for scan in scans]
        print("sids", sids)

        # self.assertEqual(len(sids), 2)

        # User the scope_owner connection to have permissions to delete
        db = WaveformDB(host='localhost', user="scope_owner", password="password")
        db.delete_scans(sids[0])
        db.delete_scans(sids[1])
        sids = [row['sid'] for row in db.query_scan_rows(begin=dt1, end=dt2)]
        self.assertEqual(0, len(sids))
        # The long running TestDB.db.conn object doesn't see these updates unless it is reset.
        TestDB.db.conn.reset()
