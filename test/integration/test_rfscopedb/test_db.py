"""Integration tests for the db module"""
import unittest
from datetime import datetime, timedelta

import numpy as np

from rfscopedb.db import WaveformDB
from rfscopedb.data_model import Scan
from rfscopedb.db import QueryFilter


class TestWaveformDB(unittest.TestCase):
    """Integration tests for the WaveformDB class"""
    db = WaveformDB(host='localhost', user='scope_rw', password='password')

    # def test_0scan_insert_query(self):
    #     dt1 = datetime.strptime("2020-01-01 01:23:45.123456", '%Y-%m-%d %H:%M:%S.%f')
    #     dt2 = datetime.strptime("2021-01-01 01:23:45.123456", '%Y-%m-%d %H:%M:%S.%f')
    #     dt3 = datetime.strptime("2022-01-01 01:23:45.123456", '%Y-%m-%d %H:%M:%S.%f')
    #     x1 = Scan(dt=dt1)
    #     x2 = Scan(dt=dt2)
    #     x3 = Scan(dt=dt3)
    #
    #     t = np.linspace(0, 1638.2, 8192) / 1000.0
    #     g1 = 0.5 * np.cos(t * 2 * np.pi * 6.103) + 1
    #     g2 = 0.5 * np.cos(t * 2 * np.pi * 12.206) + 1
    #     g3 = 0.5 * np.cos(t * 2 * np.pi * 18.309) + 1
    #
    #     cavity_data1 = {
    #         'Time': t,
    #         'GMES': g1,
    #     }
    #     cavity_data2 = {
    #         'Time': t,
    #         'GMES': g2,
    #     }
    #     cavity_data3 = {
    #         'Time': t,
    #         'GMES': g3,
    #         'PMES': g3,
    #     }
    #
    #     x1.add_cavity_data("c1", data=cavity_data1, sampling_rate=5000)
    #     x1.add_scan_data(float_data={'a': 1.0, "b": 2.0, "c": 100.0}, str_data={'c': 'on'})
    #     x2.add_cavity_data("c2", data=cavity_data2, sampling_rate=5000)
    #     x2.add_scan_data(float_data={'a': 2.0, "b": 3.0, "d": -10.0}, str_data={'c': 'off'})
    #     x3.add_cavity_data("c3", data=cavity_data3, sampling_rate=5000)
    #     x3.add_scan_data(float_data={'a': 1.1, "b": 2.1}, str_data={'c': 'on'})
    #
    #     x1.insert_data(TestDB.db.conn)
    #     x2.insert_data(TestDB.db.conn)
    #     x3.insert_data(TestDB.db.conn)

    def test_query_scans1(self):
        """Test querying all scans"""
        out = TestWaveformDB.db.query_scan_rows()
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
        """Test querying a scan using only the begin/end tags"""
        out = TestWaveformDB.db.query_scan_rows(begin=datetime.strptime("2020-06-01", "%Y-%m-%d"),
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
        """Test querying a scan using the begin/end tags and a single query filter"""
        # Test that date filters + single metadata filter work
        out = TestWaveformDB.db.query_scan_rows(begin=datetime.strptime("2020-06-01", "%Y-%m-%d"),
                                                end=datetime.strptime("2022-06-01", "%Y-%m-%d"),
                                                q_filter=QueryFilter(["c", ], ["=", ], ["off", ]))
        exp = [{'sid': 2,
                'scan_start_utc': datetime(2021, 1, 1, 6, 23, 45, 123456),
                's_c': 'off',
                'f_a': 2.0,
                'f_b': 3.0,
                'f_d': -10.0
                }]

        self.assertListEqual(exp, out)

    def test_query_scans4(self):
        """Test querying a scan using the begin/end tags and multiple query filters"""
        # Test that date filters + multiple metadata filters work
        out = TestWaveformDB.db.query_scan_rows(begin=datetime.strptime("2019-06-01", "%Y-%m-%d"),
                                                end=datetime.strptime("2023-06-01", "%Y-%m-%d"),
                                                q_filter=QueryFilter(["a", "b", "c"], ["<", "<", "="], [2, 3, "on"]))
        exp = [{'sid': 1,
                'scan_start_utc': datetime(2020, 1, 1, 6, 23, 45, 123456),
                's_c': 'on', 'f_a': 1.0, 'f_b': 2.0, 'f_c': 100.0},
               {'sid': 3,
                'scan_start_utc': datetime(2022, 1, 1, 6, 23, 45, 123456),
                's_c': 'on', 'f_a': 1.1, 'f_b': 2.1
                }]
        self.assertListEqual(exp, out)

    def test_query_scans5(self):
        """Test that date filters + multiple metadata filters work with name in both the float and string tables."""

        out = TestWaveformDB.db.query_scan_rows(begin=datetime.strptime("2019-06-01", "%Y-%m-%d"),
                                                end=datetime.strptime("2023-06-01", "%Y-%m-%d"),
                                                q_filter=QueryFilter(["a", "b", "c", "c"], ["<", "<", "=", "="],
                                                                     [2, 3, 100, "on"]))
        exp = [{'sid': 1,
                'scan_start_utc': datetime(2020, 1, 1, 6, 23, 45, 123456),
                's_c': 'on', 'f_a': 1.0, 'f_b': 2.0, 'f_c': 100.0
                }]
        self.assertListEqual(exp, out)

    def test_query_scans_no_match1(self):
        """Test behavior when no matches are found"""

        # Give a limited date range so no matches are returned
        out = TestWaveformDB.db.query_scan_rows(begin=datetime.strptime("2019-06-01", "%Y-%m-%d"),
                                                end=datetime.strptime("2019-06-02", "%Y-%m-%d"))
        exp = []
        self.assertListEqual(exp, out)

    def test_query_scans_no_match2(self):
        """Test that date filters + multiple metadata filters with duplicate names works when no matches are found."""

        # Give a limited date range so no matches are returned
        out = TestWaveformDB.db.query_scan_rows(begin=datetime.strptime("2019-06-01", "%Y-%m-%d"),
                                                end=datetime.strptime("2019-06-02", "%Y-%m-%d"),
                                                q_filter=QueryFilter(["a", "b", "c", "c"], ["<", "<", "=", "="],
                                                                     [2, 3, 100, "on"]))
        exp = []
        self.assertListEqual(exp, out)

    def test_insert_delete(self):
        """Test inserting and deleting scan data."""
        # Pick dates that don't overlap.  On the off chance the test fail to delete these, they shouldn't pollute the
        # other tests.
        scan_start1 = datetime.strptime("2000-01-01 01:23:45.123456", '%Y-%m-%d %H:%M:%S.%f')
        scan_start2 = datetime.strptime("2001-01-01 01:23:45.123456", '%Y-%m-%d %H:%M:%S.%f')
        scan_end1 = datetime.strptime("2000-01-01 01:23:55.123456", '%Y-%m-%d %H:%M:%S.%f')
        scan_end2 = datetime.strptime("2001-01-01 01:23:55.123456", '%Y-%m-%d %H:%M:%S.%f')
        x1 = Scan(start=scan_start1, end=scan_end1)
        x2 = Scan(start=scan_start2, end=scan_end2)

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

        x1.insert_data(TestWaveformDB.db.conn)
        x2.insert_data(TestWaveformDB.db.conn)

        scans = TestWaveformDB.db.query_scan_rows(begin=scan_start1, end=scan_start2)
        sids = [scan['sid'] for scan in scans]
        print("sids", sids)

        # self.assertEqual(len(sids), 2)

        # User the scope_owner connection to have permissions to delete
        db = WaveformDB(host='localhost', user="scope_owner", password="password")
        db.delete_scans(sids[0])
        db.delete_scans(sids[1])
        sids = [row['sid'] for row in db.query_scan_rows(begin=scan_start1, end=scan_start2)]
        self.assertEqual(0, len(sids))
        # The long running TestDB.db.conn object doesn't see these updates unless it is reset.
        TestWaveformDB.db.conn.cmd_reset_connection()

    # pylint: disable=no-value-for-parameter
    # noinspection PyArgumentList
    def test_query_waveform_data1(self):
        """Test expected failure mode for query_waveform data."""
        with self.assertRaises(TypeError):
            TestWaveformDB.db.query_waveform_data()

    def test_query_waveform_data2(self):
        """Test querying waveform data, single scan, all signals, all arrays"""
        exp = [
            {'wid': 1, 'sid': 1, 'cavity': 'c1', 'signal_name': 'GMES', 'sample_rate_hz': 5000.0, 'comment': None,
             'wadid': 1, 'name': 'raw', 'data': None},
            {'wid': 1, 'sid': 1, 'cavity': 'c1', 'signal_name': 'GMES', 'sample_rate_hz': 5000.0, 'comment': None,
             'wadid': 2, 'name': 'power_spectrum', 'data': None},
            {'wid': 1, 'sid': 1, 'cavity': 'c1', 'signal_name': 'GMES', 'sample_rate_hz': 5000.0, 'comment': None,
             'wadid': 3, 'name': 'frequencies', 'data': None}
        ]

        result = TestWaveformDB.db.query_waveform_data(sids=[1, ], signal_names=None, array_names=None)

        # Let's not check the data since it's a lot of entries.
        for waveform in result:
            waveform['data'] = None

        # pylint: disable=consider-using-enumerate
        for i in range(len(exp)):
            self.assertDictEqual(exp[i], result[i])

    def test_query_waveform_data3(self):
        """Test querying waveform data, single scan, single signal, all arrays"""
        exp = [
            {'wid': 1, 'sid': 1, 'cavity': 'c1', 'signal_name': 'GMES', 'sample_rate_hz': 5000.0, 'comment': None,
             'wadid': 1, 'name': 'raw', 'data': None},
            {'wid': 1, 'sid': 1, 'cavity': 'c1', 'signal_name': 'GMES', 'sample_rate_hz': 5000.0, 'comment': None,
             'wadid': 2, 'name': 'power_spectrum', 'data': None},
            {'wid': 1, 'sid': 1, 'cavity': 'c1', 'signal_name': 'GMES', 'sample_rate_hz': 5000.0, 'comment': None,
             'wadid': 3, 'name': 'frequencies', 'data': None}
        ]
        result = TestWaveformDB.db.query_waveform_data(sids=[1, ], signal_names=['GMES', ], array_names=None)

        # Let's not check the data since it's a lot of entries.
        for waveform in result:
            waveform['data'] = None

        # pylint: disable=consider-using-enumerate
        for i in range(len(exp)):
            self.assertDictEqual(exp[i], result[i])

    def test_query_waveform_data4(self):
        """Test querying waveform data, single scan, single signals, single array"""
        # Test the case where we specify each parameter and verify the data matches
        t = np.linspace(0, 1638.2, 8192) / 1000.0
        g = 0.5 * np.cos(t * 2 * np.pi * 6.103) + 1
        exp = [
            {'wid': 1, 'sid': 1, 'cavity': 'c1', 'signal_name': 'GMES', 'sample_rate_hz': 5000.0, 'comment': None,
             'wadid': 1, 'name': 'raw', 'data': g}
        ]
        result = TestWaveformDB.db.query_waveform_data(sids=[1, ], signal_names=['GMES', ], array_names=['raw', ])

        self.assertTrue(np.allclose(exp[0]['data'], result[0]['data']))
        exp[0]['data'] = None
        result[0]['data'] = None

        self.assertDictEqual(exp[0], result[0])

    def test_query_waveform_data5(self):
        """Test querying waveform data. multiple scans, multiple signals, multiple arrays"""
        # Test the case where we specify each parameter and verify the data matches
        exp = [
            {'wid': 1, 'sid': 1, 'cavity': 'c1', 'signal_name': 'GMES', 'sample_rate_hz': 5000.0, 'comment': None,
             'wadid': 1, 'name': 'raw', 'data': None},
            {'wid': 1, 'sid': 1, 'cavity': 'c1', 'signal_name': 'GMES', 'sample_rate_hz': 5000.0, 'comment': None,
             'wadid': 2, 'name': 'power_spectrum', 'data': None},
            {'wid': 2, 'sid': 2, 'cavity': 'c2', 'signal_name': 'GMES', 'sample_rate_hz': 5000.0, 'comment': None,
             'wadid': 4, 'name': 'raw', 'data': None},
            {'wid': 2, 'sid': 2, 'cavity': 'c2', 'signal_name': 'GMES', 'sample_rate_hz': 5000.0, 'comment': None,
             'wadid': 5, 'name': 'power_spectrum', 'data': None},
            {'wid': 3, 'sid': 3, 'cavity': 'c3', 'signal_name': 'GMES', 'sample_rate_hz': 5000.0, 'comment': None,
             'wadid': 7, 'name': 'raw', 'data': None},
            {'wid': 3, 'sid': 3, 'cavity': 'c3', 'signal_name': 'GMES', 'sample_rate_hz': 5000.0, 'comment': None,
             'wadid': 8, 'name': 'power_spectrum', 'data': None},
            {'wid': 4, 'sid': 3, 'cavity': 'c3', 'signal_name': 'PMES', 'sample_rate_hz': 5000.0, 'comment': None,
             'wadid': 10, 'name': 'raw', 'data': None},
            {'wid': 4, 'sid': 3, 'cavity': 'c3', 'signal_name': 'PMES', 'sample_rate_hz': 5000.0, 'comment': None,
             'wadid': 11, 'name': 'power_spectrum', 'data': None}
        ]
        result = TestWaveformDB.db.query_waveform_data(sids=[1, 2, 3], signal_names=['GMES', 'PMES'],
                                                       array_names=['raw', 'power_spectrum'])

        # Let's not check the data since it's a lot of entries.
        for waveform in result:
            waveform['data'] = None

        # pylint: disable=consider-using-enumerate
        for i in range(len(exp)):
            self.assertDictEqual(exp[i], result[i])

    # def test_insert_lots(self):
    #     """Test inserting a lot of scans.
    #
    #     This should only be run for manual review since it takes time, disk, and manual clean up (docker compose down)
    #     """
    #     # Pick dates that don't overlap.  On the off chance the test fail to delete these, they shouldn't pollute the
    #     # other tests.
    #     start = datetime.strptime("2000-01-01 01:23:45.123456", '%Y-%m-%d %H:%M:%S.%f')
    #     end = datetime.strptime("2000-01-01 01:23:55.123456", '%Y-%m-%d %H:%M:%S.%f')
    #
    #     t = np.linspace(0, 1638.2, 8192) / 1000.0
    #     g1 = 0.5 * np.cos(t * 2 * np.pi * 6.103) + 1
    #     p1 = np.cos(t * 2 * np.pi * 100.0) + np.cos(t * 2 * np.pi * 10.0)
    #
    #     cavity_data1 = {
    #         'Time': t,
    #         'GMES': g1,
    #         'PMES': p1,
    #         'IMES': g1 + 1,
    #         'QMES': p1 + 1,
    #         'TEST': p1 + 2,
    #     }
    #
    #     for i in range(1000):
    #         delta = timedelta(minutes=1*i)
    #         x = Scan(start=start + delta, end=end + delta)
    #         x.add_cavity_data("c1", data=cavity_data1, sampling_rate=5000)
    #         x.add_scan_data(float_data={'a': 1.0, "b": 2.0}, str_data={'c': 'on'})
    #         x.insert_data(TestWaveformDB.db.conn)
