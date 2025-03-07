"""Integration tests for the data_model module"""
import unittest
from datetime import datetime

import numpy as np
import pandas as pd

from rfscopedb.data_model import Scan, Query
from rfscopedb.db import WaveformDB


class TestQuery(unittest.TestCase):
    """Test the Query class"""
    db = WaveformDB(host='localhost', user='scope_rw', password='password')
    x1 = None
    x2 = None
    x3 = None

    @classmethod
    def setUpClass(cls):
        scan_start1 = datetime.strptime("2020-01-01 01:23:45.123456", '%Y-%m-%d %H:%M:%S.%f')
        scan_start2 = datetime.strptime("2021-01-01 01:23:45.123456", '%Y-%m-%d %H:%M:%S.%f')
        scan_start3 = datetime.strptime("2022-01-01 01:23:45.123456", '%Y-%m-%d %H:%M:%S.%f')
        scan_end1 = datetime.strptime("2020-01-01 01:23:55.123456", '%Y-%m-%d %H:%M:%S.%f')
        scan_end2 = datetime.strptime("2021-01-01 01:23:55.123456", '%Y-%m-%d %H:%M:%S.%f')
        scan_end3 = datetime.strptime("2022-01-01 01:23:55.123456", '%Y-%m-%d %H:%M:%S.%f')
        cls.x1 = Scan(start=scan_start1, end=scan_end1)
        cls.x2 = Scan(start=scan_start2, end=scan_end2)
        cls.x3 = Scan(start=scan_start3, end=scan_end3)

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

        cls.x1.add_cavity_data("c1", data=cavity_data1, sampling_rate=5000)
        cls.x1.add_scan_data(float_data={'a': 1.0, "b": 2.0}, str_data={'c': 'on'})
        cls.x2.add_cavity_data("c2", data=cavity_data2, sampling_rate=5000)
        cls.x2.add_scan_data(float_data={'a': 2.0, "b": 3.0, "d": -10.0}, str_data={'c': 'off'})
        cls.x3.add_cavity_data("c3", data=cavity_data3, sampling_rate=5000)
        cls.x3.add_scan_data(float_data={'a': 1.1, "b": 2.1}, str_data={'c': 'on'})

    # pylint: disable=too-many-locals
    def test_query_waveforms_arrays(self):
        """Test querying waveforms from the database"""
        signal_names = ["GMES", "PMES"]
        process_names = ["raw", "power_spectrum"]

        query = Query(db=TestQuery.db, signal_names=signal_names, array_names=process_names)
        query.stage()
        query.run()

        arrays = query.wf_data

        # Define some convenient DataFrame filters
        is_raw = arrays.name == "raw"
        is_psd = arrays.name == "power_spectrum"
        is_c1 = arrays.cavity == "c1"
        is_c2 = arrays.cavity == "c2"
        is_c3 = arrays.cavity == "c3"
        is_gmes = arrays.signal_name == "GMES"
        is_pmes = arrays.signal_name == "PMES"

        # Get 'raw' data for tests
        c1_gmes = arrays.loc[is_c1 & is_gmes & is_raw, "data"].values
        c2_gmes = arrays.loc[is_c2 & is_gmes & is_raw, "data"].values
        c3_pmes = arrays.loc[is_c3 & is_pmes & is_raw, "data"].values

        # Check that we got the right number of responses
        self.assertEqual(1, len(c1_gmes))
        self.assertEqual(1, len(c2_gmes))
        self.assertEqual(1, len(c3_pmes))

        # Check that the raw waveforms look right
        self.assertTrue(np.allclose(self.x1.waveform_data['c1']['GMES'], c1_gmes[0]))
        self.assertTrue(np.allclose(self.x2.waveform_data['c2']['GMES'], c2_gmes[0]))
        self.assertTrue(np.allclose(self.x3.waveform_data['c3']['PMES'], c3_pmes[0]))

        c1_gmes_psd = arrays.loc[is_c1 & is_gmes & is_psd, "data"].values
        c2_gmes_psd = arrays.loc[is_c2 & is_gmes & is_psd, "data"].values
        c3_pmes_psd = arrays.loc[is_c3 & is_pmes & is_psd, "data"].values

        # Check that we got the right number of responses
        self.assertEqual(1, len(c1_gmes_psd))
        self.assertEqual(1, len(c2_gmes_psd))
        self.assertEqual(1, len(c3_pmes_psd))

        # Check that one of the analysis waveforms look right
        self.assertTrue(np.allclose(self.x1.analysis_array['c1']['GMES']['power_spectrum'], c1_gmes_psd[0]))
        self.assertTrue(np.allclose(self.x2.analysis_array['c2']['GMES']['power_spectrum'], c2_gmes_psd[0]))
        self.assertTrue(np.allclose(self.x3.analysis_array['c3']['PMES']['power_spectrum'], c3_pmes_psd[0]))

    def test_query_waveforms_scalars(self):
        """Test querying waveforms from the database"""
        signal_names = ["GMES", "PMES"]
        process_names = ["raw", "power_spectrum"]

        query = Query(db=TestQuery.db, signal_names=signal_names, array_names=process_names)
        query.stage()
        query.run()

        meta = query.wf_meta

        # Define some convenient DataFrame filters for meta
        is_c1 = meta.cavity == "c1"
        is_c2 = meta.cavity == "c2"
        is_c3 = meta.cavity == "c3"
        is_gmes = meta.signal_name == "GMES"
        is_pmes = meta.signal_name == "PMES"

        pd.set_option('display.max_columns', None)
        c1_gmes_dom_freq = meta.loc[is_c1 & is_gmes, 'dominant_frequency'].values
        c2_gmes_dom_freq = meta.loc[is_c2 & is_gmes, 'dominant_frequency'].values
        c3_pmes_dom_freq = meta.loc[is_c3 & is_pmes, 'dominant_frequency'].values

        # Check that we got the right number of responses
        self.assertEqual(1, len(c1_gmes_dom_freq))
        self.assertEqual(1, len(c2_gmes_dom_freq))
        self.assertEqual(1, len(c3_pmes_dom_freq))

        # Check that one of the scalar features look right
        self.assertTrue(np.allclose(self.x1.analysis_scalar['c1']['GMES']['dominant_frequency'], c1_gmes_dom_freq[0]))
        self.assertTrue(np.allclose(self.x2.analysis_scalar['c2']['GMES']['dominant_frequency'], c2_gmes_dom_freq[0]))
        self.assertTrue(np.allclose(self.x3.analysis_scalar['c3']['PMES']['dominant_frequency'], c3_pmes_dom_freq[0]))
