import unittest
from datetime import datetime

import numpy as np

from rfscope.db import Scan


class TestDB(unittest.TestCase):
    dt = datetime.strptime("2020-01-01 01:23:45.123456", '%Y-%m-%d %H:%M:%S.%f')

    def test_scan_creation(self):
        x = Scan(dt=TestDB.dt)

        self.assertEqual(TestDB.dt, x.dt)
        self.assertEqual({}, x.waveform_data)
        self.assertEqual({}, x.analysis_array)
        self.assertEqual({}, x.analysis_scalar)
        self.assertEqual({}, x.scan_data_float)
        self.assertEqual({}, x.scan_data_str)

    def test_add_scan_data(self):
        x = Scan(dt=TestDB.dt)

        float_data = {'a': 11.34, 'b': 12.34, 'c': 12}
        string_data = {'a': 'test1', 'b': 'test2'}

        x.add_scan_data(float_data, string_data)
        self.assertDictEqual(x.scan_data_float, float_data)
        self.assertDictEqual(x.scan_data_str, string_data)

    # TODO: Figure this out
    # def test_add_cavity_data(self):
    #     x = Scan(dt=TestDB.dt)
    #     t = np.linspace(0, 1638.2, 8192)
    #     cavity_data = {
    #         'R123': {
    #             'Time': t,
    #             'GMES': 0.5 * np.cos(t * 2 * np.pi * 10) + 1,
    #         }
    #     }
    #
    #     scalars = {
    #         "minimum": min_val,
    #         "maximum": max_val,
    #         "peak_to_peak": peak_to_peak,
    #         "mean": mean,
    #         "median": median,
    #         "standard_deviation": std_dev,
    #         "rms": rms,
    #         "25th_quartile": q25,
    #         "75th_quartile": q75,
    #         "dominant_frequency": dominant_freq
    #     }
    #     arrays: dict[str, ndarray] = {
    #         "power_spectrum": Pxx_den,
    #         "frequencies": f
    #     }
    #
    #     ps = np.zeros(8192)
    #     ps[]
    #     array_data = {
    #         'R123': {
    #             'GMES': {
    #                 'power_spectrum':
    #             }
    #         }
    #     }
    #
    #     x.add_cavity_data("R123", cavity_data, sampling_rate=5000)
    #
    #     self.assertDictEqual(cavity_data, x.cavity_data)
    #     self.assertDictEqual({}, x.analysis_scalar)
    #     self.assertDictEqual({}, x.analysis_array)
