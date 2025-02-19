"""Tests for the db.py module."""
import unittest
from datetime import datetime

import numpy as np

from rfscopedb.db import QueryFilter
from rfscopedb.data_model import Scan

dt = datetime.strptime("2020-01-01 01:23:45.123456", '%Y-%m-%d %H:%M:%S.%f')


class TestQueryFilter(unittest.TestCase):
    """Tests for the QueryFilter class."""

    def test_query_filter_creation1(self):
        """Test construction with all None inputs"""
        QueryFilter(None, None, None)

    def test_query_filter_creation2(self):
        """Test construction with length one input for each inputs"""
        QueryFilter(['c'], ['='], [73])

    def test_query_filter_creation3(self):
        """Test construction with inputs of length greater than one"""
        QueryFilter(filter_params=['c', 'b', 'a'], filter_ops=['=', '<=', '='], filter_values=[73, 24, "asdf"])

    # pylint: disable=no-value-for-parameter
    # noinspection PyArgumentList
    def test_query_filter_creation_checks(self):
        """Test that construction fails for a set of expected conditions"""
        with self.assertRaises(TypeError):
            QueryFilter(['c', ])
        with self.assertRaises(TypeError):
            QueryFilter(filter_ops=['=', ])
        with self.assertRaises(TypeError):
            QueryFilter([27, ])
        with self.assertRaises(ValueError):
            QueryFilter(['c', ], ['asdf'], [73])
        with self.assertRaises(ValueError):
            QueryFilter(['c', ], ['>'], [73, 24, 'asdf'])

    def test_query_len1(self):
        """Test that the len method works as expected"""
        f = QueryFilter(filter_params=['c', 'b', 'a'], filter_ops=['=', '<=', '='], filter_values=[73, 24, "asdf"])
        self.assertEqual(3, len(f))

    def test_query_len2(self):
        """Test that the len method works as expected"""
        f = QueryFilter(filter_params=['c', 'a'], filter_ops=['=', '='], filter_values=[73, "asdf"])
        self.assertEqual(2, len(f))

    def test_query_len3(self):
        """Test that the len method works as expected"""
        f = QueryFilter(None, None, None)
        self.assertEqual(0, len(f))


class TestScan(unittest.TestCase):
    """Tests for the Scan class."""

    def test_scan_creation(self):
        """Test basic construction"""
        x = Scan(dt=dt)

        self.assertEqual(dt, x.dt)
        self.assertEqual({}, x.waveform_data)
        self.assertEqual({}, x.analysis_array)
        self.assertEqual({}, x.analysis_scalar)
        self.assertEqual({}, x.scan_data_float)
        self.assertEqual({}, x.scan_data_str)

    def test_add_scan_data(self):
        """Test adding scan data"""
        x = Scan(dt=dt)

        float_data = {'a': 11.34, 'b': 12.34, 'c': 12}
        string_data = {'as': 'test1', 'bs': 'test2'}

        x.add_scan_data(float_data, string_data)
        self.assertDictEqual(x.scan_data_float, float_data)
        self.assertDictEqual(x.scan_data_str, string_data)

    def test_add_cavity_data(self):
        """Test adding cavity data"""
        # pylint: disable=invalid-name
        self.maxDiff = None

        x = Scan(dt=dt)
        t = np.linspace(0, 1638.2, 8192) / 1000.0
        gmes = 0.5 * np.cos(t * 2 * np.pi * 6.103) + 1

        cavity_data1 = {
            'Time': t,
            'GMES': gmes,
            'PMES': gmes,
        }
        cavity_data2 = {
            'Time': t,
            'GMES': gmes + 1,
            'PMES': gmes + 1,
        }

        scalar_data = {
            "R123": {
                'GMES': {
                    "minimum": np.float64(0.5),
                    "maximum": np.float64(1.5),
                    "peak_to_peak": np.float64(1.0),
                    "mean": np.float64(0.9999577572666067),
                    "median": np.float64(0.9999629292071286),
                    "standard_deviation": np.float64(0.3535384535785386),
                    "rms": np.float64(1.0606153659439252),
                    "25th_quartile": np.float64(0.6464856093668832),
                    "75th_quartile": np.float64(1.3534360761155124),
                    "dominant_frequency": np.float64(6.103515625)
                },
                'PMES': {
                    "minimum": np.float64(0.5),
                    "maximum": np.float64(1.5),
                    "peak_to_peak": np.float64(1.0),
                    "mean": np.float64(0.9999577572666067),
                    "median": np.float64(0.9999629292071286),
                    "standard_deviation": np.float64(0.3535384535785386),
                    "rms": np.float64(1.0606153659439252),
                    "25th_quartile": np.float64(0.6464856093668832),
                    "75th_quartile": np.float64(1.3534360761155124),
                    "dominant_frequency": np.float64(6.103515625)
                }
            },
            "R124": {
                'GMES': {
                    "minimum": np.float64(1.5),
                    "maximum": np.float64(2.5),
                    "peak_to_peak": np.float64(1.0),
                    "mean": np.float64(1.9999577572666067),
                    "median": np.float64(1.9999629292071286),
                    "standard_deviation": np.float64(0.3535384535785386),
                    "rms": np.float64(2.030965403203506),
                    "25th_quartile": np.float64(1.6464856093668832),
                    "75th_quartile": np.float64(2.3534360761155124),
                    "dominant_frequency": np.float64(6.103515625)
                },
                'PMES': {
                    "minimum": np.float64(1.5),
                    "maximum": np.float64(2.5),
                    "peak_to_peak": np.float64(1.0),
                    "mean": np.float64(1.9999577572666067),
                    "median": np.float64(1.9999629292071286),
                    "standard_deviation": np.float64(0.3535384535785386),
                    "rms": np.float64(2.030965403203506),
                    "25th_quartile": np.float64(1.6464856093668832),
                    "75th_quartile": np.float64(2.3534360761155124),
                    "dominant_frequency": np.float64(6.103515625)
                }
            }
        }

        # There is a strong peak near 6.103 Hz, but there is still some mismatch in the represented frequencies that
        # leads to a lot of low level noise in the power spectrum across all frequencies (1e-7/1e-8)  It's easier to
        # save the exact output and load for a test, but the actual PSD spike is at the correct frequency.
        Pxx_den = np.loadtxt(fname="test/unit/power_spectrum.csv", delimiter=",")
        # Frequencies are multiples of the sampling resolution which equals sampling frequency / number of samples.
        f = [i * 5000.0 / 8192.0 for i in range(4097)]

        array_data = {
            'GMES': {
                "power_spectrum": Pxx_den,
                "frequencies": f
            },
            'PMES': {
                "power_spectrum": Pxx_den,
                "frequencies": f
            }
        }

        x.add_cavity_data("R123", cavity_data1, sampling_rate=5000)
        x.add_cavity_data("R124", cavity_data2, sampling_rate=5000)

        self.assertDictEqual(cavity_data1, x.waveform_data["R123"])
        self.assertDictEqual(cavity_data2, x.waveform_data["R124"])
        # pylint: disable=consider-using-dict-items
        for cavity in x.analysis_array:
            for signal in x.analysis_array[cavity].keys():
                for k in x.analysis_array[cavity][signal].keys():
                    self.assertTrue(np.allclose(array_data[signal][k], x.analysis_array[cavity][signal][k]))
                for k in x.analysis_scalar[cavity][signal].keys():
                    self.assertAlmostEqual(scalar_data[cavity][signal][k], x.analysis_scalar[cavity][signal][k],
                                           msg=f"{cavity}-{signal}-{k} has mismatch")
