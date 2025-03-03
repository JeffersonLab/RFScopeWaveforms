from unittest import TestCase

import numpy as np
from scipy.signal import periodogram

from rfscopedb.data_model import Query
from rfscopedb.db import WaveformDB


class TestQuery(TestCase):
    db = WaveformDB(host='localhost', user='scope_rw', password='password')

    def test_get_frequencies1(self):
        """Test constructing frequencies that match the distribution supplied by scipy's periodogram."""

        fs = 5000.0
        n = 8192
        query = Query(db=TestQuery.db, signal_names=["GMES", "PMES"])
        arr = np.ones(n)

        exp, Pxx_den = periodogram(arr, fs)
        result = query.get_frequency_range(fs, n)

        self.assertTrue(np.allclose(exp, result))

    def test_get_frequencies2(self):
        """Test constructing frequencies that match the distribution supplied by scipy's periodogram."""

        fs = 317.2
        n = 4101
        query = Query(db=TestQuery.db, signal_names=["GMES", "PMES"])
        arr = np.ones(n)

        exp, Pxx_density = periodogram(arr, fs)
        result = query.get_frequency_range(fs, n)

        print()
        print(exp)
        print(result)

        self.assertTrue(np.allclose(exp, result))

    def test_get_frequencies3(self):
        """Test constructing frequencies that match the distribution supplied by scipy's periodogram."""

        fs = 1.0
        n = 17
        query = Query(db=TestQuery.db, signal_names=["GMES", "PMES"])
        arr = np.ones(n)

        exp, Pxx_density = periodogram(arr, fs)
        result = query.get_frequency_range(fs, n)

        print()
        print(exp)
        print(result)

        self.assertTrue(np.allclose(exp, result))
