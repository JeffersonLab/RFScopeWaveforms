"""A package for interacting with data at a more tractable level"""

import json
from datetime import datetime
from typing import Optional, Dict, Tuple, Any, List, Union

import mysql.connector
from mysql.connector.cursor import MySQLCursor
import numpy as np
import pandas as pd
from numpy import ndarray
from scipy.signal import periodogram

from .db import WaveformDB
from .utils import get_datetime_as_utc



class Scan:
    """This class contains all the data from a scan of waveform data from one or more RF cavities and related logic.

    This class will store raw waveform data, generate collections of derivative data about each waveform, and hold
    additional data related to system state at the time of the scan.
    """

    def __init__(self, dt: datetime, sid: Optional[int] = None):
        """Construct an instance and initialize data attributes

        Args:
            dt: The date and time the scan was started
            sid: The unique database scan ID for this object.  None implies that the object was not read from the
                 database.
        """

        self.id = sid
        self.dt = dt

        # self.waveform_data will be structured as {
        #   {
        #     <cav_name1>: {<signal_name1>: [val1, val2, ... ], <signal_name2>: [val1, val2, ...]}, ...,
        #     <cav_name2>: {<signal_name1>: [val1, val2, ... ], <signal_name2>: [val1, val2, ...]}, ...,
        #     ...
        #   }
        # }
        self.waveform_data = {}

        # self.sampling_frequency will be structured
        # {
        #   <cav_name1>: sampling_freq1,
        #   <cav_name1>: sampling_freq2,
        #   ...
        # }
        self.sampling_rate = {}

        # self.analysis_scalar and self.analysis_array will be structure this where one will hold scalar values and the
        # other will hold np.array values
        #   {
        #     <cav_name1>: {<signal_name1>: {<metric1>: value, <metric2>: value}, <signal_name2>: {<metric1>...}},
        #     <cav_name2>: {<signal_name1>: {<metric1>: value, <metric2>: value}, <signal_name2>: {<metric1>...}},
        #     ...
        #   }
        self.analysis_scalar = {}
        self.analysis_array = {}

        self.scan_data_float = {}
        self.scan_data_str = {}

    def add_scan_data(self, float_data: Dict[str, float], str_data: Dict[str, str]) -> None:
        """Add data that applies to the entire scan and not a specific waveform.  There can be no overlap in keys.

        Args:
            float_data: A dictionary containing numeric data relating to the scan. Keys are data names (e.g. R1XXITOT)
            str_data: A dictionary containing textual data relating to the scan. Keys are data names. Useful for ENUMS.
        """

        for k in float_data.keys():
            if k in str_data.keys():
                raise ValueError(f"A metadata name may only appear once in either the float or str data. ('{k}')")

        self.scan_data_float.update(float_data)
        self.scan_data_str.update(str_data)

    def add_cavity_data(self, cavity: str, data: Dict[str, np.array], sampling_rate: float):
        """Add waveform data to this scan for a given cavity.  Analysis of the waveform values are done here.

        Args:
            cavity: The name of the cavity ("R123")
            data: Dictionary keyed on signal name ("Time", "GMES", etc.) with numpy arrays containing signal data
            sampling_rate: The sampling rate of the data given in Hertz (e.g. 5000 for 5 kHz).
        """
        self.waveform_data[cavity] = data
        self.analysis_scalar[cavity] = {}
        self.analysis_array[cavity] = {}
        self.sampling_rate[cavity] = sampling_rate

        for signal_name in data.keys():
            # Time is reflected in the sampling rate and can be ignored for analysis purposes
            if signal_name == "Time":
                continue

            scalars, arrays = self.analyze_signal(data[signal_name], sampling_rate=sampling_rate)
            self.analysis_scalar[cavity][signal_name] = scalars

            self.analysis_array[cavity][signal_name] = {}
            for arr_name, array in arrays.items():
                self.analysis_array[cavity][signal_name][arr_name] = array

    def insert_data(self, conn: mysql.connector.MySQLConnection):
        """Insert all data related to this Scan into the database

        Args:
            conn: Connection to the database
        """
        scan_time = get_datetime_as_utc(self.dt)
        cursor = None
        try:
            # Transaction started by default since autocommit is off.
            cursor = conn.cursor()
            # Note: execute and executemany do sanitation and prepared statements internally.
            cursor.execute("INSERT INTO scan (scan_start_utc)  VALUES (%s)",
                           (scan_time.strftime('%Y-%m-%d %H:%M:%S.%f'),))
            cursor.execute("SELECT LAST_INSERT_ID()")
            sid = cursor.fetchone()[0]

            for cav in self.waveform_data:
                for signal_name in self.waveform_data[cav]:
                    if signal_name == "Time":
                        continue

                    wid = self._insert_waveform(cursor, sid, cav, signal_name)
                    self._insert_waveform_adata(cursor, wid, cav, signal_name)
                    self._insert_waveform_sdata(cursor, wid, cav, signal_name)

            self._insert_scan_fdata(cursor, sid)
            self._insert_scan_sdata(cursor, sid)

            # Commit the transaction if we were able to successfully insert all the data.  Otherwise, an exception
            # should have been raised that was caught to roll back the transaction.
            conn.commit()
        except (mysql.connector.Error, Exception) as e:
            if conn is not None:
                # There was a problem so this should roll back the entire transaction across all the tables.
                conn.rollback()
            if cursor is not None:
                cursor.close()

            # TODO: better logging
            print(f"ERROR: {e}")
            # TODO: Alternative to catch and throw?

            raise e

    def _insert_waveform(self, cursor: MySQLCursor, sid: int, cav: str, signal_name: str) -> int:
        """Insert a waveform into the database and return it's wid key."""
        cursor.execute("INSERT INTO waveform(sid, cavity, signal_name, sample_rate_hz) VALUES (%s, %s, %s, %s)",
            (sid, cav, signal_name, self.sampling_rate[cav]))
        cursor.execute("SELECT LAST_INSERT_ID()")
        return cursor.fetchone()[0]

    def _insert_waveform_adata(self, cursor: MySQLCursor, wid: int, cav: str, signal_name: str):
        """Insert the waveform array data to the database.

        Args:
            cursor: A database cursor
            wid: The unique id of the waveform
            cav: The name of the cavity ("R123")
            signal_name: The name of the signal ("GMES")
        """
        # Append the array data for the waveform.  'raw' is not an analytical waveform and needs to be done separately
        array_data = [(wid, "raw", json.dumps(self.waveform_data[cav][signal_name].tolist()))]
        for arr_name, array in self.analysis_array[cav][signal_name].items():
            array_data.append(
                (wid, arr_name, json.dumps(self.analysis_array[cav][signal_name][arr_name].tolist())))

        cursor.executemany("INSERT INTO waveform_adata (wid, process, data) VALUES (%s, %s, %s)",
                           array_data)

    def _insert_waveform_sdata(self, cursor: MySQLCursor, wid: int, cav: str, signal_name: str):
        """Insert the waveform scalar data to the database.

        Args:
            cursor: A database cursor
            wid: The unique id of the waveform
            cav: The name of the cavity ("R123")
            signal_name: The name of the signal ("GMES")
        """

        data = []
        for metric_name, value in self.analysis_scalar[cav][signal_name].items():
            data.append((wid, metric_name, value))
        cursor.executemany("INSERT INTO waveform_sdata (wid, name, value) VALUES (%s, %s, %s)", data)

    def _insert_scan_fdata(self, cursor: MySQLCursor, sid: int):
        """Insert the float data associated with this scan into the database.

        Args:
            cursor: A database cursor
            sid: The unique database scan ID
        """
        data = []
        for key, value in self.scan_data_float.items():
            data.append((sid, key, value))

        if len(data) > 0:
            cursor.executemany("INSERT INTO scan_fdata (sid, name, value) VALUES (%s, %s, %s)", data)

    def _insert_scan_sdata(self, cursor: MySQLCursor, sid: int):
        """Insert the string data associated with this scan into the database.

        Args:
            cursor: A database cursor
            sid: The unique database scan ID
        """
        data = []
        for key, value in self.scan_data_str.items():
            data.append((sid, key, value))
        if len(data) > 0:
            cursor.executemany("INSERT INTO scan_sdata (sid, name, value) VALUES (%s, %s, %s)", data)

    @staticmethod
    def analyze_signal(arr, sampling_rate=5000) -> Tuple[dict, dict]:

        """Computes basic statistical metrics and power spectrum for a single waveform of length 8192 samples.

        Args:
            arr (np.array): the array containing data
            sampling_rate (float): samping frequency represented by data in Hz

        Returns:
            Tuple[dict, dict]: dictionary of scalar statistical metrics, dictionary of arrays data
                               (e.g. power spectrum array)
        """

        if not isinstance(arr, (list, np.ndarray, tuple)):
            raise TypeError(f"Input must be a list, numpy array, or tuple. Not {type(arr)}")

        arr = np.array(arr)

        if not np.issubdtype(arr.dtype, np.number):
            raise ValueError("Input array must contain only numerical values.")

        if len(arr) != 8192:
            raise ValueError(f"Input array must have exactly 8192 elements. Got {len(arr)} elements.")

        # basic statistics
        min_val = np.min(arr)
        max_val = np.max(arr)
        peak_to_peak = max_val - min_val
        mean = np.mean(arr)
        median = np.median(arr)
        std_dev = np.std(arr)
        rms = np.sqrt(np.mean(np.square(arr)))
        q25 = np.percentile(arr, 25)
        q75 = np.percentile(arr, 75)

        # power spectrum analysis using Welch's method
        f, Pxx_den = periodogram(arr, sampling_rate)

        # find dominant frequency
        dominant_freq = f[np.argmax(Pxx_den)]

        scalars = {
            "minimum": min_val,
            "maximum": max_val,
            "peak_to_peak": peak_to_peak,
            "mean": mean,
            "median": median,
            "standard_deviation": std_dev,
            "rms": rms,
            "25th_quartile": q25,
            "75th_quartile": q75,
            "dominant_frequency": dominant_freq
        }
        arrays: dict[str, ndarray] = {
            "power_spectrum": Pxx_den,
            "frequencies": f
        }

        return scalars, arrays

    @staticmethod
    def row_to_scan(row: Dict[str, Any]) -> 'Scan':
        """Take a singe database row result and generates a Scan object from it.  Expects rows as dictionaries.

        Args:
            row: A dictionary result from a database cursor that contains basic information about a scan.

        Returns:
            A Scan object based on the database row result.
        """
        return Scan(dt=row['scan_start_utc'].astimezone(), sid=row['sid'])


class Query:
    """This class is responsible for running queries of waveform data against the database.

    The basic idea is that a Query will be staged, where information about the set of scans will be determined from the
    database.  The user can investigate the scan information to determine if they would like to continue querying data
    from the database.  Since querying large amounts of data from the database could consume large amounts of time and
    system resources, the user may wish to check how many scans are included in their query before continuing.
    """
    staged: bool
    scan_meta: None | pd.DataFrame
    wf_data: None | pd.DataFrame
    wf_meta: None | pd.DataFrame

    def __init__(self, db: WaveformDB, signal_names: List[str], array_names: Optional[List[str]] = None,
                 begin: Optional[datetime] = None, end: Optional[datetime] = None,
                 scan_filter_params: Optional[List[str]] = None, scan_filter_ops: Optional[List[str]] = None,
                 scan_filter_values: Optional[List[Union[float, str]]] = None,
                 wf_metric_names: Optional[List[str]] = None):
        """Construct a query object with the information needed to query scan and waveform data.

        The three scan_filter_* parameters work in conjunction.  The must be list-like objects of the same length.  At
        a given index, a filter is later constructed such that "<param> <op> <value>" is used in a SQL WHERE clause.
        For example, if scan_filter_params = ['R123GMES', 'R223GMES'], scan_filter_ops = ['>', '<'], and
        scan_filter_values = [5, 2], then each scan included in the query must meet the following criteria:
            R123GMES > 5 AND R223GMES < 2

        Args:
            db: A WaveFromDB object that contains an active connection to the database.
            signal_names: A list of signals (a.k.a. waveforms) to query.  E.g., GMES, PMES, etc.
            array_names: Each signal/waveform may have multiple arrays of data associated with it.  For example, 'raw'
                         returns the unmodified waveform data, while 'power_spectrum' returns the power at different
                         frequencies.
            begin: The earliest start time for which a scan will be included in the query.  If None, there is
                   no earliest time filter.
            end: The latest end time for which a scan will be included in the query.  If None, there is latest time
                 filter.
             scan_filter_params: A list of metadata parameter names used to filter the scans for the query
             scan_filter_ops: A list of SQL comparison operators used to filter the scans for the query
             scan_filter_values: A list of values used to filter the scans for the query
             wf_metric_names: A list of scalar metrics related to a waveform that will be included if they exist in the
                              database.
            """

        self.db = db
        self.signal_names = signal_names
        self.array_names = array_names
        self.begin = begin
        self.end = end
        self.scan_filter_params = scan_filter_params
        self.scan_filter_ops = scan_filter_ops
        self.scan_filter_values = scan_filter_values
        self.wf_metric_names = wf_metric_names

        self.staged = False
        self.scan_meta = None
        self.wf_data = None
        self.wf_meta = None

    def stage(self):
        """Perform the initial query to determine which scans meet the requested criteria."""

        scan_rows = self.db.query_scan_rows(begin=self.begin, end=self.end, filter_params=self.scan_filter_params,
                                            filter_ops=self.scan_filter_ops, filter_values=self.scan_filter_values)
        self.scan_meta = pd.DataFrame(scan_rows, index=None)
        self.staged = True

    def get_scan_count(self):
        """Get the number of scans that meet the requested criteria."""
        return len(self.scan_meta)

    def run(self):
        """Run the full query that will return the full waveform data and metadata.  Must run stage() first."""
        if not self.staged:
            raise RuntimeError(f"Query not staged.")

        # Note that in the database, array names are specified by the "process" that generated them.
        rows = self.db.query_waveform_data(self.scan_meta.sid.values.tolist(), signal_names=self.signal_names,
                                           process_names=self.array_names)
        self.wf_data = pd.DataFrame(rows)

        rows = self.db.query_waveform_metadata(self.scan_meta.sid.values.tolist(), signal_names=self.signal_names,
                                               metric_names=self.wf_metric_names)
        self.wf_meta = pd.DataFrame(rows)
