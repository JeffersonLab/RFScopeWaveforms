import json
from datetime import datetime
from typing import Optional, Dict, Tuple, Any, List, Union

import mariadb
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

    def insert_data(self, conn: mariadb.Connection):
        """Insert all data related to this Scan"""
        scan_time = get_datetime_as_utc(self.dt)
        cursor = None
        try:
            # Start the transaction
            conn.begin()
            cursor = conn.cursor()
            # Note: execute and executemany do sanitation and prepared statements internally.
            cursor.execute("INSERT INTO scan (scan_start_utc)  VALUES (?)",
                           (scan_time.strftime('%Y-%m-%d %H:%M:%S.%f'),))
            cursor.execute("SELECT LAST_INSERT_ID()")
            sid = cursor.fetchone()[0]

            for cav in self.waveform_data:
                for signal_name in self.waveform_data[cav]:
                    if signal_name == "Time":
                        continue
                    waveform_data = (sid, cav, signal_name, self.sampling_rate[cav])
                    cursor.execute("INSERT INTO waveform(sid, cavity, signal_name, sample_rate_hz) VALUES (?, ?, ?, ?)",
                                   waveform_data)
                    cursor.execute("SELECT LAST_INSERT_ID()")
                    wid = cursor.fetchone()[0]

                    # Append the array data for the waveform
                    array_data = [(wid, "raw", json.dumps(self.waveform_data[cav][signal_name].tolist()))]
                    for arr_name, array in self.analysis_array[cav][signal_name].items():
                        array_data.append(
                            (wid, arr_name, json.dumps(self.analysis_array[cav][signal_name][arr_name].tolist())))

                    cursor.executemany("INSERT INTO waveform_adata (wid, process, data) VALUES (?, ?, ?)", array_data)

                    scalar_data = []
                    for metric_name, value in self.analysis_scalar[cav][signal_name].items():
                        scalar_data.append((wid, metric_name, value))

                    cursor.executemany("INSERT INTO waveform_sdata (wid, name, value) VALUES (?, ?, ?)",
                                       scalar_data)

            sdf = []
            for key, value in self.scan_data_float.items():
                sdf.append((sid, key, value))

            if len(sdf) > 0:
                cursor.executemany("INSERT INTO scan_fdata (sid, name, value) VALUES (?, ?, ?)", sdf)

            sds = []
            for key, value in self.scan_data_str.items():
                sds.append((sid, key, value))
            if len(sds) > 0:
                cursor.executemany("INSERT INTO scan_sdata (sid, name, value) VALUES (?, ?, ?)", sds)

            # Commit the transaction if we were able to successfully insert all the data.  Otherwise, an exception
            # should have been raised that was caught to roll back the transaction.
            conn.commit()
        except (mariadb.Error, Exception) as e:
            if conn is not None:
                # There was a problem so this should roll back the entire transaction across all the tables.
                conn.rollback()
            if cursor is not None:
                cursor.close()

            # TODO: better logging
            print(f"ERROR: {e}")
            # TODO: Alternative to catch and throw?

            raise e

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
    def row_to_scan(row: Dict[str, Any]):
        """Take a singe database row result and generates a Scan object from it.  Expects rows as dictionaries."""
        return Scan(dt=row['scan_start_utc'].astimezone(), sid=row['sid'])


class Query:

    def __init__(self, db: WaveformDB, signal_names: List[str], process_names: Optional[List[str]] = None,
                 begin: Optional[datetime] = None, end: Optional[datetime] = None,
                 scan_filter_params: Optional[List[str]] = None, scan_filter_ops: Optional[List[str]] = None,
                 scan_filter_values: Optional[List[Union[float, str]]] = None, wf_metric_names: Optional[List[str]] = None):
        self.db = db
        self.signal_names = signal_names
        self.process_names = process_names
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

    def run(self):
        if not self.staged:
            raise RuntimeError(f"Query not staged.")

        rows = self.db.query_waveform_data(self.scan_meta.sid.values.tolist(), signal_names=self.signal_names,
                             process_names=self.process_names)
        self.wf_data = pd.DataFrame(rows)

        rows = self.db.query_waveform_metadata(self.scan_meta.sid.values.tolist(), signal_names=self.signal_names,
                                     metric_names=self.wf_metric_names)
        self.wf_meta = pd.DataFrame(rows)
