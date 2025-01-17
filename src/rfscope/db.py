import json
from datetime import datetime, timezone
from typing import Dict, Tuple

import numpy as np
import mariadb
from numpy.core.multiarray import ndarray
from scipy.signal import welch


class Scan:
    """This class contains all the data from a scan of waveform data from one or more RF cavities and related logic.

    This class will store raw waveform data, generate collections of derivative data about each waveform, and hold
    additional data related to system state at the time of the scan.
    """
    def __init__(self, dt: datetime):
        """Construct an instance and initialize data attributes

        Args:
            dt: The date and time the scan was started
        """
        self.dt = dt

        # self.data will be structured as {
        #   {
        #     <cav_name1>: {<signal_name1>: {<metric1>: value, <metric2>: value}, <signal_name2>: {<metric1>...},
        #     <cav_name2>: {<signal_name1>: {<metric1>: value, <metric2>: value}, <signal_name2>: {<metric1>...},
        #     ...
        #   }
        # }
        self.waveform_data = {}

        # self.analysis_scalar and self.analysis_array will be structure this where one will hold scalar values and the
        # other will hold np.array values
        #   {
        #     <cav_name1>: {<signal_name1>: {<metric1>: value, <metric2>: value}, <signal_name2>: {<metric1>...},
        #     <cav_name2>: {<signal_name1>: {<metric1>: value, <metric2>: value}, <signal_name2>: {<metric1>...},
        #     ...
        #   }
        self.analysis_scalar = {}
        self.analysis_array = {}

        self.scan_data_float = {}
        self.scan_data_str = {}

    def add_scan_data(self, float_data: Dict[str, float], str_data: Dict[str, str]) -> None:
        """Add data that applies to the entire scan and not a specific waveform.

        Args:
            float_data: A dictionary containing numeric data relating to the scan. Keys are data names (e.g. R1XXITOT)
            str_data: A dictionary containing textual data relating to the scan. Keys are data names. Useful for ENUMS.
        """
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

        for signal_name in data.keys():
            # Time is reflected in the sampling rate and can be ignored for analysis purposes
            if signal_name == "Time":
                continue
            scalars, arrays = self.analyze_signal(data[signal_name], sampling_rate=sampling_rate)
            self.analysis_scalar[cavity][signal_name] = scalars
            for arr_name, array in arrays.items():
                self.analysis_array[cavity][arr_name] = array

    def insert_data(self, conn: mariadb.Connection):
        """Insert all data related to this Scan"""
        scan_time = self.dt.astimezone(timezone.utc)
        cursor = conn.cursor()
        try:
            # Note: execute and executemany do sanitation and prepared statements internally.
            cursor.execute("INSERT INTO scan (scan_start_utc)  VALUES (?)", (scan_time.strftime('%Y-%m-%d %H:%M:%S'),))
            sid = cursor.execute("SELECT LAST_INSERT_ID()").fetchone()[0]

            for cav in self.waveform_data:
                for signal_name in self.waveform_data[cav]:
                    waveform_data = (sid, cav, signal_name)
                    cursor.execute("INSERT INTO waveform(sid, cavity, signal_name) VALUES (?, ?, ?)", waveform_data)
                    wid = cursor.execute("SELECT LAST_INSERT_ID()").fetchone()[0]

                    # Append the array data for the waveform
                    array_data = []
                    array_data.append(("raw", signal_name, json.dumps(self.waveform_data[cav][signal_name].to_list())))
                    for arr_name, array in self.analysis_array[cav].items():
                        array_data.append((wid, arr_name, json.dumps(self.analysis_array[cav][arr_name].to_list())))
                    cursor.executemany("INSERT INTO waveform_adata (wid, name, data) VALUES (?, ?, ?)", array_data)

                    scalar_data = []
                    for metric_name, value in self.analysis_scalar[cav].items():
                        scalar_data.append((wid, metric_name, value))
                        cursor.executemany("INSERT INTO waveform_sdata (wid, name, value) VALUES (?, ?, ?)",
                                           scalar_data)

            sdf = []
            for key, value in self.scan_data_float.items():
                sdf.append((sid, key, value))
            cursor.executemany("INSERT INTO scan_fdata (sid, name, value) VALUES (?, ?, ?)", sdf)

            sds = []
            for key, value in self.scan_data_str.items():
                sds.append((sid, key, value))
            cursor.executemany("INSERT INTO scan_sdata (sid, name, value) VALUES (?, ?, ?)", sds)

            # Commit the transaction if we were able to successfully insert all the data.  Otherwise, an exception
            # should have been raised that was caught to rollback the transaction.
            conn.commit()
        except (mariadb.Error, Exception) as e:
            cursor.rollback()
            # TODO: better logging
            print(f"ERROR: {e}")
            # TODO: Alternative to catch and throw?
            raise e

    @staticmethod
    def analyze_signal(arr, sampling_rate=5000) -> Tuple[dict, dict]:

        """Computes basic statistical metrics and power spectrum for a single waveform (8192,)

        Args:
            arr (np.array): the array containing data
            sampling_rate (float): samping frequency represented by data in Hz

        Returns:
            Tuple[dict, dict]: dictionary of scalar statistical metrics, dictionary of arrays data
                               (e.g. power spectrum array)
        """

        if not isinstance(arr, (list, np.ndarray, tuple)):
            raise TypeError("Input must be a list, numpy array, or tuple.")

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
        f, Pxx_den = welch(arr, sampling_rate)

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


class WaveformDB:

    def __init__(self, host, user, password, port: int = 3306):
        self.host = host
        self.user = user
        self.port = port

        # Will throw exception if it cannot connect
        self.conn = mariadb.connect(
            host=self.host,
            port=self.port,
            user=self.user,
            password=password
        )
        self.conn.autocommit = False

    def __del__(self):
        self.conn.close()
