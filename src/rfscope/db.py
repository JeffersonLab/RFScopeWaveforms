import json
from datetime import datetime, timezone
from typing import Dict, Tuple, List, Union

import numpy as np
import mariadb
from numpy.core.multiarray import ndarray
from scipy.signal import periodogram


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
        scan_time = self.dt.astimezone(timezone.utc)
        cursor = conn.cursor()
        try:
            # Note: execute and executemany do sanitation and prepared statements internally.
            cursor.execute("INSERT INTO scan (scan_start_utc)  VALUES (?)", (scan_time.strftime('%Y-%m-%d %H:%M:%S'),))
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
                    array_data = []
                    array_data.append((wid, "raw", json.dumps(self.waveform_data[cav][signal_name].tolist())))
                    for arr_name, array in self.analysis_array[cav][signal_name].items():
                        array_data.append(
                            (wid, arr_name, json.dumps(self.analysis_array[cav][signal_name][arr_name].tolist())))

                    cursor.executemany("INSERT INTO waveform_adata (wid, name, data) VALUES (?, ?, ?)", array_data)

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
            # should have been raised that was caught to rollback the transaction.
            conn.commit()
        except (mariadb.Error, Exception) as e:
            conn.rollback()
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


class WaveformDB:
    valid_ops = (">", ">=", "<", "<=", "==", "!=")

    def __init__(self, host, user, password, port: int = 3306, database="scope_waveforms"):
        self.host = host
        self.user = user
        self.port = port
        self.database = database

        # Will throw exception if it cannot connect
        self.conn = mariadb.connect(
            host=self.host,
            port=self.port,
            user=self.user,
            password=password,
            database=self.database
        )
        self.conn.autocommit = False

    def __del__(self):
        self.conn.close()

    @classmethod
    def get_scan_filters(cls, begin: datetime, end: datetime, filter_params: List[str], filter_ops: List[str],
                         filter_values: List[Union[float, str]]) -> Tuple[str, str, str]:
        f"""Generates WHERE clauses for querying data from the scan table and scan metadata tables.

        Args:
            begin: The earliest scan start time
            end: The latest scan end time
            filter_params: List of parameter names of scan meta data on which to filter
            filter_ops: List of operations to apply to filter.  Valid: {cls.valid_ops}
            filter_values: List of values to compare filter_params on using filter_ops.
            
        Returns:
            scan table WHERE clause, scan_fdata table WHERE clause, scan_sdata table WHERE clause
        """

        # Build a list of the different tests to be used in SQL WHERE clauses
        scan_tests = []
        scan_s_tests = []
        scan_f_tests = []

        # Process the begin/end filter on the scan table
        if begin is not None:
            if begin.tzinfo is None:
                begin = begin.astimezone(timezone.utc).replace(tzinfo=timezone.utc)
            scan_tests.append(f"scan_start_utc >= {begin.strftime('%Y-%m-%d %H:%M:%S.%f')}")

        if end is not None:
            if end.tzinfo is None:
                end = end.astimezone(timezone.utc).replace(tzinfo=timezone.utc)
            scan_tests.append(f"scan_start_utc <= {end.strftime('%Y-%m-%d %H:%M:%S.%f')}")

        # Process the other filters on scan metadata.  Split up the string based values from the numeric values since
        # they are in different tables.
        if filter_params is not None:
            if (len(filter_params) != len(filter_values)) or (len(filter_params) != len(filter_ops)):
                raise ValueError("Filter_params must have same length as filter_ops and filter_values.")

            for item in zip(filter_params, filter_ops, filter_values):
                if item[1] not in cls.valid_ops:
                    raise ValueError(f"Invalid operation {item[1]}")
                else:
                    if isinstance(item[2], str):
                        scan_s_tests.append(f"s_{item[0]} {item[1]} '{item[2]}'")
                    else:
                        scan_f_tests.append(f"f_{item[0]} {item[1]} {item[2]}")

        sql = WaveformDB.gen_where_clause(scan_tests + scan_f_tests + scan_s_tests)

        return sql

    @staticmethod
    def gen_where_clause(tests: List[str]):
        """Simple function that generates a WHERE clause given a list of tests (e.g. 'A > 73' )"""
        sql = ""
        if len(tests) > 0:
            sql = "WHERE " + tests[0]
            for idx, test in enumerate(tests):
                if idx == 0:
                    continue
                sql += " AND " + test
        return sql

    # TODO: Process the results and return them
    def test_query_multi(self, begin: datetime = None, end: datetime = None, filter_params: List[str] = None,
                                    filter_ops: List[str] = None, filter_values: List[Union[float, str]] = None):
        """Query scans using multiple queries."""

        sql1 = """SELECT
    GROUP_CONCAT(DISTINCT CONCAT('SUM(CASE WHEN scan_fdata.name = ''', name, ''' THEN scan_fdata.value ELSE NULL END) AS `f_', name, '`'))
FROM scan_fdata"""
        cursor = self.conn.cursor()
        cursor.execute(sql1)
        f1 = cursor.fetchone()[0]

        sql2 = """SELECT
            GROUP_CONCAT(DISTINCT CONCAT('SUM(CASE WHEN scan_sdata.name = ''', name, ''' THEN scan_sdata.value ELSE NULL END) AS `s_', name, '`'))
        FROM scan_sdata"""
        cursor = self.conn.cursor()
        cursor.execute(sql2)
        f2 = cursor.fetchone()[0]

        # Get the filter clauses if they exist
        where = self.get_scan_filters(begin, end, filter_params, filter_ops, filter_values)

        sql = f"""SELECT * FROM (SELECT scan.*, 
                    {f1},
                    {f2}
                   FROM scan_fdata
                     JOIN scan on scan.sid = scan_fdata.sid 
                       JOIN scan_sdata ON scan.sid = scan_sdata.sid 
                   
                   GROUP BY scan_fdata.sid
                   ORDER BY scan_fdata.sid
                   ) as t
                   {where}
"""

        # For debug
        print(sql)

        cursor.execute(sql)
        for row in cursor:
            print(row)

    # TODO: Figure out why this doesn't work and the flesh it out.  It is only the bones of the query for now.
    def test_query_single(self):

        sql = """-- Step 1: Get unique values to pivot
SET @sql1 = NULL;
SET @sql2 = NULL;
SELECT GROUP_CONCAT(DISTINCT
                    CONCAT('SUM(CASE WHEN scan_fdata.name = ''', name, ''' THEN scan_fdata.value ELSE NULL END) AS `f_',
                           name, '`'))
INTO @sql1
FROM scan_fdata;

SELECT GROUP_CONCAT(DISTINCT
                    CONCAT('SUM(CASE WHEN scan_sdata.name = ''', name, ''' THEN scan_sdata.value ELSE NULL END) AS `s_',
                           name, '`'))
INTO @sql2
FROM scan_sdata;

-- Step 2: Construct the dynamic query
SET @query = CONCAT('SELECT scan.scan_start_utc, scan_fdata.sid, ', @sql1, ", ", @sql2, '
                     FROM scan_fdata 
                       JOIN scan ON scan.sid = scan_fdata.sid
                         JOIN scan_sdata ON scan.sid = scan_sdata.sid
                     GROUP BY scan_fdata.sid');

SELECT @query;

-- Step 3: Prepare and execute the dynamic query
PREPARE stmt FROM @query;
EXECUTE stmt;
DEALLOCATE PREPARE stmt;
"""
        cursor = self.conn.cursor()
        cursor.execute(sql)
        for row in cursor.fetchall():
            print(row)
