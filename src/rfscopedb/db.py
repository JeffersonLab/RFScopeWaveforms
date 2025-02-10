import json
from datetime import datetime
from typing import Dict, Tuple, List, Union, Any

import numpy as np
import mariadb

from .utils import get_datetime_as_utc


class WaveformDB:
    valid_ops = (">", "<", "=", "!=", ">=", "<=")

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
        if self.conn is not None:
            self.conn.close()

    @classmethod
    def validate_ops(cls, filter_ops: List[str]):
        """Validate the selected database comparison operations against a pre-approved list.

        Args:
            filter_ops (List[str]): List of database comparison operations to validate.

        Raises:
            ValueError: If any item from filter_ops is an unsupported comparison.
        """
        # Validate the op clause.  The others can be part of standard prepared statement sanitization
        if filter_ops is not None and len(filter_ops) > 0:
            for op in filter_ops:
                if op not in cls.valid_ops:
                    raise ValueError(f"Invalid operation {op}")

    def query_scan_rows(self, begin: datetime = None, end: datetime = None,
                        filter_params: List[str] = None, filter_ops: List[str] = None,
                        filter_values: List[Union[float, str]] = None) -> List[Dict[str, Any]]:
        f"""Query scan data (sans waveforms) from the database and return it in an easy to process format.
        
        Note all filter_* parameters must be of the same length.

        Args:
            begin: The earliest scan start time for scans to be returned.  If None, there is no earliest cutoff.
            end: The latest scan start time for scans to be returned.  If None, there is no latest cutoff.
            filter_params: The name of the scan metadata to be filtered on (i.e., PV name).  If None, no filtering is
                           applied.
            filter_ops: The type of comparison to be made.  Supported ops are {self.valid_ops}. If None, no filtering is
                           applied.
            filter_values: The value to be compared against.  The comparisons are sanitized, but essentially follow
                           the <filter_param> <filter_op> <filter_value> pattern, e.g., R123GMES >= 2.0.  If None, no 
                           filtering is applied
                           
        Returns:
            A list of dictionaries containing the data for a single scan including metadata.
        """

        self.validate_ops(filter_ops)
        filters, data = self.get_scan_join_clauses(begin, end, filter_params, filter_ops, filter_values)

        sub_sql = f"SELECT scan.* FROM scan \n{filters}"
        s_sql = f"""
        SELECT t1.sid, t1.scan_start_utc, scan_sdata.ssid, scan_sdata.name as s_name, scan_sdata.value as s_value 
        FROM scan_sdata 
        JOIN ({sub_sql}) AS t1
            ON t1.sid = scan_sdata.sid"""

        f_sql = f"""
        SELECT t1.sid, t1.scan_start_utc, scan_fdata.sfid, scan_fdata.name as f_name, scan_fdata.value as f_value 
        FROM scan_fdata 
        JOIN ({sub_sql}) AS t1
            ON t1.sid = scan_fdata.sid"""

        out = []
        cursor = None
        try:
            cursor = self.conn.cursor(dictionary=True)
            cursor.execute(s_sql, data)
            for row in cursor:
                out.append(row)

            cursor.execute(f_sql, data)
            for row in cursor:
                out.append(row)

        finally:
            if cursor is not None:
                cursor.close()

        # Convert the row-per-metadata to row-per-scan.  Keep a single row as a dictionary for easy consumption.
        scan_meta = {}
        for row in out:
            sid = row['sid']
            if sid not in scan_meta:
                scan_meta[sid] = {}

            scan_meta[sid]['sid'] = sid
            scan_meta[sid]['scan_start_utc'] = row['scan_start_utc']
            if 's_name' in row:
                scan_meta[sid][f"s_{row['s_name']}"] = row['s_value']
            elif 'f_name' in row:
                scan_meta[sid][f"f_{row['f_name']}"] = row['f_value']

        return [val for val in scan_meta.values()]

    def query_waveform_data(self, sids: List[int], signal_names: List[str],
                            process_names: List[str]) -> List[Dict[str, Any]]:
        """Queries the waveform array data for a given set of sids, signal_names, and process_names.

        Results are stored internal to this object.

        Args:
            sids: A list of scan database identifiers to query waveform data
            signal_names: A list of the signal names to include data from  (GMES, PMES, etc.)
            process_names: A list of the process names to include data from (names of array transforms, e.g. raw
                           or power_spectrum)

        Returns:
            A list of dictionaries each containing the data for a single array of raw or process data from a waveform.
        """
        sid_params = ", ".join(["?" for i in range(len(sids))])
        signal_params = ", ".join(["?" for i in range(len(signal_names))])
        process_params = ", ".join(["?" for i in range(len(process_names))])
        sql = f"""
        SELECT * FROM waveform 
            JOIN waveform_adata 
                ON waveform.wid = waveform_adata.wid
                WHERE waveform.sid in ({sid_params}) 
                    AND waveform.signal_name IN ({signal_params})
                    AND waveform_adata.process IN ({process_params})
"""
        cursor = None
        try:
            data = sids + signal_names + process_names
            cursor = self.conn.cursor(dictionary=True)
            cursor.execute(sql, data)

            rows = []
            for row in cursor:
                row['data'] = np.array(json.loads(row['data']))
                rows.append(row)

        finally:
            if cursor is not None:
                cursor.close()

        return rows

    def query_waveform_metadata(self, sids: List[int], signal_names: List[str],
                                metric_names: List[str]) -> List[Dict[str, Any]]:
        """Queries the waveform scalar metadata for a given set of sids, signal_names, and metric names.

        Results are stored internal to this object.

        Args:
            sids: A list of scan database identifiers to query waveform data
            signal_names: A list of the signal names to include data from  (GMES, PMES, etc.)
            metric_names: A list of the scalar metad to include in the output (mean, median, etc.).  If None, get all.

        Returns:
            A list of dictionaries each containing the scalar metadata for a single waveform.
        """
        sid_params = ", ".join(["?" for i in range(len(sids))])
        signal_params = ", ".join(["?" for i in range(len(signal_names))])

        sql = f"""
        SELECT * FROM waveform 
            JOIN waveform_sdata 
                ON waveform.wid = waveform_sdata.wid
                WHERE waveform.sid in ({sid_params}) 
                    AND waveform.signal_name IN ({signal_params})
        """

        data = sids + signal_names
        if metric_names is not None and len(metric_names) > 0:
            meta_params = ", ".join(["?" for i in range(len(metric_names))])
            sql += f" AND waveform_sdata.name IN ({meta_params})"
            data += metric_names

        cursor = None
        try:
            cursor = self.conn.cursor(dictionary=True)
            cursor.execute(sql, data)
            rows = []
            for row in cursor:
                rows.append(row)

        finally:
            if cursor is not None:
                cursor.close()

        # Convert the row-per-metadata to row-per-waveform.  Keep a single row as a dictionary for easy consumption.
        meta = {}
        for row in rows:
            wid = row['wid']
            if wid not in meta:
                meta[wid] = {}

            # one sid maps to many wids, but each wids maps to one sid
            meta[wid]['wid'] = wid
            meta[wid]['sid'] = row['sid']
            meta[wid]['cavity'] = row['cavity']
            meta[wid]['signal_name'] = row['signal_name']
            meta[wid]['comment'] = row['comment']
            meta[wid]['sample_rate_hz'] = row['sample_rate_hz']
            meta[wid][row['name']] = row['value']

        return [val for val in meta.values()]

    @staticmethod
    def gen_scan_join_statements(tests: List[Tuple[str, str, Any]]) -> Tuple[str, List[Any]]:
        """Generate a JOIN/WHERE statement that will filter out scan IDs that don't match the given test.

        Note: this method checks the type of the comparison target and builds the SQL statement around the table for
        that type.

        Args:
            tests: A List of 3-tuples of format (metadata_name, comparison, comparison_target), e.g.
                   ("R2XXITOT", ">=", "10.0")
        Returns:
             Two items, the compound JOIN statement and a list of data values to be used in the prepared statement query
        """
        sql = ""
        data = []
        for idx, item in enumerate(tests):
            WaveformDB.validate_ops([item[1]])
            data.append(item[0])
            data.append(item[2])
            table = "scan_fdata"
            if isinstance(item[2], str):
                table = "scan_sdata"
            sql += f" JOIN (SELECT {table}.sid FROM {table} WHERE name = "
            sql += f"? and value {item[1]} ?) as s{idx} ON scan.sid = s{idx}.sid\n"
        return sql, data

    @classmethod
    def get_scan_join_clauses(cls, begin: datetime, end: datetime, filter_params: List[str], filter_ops: List[str],
                              filter_values: List[Union[float, str]]) -> tuple[str, List[str]]:
        f"""Generates JOIN/WHERE clauses for efficiently filtering scans by its metadata.

        Args:
            begin: The earliest scan start time
            end: The latest scan end time
            filter_params: List of parameter names of scan meta data on which to filter
            filter_ops: List of operations to apply to filter.  Valid: {cls.valid_ops}
            filter_values: List of values to compare filter_params on using filter_ops.

        Returns:
            A string of JOIN/WHERE statements 
        """
        cls.validate_ops(filter_ops)

        # scan_tests, meta_tests = WaveformDB.get_tests(begin, end, filter_params, filter_ops, filter_values)
        # print(scan_tests)

        meta_tests = []
        data = []

        # Process the other filters on scan metadata.  Split up the string based values from the numeric values since
        # they are in different tables.
        if filter_params is not None:
            if (len(filter_params) != len(filter_values)) or (len(filter_params) != len(filter_ops)):
                raise ValueError("Filter_params must have same length as filter_ops and filter_values.")

            for item in zip(filter_params, filter_ops, filter_values):
                meta_tests.append((item[0], item[1], item[2]))

        sql, data = WaveformDB.gen_scan_join_statements(meta_tests)

        scan_tests = []
        if begin is not None:
            scan_tests.append(("scan.scan_start_utc", ">=",
                               get_datetime_as_utc(begin).strftime("%Y-%m-%d %H:%M:%S.%f")))
        if end is not None:
            scan_tests.append(("scan.scan_start_utc", "<=",
                               get_datetime_as_utc(end).strftime("%Y-%m-%d %H:%M:%S.%f")))

        if len(scan_tests) != 0:
            sql += f" WHERE {scan_tests[0][0]} {scan_tests[0][1]} ?"
            data.append(scan_tests[0][2])
            for i in range(1, len(scan_tests)):
                sql += f" AND {scan_tests[i][0]} {scan_tests[i][1]} ?"
                data.append(scan_tests[i][2])

        return sql, data

    def delete_scans(self, sid: int) -> int:
        """Delete a single scan and all associated data (including waveforms) from the database.

        Note: this requires DELETE permissions which may not be available on standard usage.

        Args:
            sid: The scan ID of the scan to be deleted

        Returns:
            The number of deleted scans.
        """

        cursor = None
        sql = "DELETE FROM scan WHERE sid = ?"
        try:
            self.conn.begin()
            cursor = self.conn.cursor()
            cursor.execute(sql, (sid,))
            count = cursor.rowcount
            self.conn.commit()
        finally:
            if cursor is not None:
                cursor.close()
        return count
