"""This module contains classes related to performing operations on data that already exists within the database.

New data that is to be written to the database should be handled by the objects containing that data.
"""

import json
from datetime import datetime
from typing import Dict, Tuple, List, Any, Optional

import numpy as np
import mysql.connector

from .utils import get_datetime_as_utc


class QueryFilter:
    """This class is used to construct multipart where clauses.

    This is intended for use filtering scans, but it could potentially be used in other scenarios.
    """
    valid_ops = (">", "<", "=", "!=", ">=", "<=")

    def __init__(self, filter_params, filter_ops, filter_values):
        """An object that contains the filter rules to be applied to a database query.

        The three filter_* parameters work in conjunction.  The must be list-like objects of the same length.  At
        a given index, a filter is later constructed such that "<param> <op> <value>" is used in a SQL WHERE clause.
        For example, if filter_params = ['R123GMES', 'R223GMES'], filter_ops = ['>', '<'], and
        filter_values = [5, 2], then each scan included in the query must meet the following criteria:
            R123GMES > 5 AND R223GMES < 2

        Args:
            filter_params: The name of the scan metadata to be filtered on (i.e., PV name).  If None, no filtering is
                           applied.
            filter_ops: The type of comparison to be made.  Supported ops are {self.valid_ops}. If None, no filtering is
                           applied.
            filter_values: The value to be compared against.  The comparisons are sanitized, but essentially follow
                           the <filter_param> <filter_op> <filter_value> pattern, e.g., R123GMES >= 2.0.  If None, no
                           filtering is applied
        """

        if (filter_params is not None) or (filter_ops is not None) or (filter_values is not None):
            if (len(filter_params) != len(filter_values)) or (len(filter_params) != len(filter_ops)):
                raise ValueError("Filter_params must have same length as filter_ops and filter_values.")

        self.params = filter_params
        self.ops = filter_ops
        self.values = filter_values
        self.validate_ops()

    def validate_ops(self):
        """Validate the selected database comparison operations against a pre-approved list.

        Raises:
            ValueError: If any item from filter_ops is an unsupported comparison.
        """
        # Validate the op clause.  The others can be part of standard prepared statement sanitization
        if self.ops is not None and len(self.ops) > 0:
            for op in self.ops:
                if op not in self.valid_ops:
                    raise ValueError(f"Invalid operation {op}")

    def __len__(self) -> int:
        """Return the number of conditional clauses that will be generated."""
        if self.params is None:
            return 0
        return len(self.params)


class WaveformDB:
    """A class that handles operations on data that already exists within the database.

    This class will manage the connection lifecycle.
    """

    def __init__(self, host: str, user: str, password: str, *, port: int = 3306, database="scope_waveforms"):
        self.host = host
        self.user = user
        self.port = port
        self.database = database

        # Prevents error on del if creating connection fails.
        self.conn = None
        # Will throw exception if it cannot connect
        self.conn = mysql.connector.connect(
            host=self.host,
            port=self.port,
            user=self.user,
            password=password,
            database=self.database,
            autocommit=False
        )
        self.conn.autocommit = False

    def __del__(self):
        if self.conn is not None:
            self.conn.close()

    # noinspection PyTypeChecker
    def query_scan_rows(self, begin: datetime = None, end: datetime = None, q_filter: QueryFilter = None
                        ) -> List[Dict[str, Any]]:
        """Query scan data (sans waveforms) from the database and return it in an easy to process format.
        
        Note all filter_* parameters must be of the same length.

        Args:
            begin: The earliest scan start time for scans to be returned.  If None, there is no earliest cutoff.
            end: The latest scan start time for scans to be returned.  If None, there is no latest cutoff.
            q_filter: The filter to apply to the scan data.

                           
        Returns:
            A list of dictionaries containing the data for a single scan including metadata.
        """

        filters, data = self.get_scan_join_clauses(begin, end, q_filter)

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

        return list(scan_meta.values())

    # noinspection PyTypeChecker
    def query_waveform_data(self, sids: List[int], signal_names: Optional[List[str]],
                            array_names: Optional[List[str]]) -> List[Dict[str, Any]]:
        """Queries the waveform array data for a given set of sids, signal_names, and array_names.

        Results are stored internal to this object.

        Args:
            sids: A list of scan database identifiers to query waveform data
            signal_names: A list of the signal names to include data from  (GMES, PMES, etc.).  If None, all signals are
                          queried.
            array_names: A list of the array names to include data from (names of array transforms, e.g. raw
                           or power_spectrum). If None, all array types are queried.

        Returns:
            A list of dictionaries each containing the data for a single array of raw or processed data from a waveform.
        """
        if sids is None or len(sids) == 0:
            raise ValueError("Must specify at least one sid")

        data = sids
        sid_params = ", ".join(["%s" for _ in range(len(sids))])
        sql = f"""
        SELECT * FROM waveform 
            JOIN waveform_adata 
                ON waveform.wid = waveform_adata.wid
                WHERE waveform.sid in ({sid_params})
                """
        if signal_names is not None and len(signal_names) > 0:
            data += signal_names
            signal_params = ", ".join(["%s" for _ in range(len(signal_names))])
            sql += f"AND waveform.signal_name IN ({signal_params})\n"

        if array_names is not None and len(array_names) > 0:
            data += array_names
            array_name_params = ", ".join(["%s" for _ in range(len(array_names))])
            sql += f"AND waveform_adata.name IN ({array_name_params})\n"

        cursor = None
        try:
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

    # noinspection PyTypeChecker
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
        sid_params = ", ".join(["%s" for _ in range(len(sids))])
        signal_params = ", ".join(["%s" for _ in range(len(signal_names))])

        sql = f"""
        SELECT * FROM waveform 
            JOIN waveform_sdata 
                ON waveform.wid = waveform_sdata.wid
                WHERE waveform.sid in ({sid_params}) 
                    AND waveform.signal_name IN ({signal_params})
        """

        data = sids + signal_names
        if metric_names is not None and len(metric_names) > 0:
            meta_params = ", ".join(["%s" for _ in range(len(metric_names))])
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

        return list(meta.values())

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
            data.append(item[0])
            data.append(item[2])
            table = "scan_fdata"
            if isinstance(item[2], str):
                table = "scan_sdata"
            sql += f" JOIN (SELECT {table}.sid FROM {table} WHERE name = "
            sql += f"%s and value {item[1]} %s) as s{idx} ON scan.sid = s{idx}.sid\n"
        return sql, data

    @classmethod
    def get_scan_join_clauses(cls, begin: datetime, end: datetime, q_filter: QueryFilter) -> tuple[str, List[str]]:
        """Generates JOIN/WHERE clauses for efficiently filtering scans by its metadata.

        Args:
            begin: The earliest scan start time
            end: The latest scan end time
            q_filter: The filter to apply to the query

        Returns:
            A string of JOIN/WHERE statements 
        """
        meta_tests = []

        # Process the other filters on scan metadata.  Split up the string based values from the numeric values since
        # they are in different tables.
        if q_filter is not None and len(q_filter) > 0:
            for item in zip(q_filter.params, q_filter.ops, q_filter.values):
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
            sql += f" WHERE {scan_tests[0][0]} {scan_tests[0][1]} %s"
            data.append(scan_tests[0][2])
            for i in range(1, len(scan_tests)):
                sql += f" AND {scan_tests[i][0]} {scan_tests[i][1]} %s"
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
        sql = "DELETE FROM scan WHERE sid = %s"
        try:
            # First command begins a transaction when autocommit == False
            cursor = self.conn.cursor()
            cursor.execute(sql, (sid,))
            count = cursor.rowcount
            self.conn.commit()
        finally:
            if cursor is not None:
                cursor.close()
        return count
