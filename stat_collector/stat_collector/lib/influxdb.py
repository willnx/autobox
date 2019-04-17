# -*- coding: UTF-8 -*-
"""Abstact the InfluxDB API"""
import time

from requests import Session
from stat_collector.lib.std_logger import get_logger


class InfluxError(Exception):
    """Raised when unable to write to InfluxDB"""
    def __init__(self, error, status_code):
        msg = 'Status Code: {}, Error: {}'.format(status_code, error)
        super().__init__(msg)


class InfluxDB:
    def __init__(self, server, user, password, measurement, database='vlab'):
        self._server = server
        self.url = 'https://{}:8086/write'.format(self._server)
        self._creds = (user, password)
        self._db = database
        self.session = Session()
        self._staged = []
        self._last_write = 0
        self._measurement = measurement
        self.headers = {'Content-Type': 'application/octet-stream'}
        self.params = {'db' : self._db, 'precision' : 's'} # seconds
        self.first_write = True
        self.log = get_logger('Influx')

    def write(self, fields, tags=None, timestamp=None):
        """Add a data point to InfluxDB.

        Call ``flush`` to immedately send, but it note that you will pay a hefty
        performance penalty if you constantly ``flush`` the data manually.

        :Returns: None

        :param fields: A required dictionary of values to write to InfluxDB
        :type fields: Dictionary

        :param tags: A option dictionary of strings to create indexes on in InfluxDB
        :type tags: Dictionary
        """
        write_time = int(time.time())
        if timestamp is None:
            timestamp = write_time
        self._staged.append({'tags' : tags, 'fields' : fields, 'timestamp' : timestamp})
        last_write_delta = write_time - self._last_write
        # InfluxDB docs say to write in batches of 5,000 for optimal perf
        # but I don't want to lose more than 10sec of history
        if len(self._staged) > 5000 or last_write_delta >= 10:
            if self.first_write:
                # Fixes bootstrap error when inital write after object creation comes in
                self.first_write = False
                self._last_write = write_time
            else:
                self.flush(write_time=write_time)

    def flush(self, write_time=0):
        """Construct the data points into the correct format, and send to InfluxDB

        :Returns: None

        :param write_time: Optionally supply the EPOCH timestamp of when the write is sent
        :type write_time: Integer
        """
        payload = _format_data(self._staged, self._measurement)
        resp = self.session.post(self.url, headers=self.headers, auth=self._creds, params=self.params, data=payload, verify=False)
        if not resp.ok:
            try:
                error = resp.json()
            except Exception:
                error = resp.content
            raise InfluxError(error, resp.status_code)
        self._last_write = write_time
        self._staged = []


def _format_data(influx_data, measurement):
    """Format the supplied data into the InfluxDB Line Protocol format

    :Returns: String

    :param influx_data: The list of data points to convert. Elements MUST be dictionaries
                        with the keys 'tags', 'fields', 'timestamp'.
    :type influx_data: List

    :param measurement: The measurement in InfluxDB to add the data points to
    :type measurement: Sting
    """
    chunks = []
    for data_point in influx_data:
        if data_point['tags'] is not None:
            tags = ','.join(['{}={}'.format(k,v) for k,v in data_point['tags'].items()])
            beginning = "{},{}".format(measurement, tags)
        else:
            beginning = measurement
        fields = ','.join(['{}={}'.format(k,v) for k,v in data_point['fields'].items()])
        chunks.append('{} {} {}'.format(beginning, fields, data_point['timestamp']))
    return '\n'.join(chunks)
