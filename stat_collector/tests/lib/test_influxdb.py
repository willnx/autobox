# -*- coding: UTF-8 -*-
"""A suite of unit tests for the ``InfluxDB`` object"""
import unittest
from unittest.mock import patch, MagicMock

from stat_collector.lib import influxdb

# Patch requests.Session in every test case
@patch.object(influxdb, 'Session')
class TestInfluxDB(unittest.TestCase):
    """A suite of test cases for the ``InfluxDB`` object"""

    def test_init(self, fake_Session):
        """``InfluxDB`` constructs an HTTP session object upon creation"""
        fake_session = MagicMock()
        fake_Session.return_value = fake_session

        influx = influxdb.InfluxDB(server='no-where.org',
                                   user='sam',
                                   password='iLoveKats!',
                                   measurement='someThing')

        self.assertTrue(influx.session is fake_session)

    @patch.object(influxdb.time, 'time')
    def test_write(self, fake_time, fake_Session):
        """``InfluxDB.write`` generates a timestamp if one is not given"""
        fake_time.return_value = 1234
        influx = influxdb.InfluxDB(server='no-where.org',
                                   user='sam',
                                   password='iLoveKats!',
                                   measurement='someThing')
        influx._last_write = 1234

        influx.write(fields={'cpu':'23'}, tags={'host':'myComputer'})
        timestamp = influx._staged[0]['timestamp']
        expected = 1234

        self.assertEqual(expected, timestamp)

    @patch.object(influxdb.time, 'time')
    def test_write_flush_time(self, fake_time, fake_Session):
        """``InfluxDB.write`` sends the pending data to Influx after 10 seconds"""
        fake_time.return_value = 1234
        fake_session = MagicMock()
        fake_Session.return_value = fake_session
        influx = influxdb.InfluxDB(server='no-where.org',
                                   user='sam',
                                   password='iLoveKats!',
                                   measurement='someThing')
        influx._last_write = 1224
        influx.first_write = False

        influx.write(fields={'cpu':'23'}, tags={'host':'myComputer'})

        self.assertTrue(fake_session.post.called)

    @patch.object(influxdb.time, 'time')
    def test_write_flust_points(self, fake_time, fake_Session):
        """``InfluxDB.write`` sends pending data if there are more than 5000 data points"""
        fake_time.return_value = 1234
        fake_session = MagicMock()
        fake_Session.return_value = fake_session
        influx = influxdb.InfluxDB(server='no-where.org',
                                   user='sam',
                                   password='iLoveKats!',
                                   measurement='someThing')
        influx._staged = [{'tags': {'host': 'myComputer'}, 'fields': {'cpu': '23'}, 'timestamp': 1234}] * 5000
        influx.first_write = False

        influx.write(fields={'cpu':'23'}, tags={'host':'myComputer'})

        self.assertTrue(fake_session.post.called)

    @patch.object(influxdb.time, 'time')
    def test_supplied_timestamp(self, fake_time, fake_Session):
        """``InfluxDB.write`` will not generate a timestamp if one is supplied"""
        fake_time.return_value = 1234
        influx = influxdb.InfluxDB(server='no-where.org',
                                   user='sam',
                                   password='iLoveKats!',
                                   measurement='someThing')
        influx._last_write = 1234

        influx.write(fields={'cpu':'23'}, tags={'host':'myComputer'}, timestamp=9001)
        timestamp = influx._staged[0]['timestamp']
        expected = 9001

        self.assertEqual(timestamp, expected)

    @patch.object(influxdb.time, 'time')
    def test_tags_optional(self, fake_time, fake_Session):
        """``InfluxDB.write`` the param 'tags' is optional"""
        fake_time.return_value = 1234
        influx = influxdb.InfluxDB(server='no-where.org',
                                   user='sam',
                                   password='iLoveKats!',
                                   measurement='someThing')
        influx._last_write = 1234

        influx.write(fields={'cpu':'23'})
        tags = influx._staged[0]['tags']
        expected = None

        self.assertEqual(tags, expected)

    @patch.object(influxdb.time, 'time')
    def test_fist_write(self, fake_time, fake_Session):
        """``InfluxDB.write`` does not send a single data point when adding the first few writes"""
        fake_time.return_value = 1200
        influx = influxdb.InfluxDB(server='no-where.org',
                                   user='sam',
                                   password='iLoveKats!',
                                   measurement='someThing')


        influx.write(fields={'cpu':'23'})
        influx.write(fields={'cpu':'23'})

        self.assertFalse(influx.first_write)
        self.assertFalse(influx.session.post.called)

    def test_http_error(self, fake_Session):
        """``InfluxDB.flush`` raises 'InfluxError' if the HTTP response indicates an error"""
        fake_resp = MagicMock()
        fake_resp.ok = False
        fake_resp.json.return_value = {'decoded' : 'JSON'}
        influx = influxdb.InfluxDB(server='no-where.org',
                                   user='sam',
                                   password='iLoveKats!',
                                   measurement='someThing')
        influx.session.post.return_value = fake_resp

        with self.assertRaises(influxdb.InfluxError):
            influx.flush()

    def test_http_error_not_json(self, fake_Session):
        """``InfluxDB.flush`` raises 'InfluxError' if the HTTP error isn't in JSON format"""
        fake_resp = MagicMock()
        fake_resp.ok = False
        fake_resp.json.side_effect = Exception('doh')
        influx = influxdb.InfluxDB(server='no-where.org',
                                   user='sam',
                                   password='iLoveKats!',
                                   measurement='someThing')
        influx.session.post.return_value = fake_resp

        with self.assertRaises(influxdb.InfluxError):
            influx.flush()


class TestInfluxError(unittest.TestCase):
    """A suite of test cases for the ``InfluxError`` exception"""
    def test_message(self):
        """``InfluxError`` message contains an error and HTTP status"""
        error = influxdb.InfluxError('my error', 419)

        expected = 'Status Code: 419, Error: my error'
        actual = '{}'.format(error)

        self.assertEqual(expected, actual)


class TestFormatData(unittest.TestCase):
    """A suit of test cases for the ``_format_data`` function"""

    def test_no_tags(self):
        """``_format_data`` correctly formats when supplied with no tags"""
        data = [{'tags': None, 'fields': {'cpu': '23'}, 'timestamp': 1234}]

        output = influxdb._format_data(data, measurement='someThing')
        expected = 'someThing cpu=23 1234'

        self.assertEqual(output, expected)

    def test_tags(self):
        """``_format_data`` correctly formats when supplied with tags"""
        data = [{'tags': {'foo': 'bar'}, 'fields': {'cpu': '23'}, 'timestamp': 1234}]

        output = influxdb._format_data(data, measurement='someThing')
        expected = 'someThing,foo=bar cpu=23 1234'

        self.assertEqual(output, expected)

    def test_many(self):
        """``_format_data`` delimits data points with the newline char"""
        data = [{'tags': {'foo': 'bar'}, 'fields': {'cpu': '23'}, 'timestamp': 1234}] * 2

        output = influxdb._format_data(data, measurement='someThing')
        expected = 'someThing,foo=bar cpu=23 1234\nsomeThing,foo=bar cpu=23 1234'

        self.assertEqual(output, expected)


if __name__ == '__main__':
    unittest.main()
