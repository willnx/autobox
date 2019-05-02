# -*- coding: UTF-8 -*-
"""A suite of unit tests for the ``Unity`` object"""
import unittest
from unittest.mock import patch, MagicMock

from stat_collector.lib import unity


class TestUnity(unittest.TestCase):
    """A suite of test cases for the ``Unity`` object"""
    @patch.object(unity.Unity, '_login')
    def test_init(self, fake_login):
        """``Unity` __init__ obtains the CSRF token"""
        u = unity.Unity('my.san.org', 'someAdmin', 'IloveCats')

        self.assertTrue(fake_login.called)

    @patch.object(unity.requests, 'Session')
    def test_login(self, fake_Session):
        """``Unity`` '_login' performs a GET to obtain a CSRF token"""
        fake_conn = MagicMock()
        fake_Session.return_value = fake_conn

        u = unity.Unity('my.san.org', 'someAdmin', 'IloveCats')

        the_args, _ = fake_conn.get.call_args
        url = the_args[0]
        expected = 'https://my.san.org/api/types/loginSessionInfo'

        self.assertTrue(fake_conn.get.called)
        self.assertEqual(url, expected)

    @patch.object(unity.Unity, '_login')
    def test_extract_csrf_token(self, fake_login):
        """``Unity`` '_extract_csrf_token' pulls the anti CSRF token from a response"""
        fake_resp = MagicMock()
        fake_resp.headers.get.return_value = 'someAntiCSRFtoken'
        u = unity.Unity('my.san.org', 'someAdmin', 'IloveCats')

        u._extract_csrf_token(fake_resp)
        actual_headers = u.headers
        expected = {'EMC-CSRF-TOKEN': 'someAntiCSRFtoken', 'X-EMC-REST-CLIENT': 'true'}

        self.assertEqual(actual_headers, expected)

    @patch.object(unity.Unity, '_login')
    def test_call(self, fake_login):
        """``Unity`` '_call' curries the request to ``requests.Session`` object"""
        fake_get = MagicMock()
        fake_session = MagicMock()
        fake_session.get = fake_get
        u = unity.Unity('my.san.org', 'someAdmin', 'IloveCats')
        u._session = fake_session

        resp = u._call('GET', '/some/endpoint')
        the_args, _ = fake_get.call_args
        end_point = the_args[0]
        expected = 'https://my.san.org/some/endpoint'

        self.assertTrue(fake_get.called)
        self.assertEqual(end_point, expected)

    @patch.object(unity.Unity, '_login')
    def test_call_uri(self, fake_login):
        """``Unity`` '_call' prepends a '/' if the end point arg doesn't have it"""
        fake_get = MagicMock()
        fake_session = MagicMock()
        fake_session.get = fake_get
        u = unity.Unity('my.san.org', 'someAdmin', 'IloveCats')
        u._session = fake_session

        resp = u._call('GET', 'some/endpoint')
        the_args, _ = fake_get.call_args
        end_point = the_args[0]
        expected = 'https://my.san.org/some/endpoint'

        self.assertEqual(end_point, expected)

    @patch.object(unity.Unity, '_login')
    def test_close_socket(self, fake_login):
        """``Unity`` the 'close' method terminate the TCP socket"""
        fake_session = MagicMock()
        u = unity.Unity('my.san.org', 'someAdmin', 'IloveCats')
        u._session = fake_session

        u.close()

        self.assertTrue(fake_session.close.called)

    @patch.object(unity.Unity, '_login')
    def test_close(self, fake_login):
        """``Unity`` the 'close' method deletes the session token"""
        fake_session = MagicMock()
        u = unity.Unity('my.san.org', 'someAdmin', 'IloveCats')
        u._session = fake_session

        u.close()

        self.assertTrue(fake_session.close.called)

    @patch.object(unity.Unity, '_login')
    def test_get(self, fake_login):
        """``Unity`` the 'get' method performs an HTTP GET request"""
        fake_session = MagicMock()
        u = unity.Unity('my.san.org', 'someAdmin', 'IloveCats')
        u._session = fake_session

        u.get('some/endpoint')

        self.assertTrue(fake_session.get.called)

    @patch.object(unity.Unity, '_login')
    def test_post(self, fake_login):
        """``Unity`` the 'post' method performs an HTTP POST request"""
        fake_session = MagicMock()
        u = unity.Unity('my.san.org', 'someAdmin', 'IloveCats')
        u._session = fake_session

        u.post('some/endpoint')

        self.assertTrue(fake_session.post.called)

    @patch.object(unity.Unity, '_login')
    def test_put(self, fake_login):
        """``Unity`` the 'put' method performs an HTTP PUT request"""
        fake_session = MagicMock()
        u = unity.Unity('my.san.org', 'someAdmin', 'IloveCats')
        u._session = fake_session

        u.put('some/endpoint')

        self.assertTrue(fake_session.put.called)

    @patch.object(unity.Unity, '_login')
    def test_delete(self, fake_login):
        """``Unity`` the 'delete' method performs an HTTP DELETE request"""
        fake_session = MagicMock()
        u = unity.Unity('my.san.org', 'someAdmin', 'IloveCats')
        u._session = fake_session

        u.delete('some/endpoint')

        self.assertTrue(fake_session.delete.called)


if __name__ == '__main__':
    unittest.main()
