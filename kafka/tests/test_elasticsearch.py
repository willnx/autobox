# -*- coding: UTF-8 -*-
"""A suite of unit tests for the ``elasticsearch`` module"""
import unittest
from unittest.mock import patch, MagicMock
import time

from log_processor import elasticsearch


class TestElasticSearch(unittest.TestCase):
    """A suite of test cases for the ElasticSearch object"""
    def test_init(self):
        """``ElasticSearch`` does not IO upon __init__ of object"""
        es = elasticsearch.ElasticSearch(server='8.8.8.8',
                                         user='alice',
                                         password='iLoveDogs',
                                         doc_type='someLogCategory')

        self.assertTrue(isinstance(es, elasticsearch.ElasticSearch))

    def test_index(self):
        """``ElasticSearch`` the 'index' property has the correct daily format"""
        es = elasticsearch.ElasticSearch(server='8.8.8.8',
                                         user='alice',
                                         password='iLoveDogs',
                                         doc_type='someLogCategory')

        index = es.index
        expected = time.strftime('logs-%Y.%m.%d')

        self.assertEqual(index, expected)

    @patch.object(elasticsearch.requests, 'Session')
    def test_write(self, fake_Session):
        """``ElasticSearch`` 'write' checks that the HTTP response was OK automatically"""
        fake_resp = MagicMock()
        fake_session = MagicMock()
        fake_session.post.return_value = fake_resp
        fake_Session.return_value = fake_session
        es = elasticsearch.ElasticSearch(server='8.8.8.8',
                                         user='alice',
                                         password='iLoveDogs',
                                         doc_type='someLogCategory')

        es.write(document='{"some":"JSON"}')

        self.assertTrue(fake_resp.raise_for_status.called)

    @patch.object(elasticsearch.requests, 'Session')
    def test_write_url(self, fake_Session):
        """``ElasticSearch`` 'write' constructs the correct URL"""
        fake_resp = MagicMock()
        fake_session = MagicMock()
        fake_session.post.return_value = fake_resp
        fake_Session.return_value = fake_session
        es = elasticsearch.ElasticSearch(server='8.8.8.8',
                                         user='alice',
                                         password='iLoveDogs',
                                         doc_type='someLogCategory')

        es.write(document='{"some":"JSON"}')

        the_args, _ = fake_session.post.call_args
        url = the_args[0]
        expected = 'https://8.8.8.8:9200/{}/someLogCategory'.format(time.strftime('logs-%Y.%m.%d'))

        self.assertEqual(url, expected)


    @patch.object(elasticsearch.requests, 'Session')
    def test_close(self, fake_Session):
        """``ElasticSearch`` 'close' terminates the TCP socket with the ElasticSearch server"""
        fake_resp = MagicMock()
        fake_session = MagicMock()
        fake_session.post.return_value = fake_resp
        fake_Session.return_value = fake_session
        es = elasticsearch.ElasticSearch(server='8.8.8.8',
                                         user='alice',
                                         password='iLoveDogs',
                                         doc_type='someLogCategory')

        es.close()

        self.assertTrue(fake_session.close.called)


if __name__ == '__main__':
    unittest.main()
