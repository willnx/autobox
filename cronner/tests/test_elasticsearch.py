# -*- coding: UTF-8 -*-
"""A suite of unit tests for the ``elasticsearch`` module"""
import unittest
from unittest.mock import patch, MagicMock

from cronner import elasticsearch as es

@patch('cronner.elasticsearch.logger')
@patch('cronner.elasticsearch._get_indices')
@patch('cronner.elasticsearch._call_es')
class TestPruneIndexes(unittest.TestCase):
    """A suite of test cases for the ``prune_indices`` function"""
    def test_no_prune(self, fake_call_es, fake_get_indices, fake_logger):
        """``prune_indices`` only deletes an index when there are too many"""
        fake_get_indices.return_value = {'logs-2016.05.12', 'logs-2016.01.01'}

        es.prune_indices(max_log_records=100)

        deleted = fake_call_es.call_count
        expected = 0

        self.assertEqual(deleted, expected)

    def test_prune_oldes(self, fake_call_es, fake_get_indices, fake_logger):
        """``prune_indices`` deletes the oldest index"""
        fake_get_indices.return_value = {'logs-2016.05.12', 'logs-2016.01.01'}

        es.prune_indices(max_log_records=1)

        the_args, _ = fake_call_es.call_args
        delete_url = the_args[0]
        expected = 'https://vlab-data.emc.com:9200/logs-2016.01.01'

        self.assertEqual(delete_url, expected)

    def test_prune_error(self, fake_call_es, fake_get_indices, fake_logger):
        """``prune_indices`` logs failures to delete an index"""
        fake_get_indices.return_value = {'logs-2016.05.12', 'logs-2016.01.01'}
        fake_resp = MagicMock()
        fake_resp.ok = False
        fake_resp.status_code = 418
        fake_resp.content = "I'm a tea pot :D"
        fake_call_es.return_value = fake_resp

        es.prune_indices(max_log_records=1)

        errors_logged = fake_logger.error.call_count
        expected_errors = 1

    def test_prune_error_msg(self, fake_call_es, fake_get_indices, fake_logger):
        """``prune_indices`` the log message to delete an index contains the index name"""
        fake_get_indices.return_value = {'logs-2016.05.12', 'logs-2016.01.01'}
        fake_resp = MagicMock()
        fake_resp.ok = False
        fake_resp.status_code = 418
        fake_resp.content = "I'm a tea pot :D"
        fake_call_es.return_value = fake_resp

        es.prune_indices(max_log_records=1)

        the_args, _ = fake_logger.error.call_args
        logged_error_msg = the_args[0]
        expected_msg = "Failed to delete index logs-2016.01.01, Status: 418, Msg: I'm a tea pot :D"

        self.assertEqual(logged_error_msg, expected_msg)


@patch('cronner.elasticsearch._get_indices')
@patch('cronner.elasticsearch._call_es')
class TestAddFieldData(unittest.TestCase):
    """A suite of test cases for the ``add_field_data`` function"""
    def test_add_field_data(self, fake_call_es, fake_get_indices):
        """``add_field_data`` updates all indices"""
        fake_get_indices.return_value = {'logs-2016.05.12', 'logs-2016.01.01'}

        es.add_field_data()
        indexes_updated = fake_call_es.call_count
        expected = 2

        self.assertTrue(indexes_updated, expected)

    @patch.object(es, 'logger')
    def test_add_field_data_error(self, fake_logger, fake_call_es, fake_get_indices):
        """``add_field_data`` logs failures to update an index"""
        fake_get_indices.return_value = {'logs-2016.05.12', 'logs-2016.01.01'}
        fake_resp1 = MagicMock()
        fake_resp2 = MagicMock()
        fake_resp2.ok = False
        fake_resp2.status_code = 418
        fake_resp2.content = b"I'm a teapot"
        fake_call_es.side_effect = [fake_resp1, fake_resp2]

        es.add_field_data()
        errors_logged = fake_logger.error.call_count
        expected = 1

        self.assertTrue(errors_logged, expected)


@patch('cronner.elasticsearch._call_es')
class TestGetIndices(unittest.TestCase):
    """A suite of test cases for the ``_get_indices`` function"""

    def test_get_indices(self, fake_call_es):
        """``_get_indices`` Returns the expected set of data"""
        resp = MagicMock()
        resp.content = b'yellow open logs-2019.04.28 Ei_u-uXRTvyT3QJOYfWEpg 5 1 119376 0 53.6mb 53.6mb\nyellow open logs-2019.05.04 Jytkex9wQkiscV3MzwVUVw 5 1 222043 0 102.4mb 102.4mb\nyellow open logs-2019.05.16 _v_r9Ui4SKCpUn0riLpj5g 5 1 315299 0 146.2mb 146.2mb'
        fake_call_es.return_value = resp

        indices = es._get_indices()
        expected = set(['logs-2019.05.16', 'logs-2019.04.28', 'logs-2019.05.04'])

        self.assertEqual(indices, expected)


class TestCallEs(unittest.TestCase):
    """A suite of test cases for the ``_call_es`` function"""
    @patch.object(es, 'requests')
    def test_call_es(self, fake_requests):
        """``_call_es`` Returns a Response object"""
        fake_resp = MagicMock()
        fake_requests.get.return_value = fake_resp

        resp = es._call_es('https://some.es.server:9200')

        self.assertTrue(resp is fake_resp)


if __name__ == '__main__':
    unittest.main()
