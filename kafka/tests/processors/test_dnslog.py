# -*- coding: UTF-8 -*-
"""A suite of unit tests for the ``dnslog`` module"""
import unittest
from unittest.mock import patch, MagicMock
import builtins
import os

from log_processor import worker
from log_processor.processors import dnslog


class TestDnsLog(unittest.TestCase):
    """A suite of test cases for the ``DnsLogWorker`` object"""
    @classmethod
    def setUp(cls):
        """Runs before every test case"""
        os.environ['CIPHER_KEY_FILE'] = './.fake_file.txt'
        os.environ['ELASTICSEARCH_SERVER'] = '127.0.0.1'
        os.environ['ELASTICSEARCH_USER'] = 'bob'
        os.environ['ELASTICSEARCH_DOC_TYPE'] = 'someLogType'
        os.environ['ELASTICSEARCH_PASSWD_FILE'] = './.fake_file.txt'

    @classmethod
    def tearDown(cls):
        """Runs after every test case"""
        os.environ.pop('CIPHER_KEY_FILE', None)
        os.environ.pop('ELASTICSEARCH_SERVER', None)
        os.environ.pop('ELASTICSEARCH_USER', None)
        os.environ.pop('ELASTICSEARCH_DOC_TYPE', None)
        os.environ.pop('ELASTICSEARCH_PASSWD_FILE', None)

    @patch.object(worker, 'ElasticSearch')
    @patch.object(worker, 'Fernet')
    @patch.object(builtins, "open")
    def test_init(self, fake_open, fake_Fernet, fake_ElasticSearch):
        """``DnsLogWorker`` accepts standard ``Worker`` init params"""
        work_group = 'web'
        work_queue = MagicMock()
        idle_queue = MagicMock()

        dns_worker = dnslog.DnsLogWorker(work_group, work_queue, idle_queue)

        self.assertTrue(isinstance(dns_worker, worker.Worker))

    @patch.object(worker, 'ElasticSearch')
    @patch.object(worker, 'Fernet')
    @patch.object(builtins, "open")
    def test_format_info(self, fake_open, fake_Fernet, fake_ElasticSearch):
        """``DnsLogWorker`` 'format_info' returns a JSON document"""
        work_group = 'web'
        work_queue = MagicMock()
        idle_queue = MagicMock()
        info = {'name' : 'system_dns_1',
                'log' : '03-Apr-2019 20:31:41.392 client @0x7f67b02031b0 172.21.0.16#47486 (willhn.vlab.emc.com): query: willhn.vlab.emc.com IN AAAA + (10.241.80.49)'}

        dns_worker = dnslog.DnsLogWorker(work_group, work_queue, idle_queue)
        json_doc = dns_worker.format_info(info)
        expected = '{"service":"system_dns_1","log":"03-Apr-2019 20:31:41.392 client @0x7f67b02031b0 172.21.0.16#47486 (willhn.vlab.emc.com): query: willhn.vlab.emc.com IN AAAA + (10.241.80.49)","timestamp":"2019\\/04\\/03 20:31:41","query":true,"update":false,"client_ip":"172.21.0.16"}'

        self.assertEqual(json_doc, expected)

    def test_get_timestamp(self):
        """``DnsLogWorker`` 'get_time_stamp' returns an ElasticSearch friendly timestamp"""
        log_message = '03-Apr-2019 21:50:42.811 resolver priming query complete'

        timestamp = dnslog.DnsLogWorker.get_timestamp(log_message)
        expected = '2019/04/03 21:50:42'

        self.assertEqual(timestamp, expected)

    def test_is_update(self):
        """``DnsLogWorker`` 'is_update' correctly identifies a log message when it's a DDNS update"""
        log_message = "03-Apr-2019 21:55:46.589 client @0x7f67a40db150 10.241.80.79#38373/key ddns_update: updating zone 'vlab.emc.com/IN': adding an RR at 'batinj.vlab.emc.com' A 10.241.80.79"

        is_update = dnslog.DnsLogWorker.is_update(log_message)

        self.assertTrue(is_update)

    def test_is_not_update(self):
        """``DnsLogWorker`` 'is_update' correctly identifies a log message when it is not a DDNS update"""
        log_message = "03-Apr-2019 20:35:20.080 client @0x7f67882c0cd0 172.21.0.16#58693 (willhn.vlab.emc.com): query: willhn.vlab.emc.com IN A + (10.241.80.49)"

        is_update = dnslog.DnsLogWorker.is_update(log_message)

        self.assertFalse(is_update)

    def test_is_query(self):
        """``DnsLogWorker`` 'is_query' correctly identifies a log message when it's a DNS lookup"""
        log_message = "03-Apr-2019 20:35:20.080 client @0x7f67882c0cd0 172.21.0.16#58693 (willhn.vlab.emc.com): query: willhn.vlab.emc.com IN A + (10.241.80.49)"

        is_query = dnslog.DnsLogWorker.is_query(log_message)

        self.assertTrue(is_query)

    def test_is_not_query(self):
        """``DnsLogWorker`` 'is_query' correctly identifies a log message when it is not a DNS lookup"""
        log_message = "03-Apr-2019 21:55:46.589 client @0x7f67a40db150 10.241.80.79#38373/key ddns_update: updating zone 'vlab.emc.com/IN': adding an RR at 'batinj.vlab.emc.com' A 10.241.80.79"

        is_query = dnslog.DnsLogWorker.is_query(log_message)

        self.assertFalse(is_query)

    def test_get_client_ip(self):
        """``DnsLogWorker`` 'get_client_ip' returns the client IP when logged"""
        log_message = "03-Apr-2019 21:52:47.703 client @0x7f67b02686a0 10.241.80.69#45154 (vs.login.msa.akadns6.net): query: vs.login.msa.akadns6.net IN A +E(0)DCV (10.241.80.49)"

        ip = dnslog.DnsLogWorker.get_client_ip(log_message)
        expected = '10.241.80.69'

        self.assertEqual(ip, expected)

    def test_get_client_ip_none(self):
        """``DnsLogWorker`` 'get_client_ip' returns an empty string when no client IP is logged"""
        log_message = "03-Apr-2019 21:50:42.811 resolver priming query complete"

        ip = dnslog.DnsLogWorker.get_client_ip(log_message)
        expected = ''

        self.assertEqual(ip, expected)


if __name__ == '__main__':
    unittest.main()
