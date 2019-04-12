# -*- coding: UTF-8 -*-
"""A suite of unit tests for the ``processors.weblog`` module"""
import unittest
from unittest.mock import patch, MagicMock
import os
import builtins

from log_processor import worker
from log_processor.processors import weblog


class TestWebLogWorker(unittest.TestCase):
    """A suite of test cases for the ``WebLogWorker`` object"""
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
        """``WebLogWorker`` accepts standard ``Worker`` init params"""
        work_group = 'web'
        work_queue = MagicMock()
        idle_queue = MagicMock()

        web_worker = weblog.WebLogWorker(work_group, work_queue, idle_queue)

        self.assertTrue(isinstance(web_worker, worker.Worker))

    @patch.object(worker, 'ElasticSearch')
    @patch.object(worker, 'Fernet')
    @patch.object(builtins, "open")
    def test_format_info(self, fake_open, fake_Fernet, fake_ElasticSearch):
        """``WebLogWorker`` the 'format_info' method returns JSON"""
        work_group = 'web'
        work_queue = MagicMock()
        idle_queue = MagicMock()

        web_worker = weblog.WebLogWorker(work_group, work_queue, idle_queue)
        example_info = {'name' : 'some container',
                        'log' : '10.200.217.90 - unset [08/Apr/2019:22:21:57 -0000] "GET /api/1/inf/onefs/task/2b311e03-455c-4409-b8c7-425961533a44? HTTP/1.1" 200 248 "None" "vLab CLI 2019.03.28 rid=85c1c19d38e0485da38d4d0a9da2f43f"'
                       }
        answer = web_worker.format_info(example_info)
        expected = '{"source":"some container","timestamp":"2019\\/04\\/08 22:21:57","user":"unset","client_ip":"10.200.217.90","method":"GET","url":"\\/api\\/1\\/inf\\/onefs\\/task\\/2b311e03-455c-4409-b8c7-425961533a44?","status_code":"200","user_agent":"vLab CLI 2019.03.28 ","transaction_id":"85c1c19d38e0485da38d4d0a9da2f43f","log":"10.200.217.90 - unset [08\\/Apr\\/2019:22:21:57 -0000] \\"GET \\/api\\/1\\/inf\\/onefs\\/task\\/2b311e03-455c-4409-b8c7-425961533a44? HTTP\\/1.1\\" 200 248 \\"None\\" \\"vLab CLI 2019.03.28 rid=85c1c19d38e0485da38d4d0a9da2f43f\\""}'

        self.assertEqual(answer, expected)

    @patch.object(worker, 'ElasticSearch')
    @patch.object(worker, 'Fernet')
    @patch.object(builtins, "open")
    def test_format_info_traceback(self, fake_open, fake_Fernet, fake_ElasticSearch):
        """``WebLogWorker`` the 'format_info' handles non-Apache style logs too"""
        work_group = 'web'
        work_queue = MagicMock()
        idle_queue = MagicMock()

        web_worker = weblog.WebLogWorker(work_group, work_queue, idle_queue)
        example_info = {'name' : 'some container',
                        'log' : 'Traceback (most recent call last):\n  File "some file", line 42 in test\n'
                       }
        answer = web_worker.format_info(example_info)
        expected = '{"source":"some container","timestamp":null,"user":null,"client_ip":null,"method":null,"url":null,"status_code":null,"user_agent":null,"transaction_id":null,"log":"Traceback (most recent call last):\\n  File \\"some file\\", line 42 in test\\n"}'

        self.assertEqual(answer, expected)

    @patch.object(worker, 'ElasticSearch')
    @patch.object(worker, 'Fernet')
    @patch.object(builtins, "open")
    def test_format_info_other_client(self, fake_open, fake_Fernet, fake_ElasticSearch):
        """``WebLogWorker`` the 'format_info' method handles user agents not overloaded with a transaction id"""
        work_group = 'web'
        work_queue = MagicMock()
        idle_queue = MagicMock()

        web_worker = weblog.WebLogWorker(work_group, work_queue, idle_queue)
        example_info = {'name' : 'some container',
                        'log' : '10.200.217.90 - unset [08/Apr/2019:22:21:57 -0000] "GET /api/1/inf/onefs/task/2b311e03-455c-4409-b8c7-425961533a44? HTTP/1.1" 200 248 "None" "python/requests"'
                       }
        answer = web_worker.format_info(example_info)
        expected = '{"source":"some container","timestamp":"2019\\/04\\/08 22:21:57","user":"unset","client_ip":"10.200.217.90","method":"GET","url":"\\/api\\/1\\/inf\\/onefs\\/task\\/2b311e03-455c-4409-b8c7-425961533a44?","status_code":"200","user_agent":"python\\/requests","transaction_id":null,"log":"10.200.217.90 - unset [08\\/Apr\\/2019:22:21:57 -0000] \\"GET \\/api\\/1\\/inf\\/onefs\\/task\\/2b311e03-455c-4409-b8c7-425961533a44? HTTP\\/1.1\\" 200 248 \\"None\\" \\"python\\/requests\\""}'

        self.assertEqual(answer, expected)

    @patch.object(worker, 'ElasticSearch')
    @patch.object(worker, 'Fernet')
    @patch.object(builtins, "open")
    def test_process_data(self, fake_open, fake_Fernet, fake_ElasticSearch):
        """``WebLogworker`` 'process_data' formats logs, then uploads to ElasticSearch"""
        work_group = 'web'
        work_queue = MagicMock()
        idle_queue = MagicMock()
        fake_es = MagicMock()
        fake_ElasticSearch.return_value = fake_es
        fake_cipher = MagicMock()
        fake_cipher.decrypt.return_value =  '{"name":"some container","log":"10.200.217.90 - unset [08\\/Apr\\/2019:22:21:57 -0000] \\"GET \\/api\\/1\\/inf\\/onefs\\/task\\/2b311e03-455c-4409-b8c7-425961533a44? HTTP\\/1.1\\" 200 248 \\"None\\" \\"vLab CLI 2019.03.28 rid=85c1c19d38e0485da38d4d0a9da2f43f\\""}'
        fake_Fernet.return_value = fake_cipher

        web_worker = weblog.WebLogWorker(work_group, work_queue, idle_queue)

        web_worker.process_data('some encrypted data')

        self.assertTrue(fake_es.write.called)

    @patch.object(worker, 'ElasticSearch')
    @patch.object(worker, 'Fernet')
    @patch.object(builtins, "open")
    def test_process_data_error(self, fake_open, fake_Fernet, fake_ElasticSearch):
        """``WebLogworker`` 'process_data' logs if it cannot decrypt/de-serialize the data"""
        work_group = 'web'
        work_queue = MagicMock()
        idle_queue = MagicMock()
        fake_es = MagicMock()
        fake_ElasticSearch.return_value = fake_es
        fake_cipher = MagicMock()
        fake_cipher.decrypt.return_value =  '{Invalid JSON'
        fake_Fernet.return_value = fake_cipher
        fake_log = MagicMock()

        web_worker = weblog.WebLogWorker(work_group, work_queue, idle_queue)
        web_worker.log = fake_log

        web_worker.process_data('some encrypted data')
        the_args, _ = fake_log.error.call_args
        error_msg = the_args[0]
        expected = 'Error: Expected object or value, Data: some encrypted data'

        self.assertEqual(error_msg, expected)


if __name__ == '__main__':
    unittest.main()
