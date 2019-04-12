# -*- coding: UTF-8 -*-
"""Processes the DNS logs and upload them to ElasticSearch"""
import time
from os import environ

import ujson

from log_processor.worker import LogWorker, InvalidToken
from log_processor.manager import process_logs


class DnsLogWorker(LogWorker):
    """Convert the raw DNS log into a JSON document then upload it to ElasticSearch"""
    @staticmethod
    def get_timestamp(log_message):
        """Extract the timestamp and format it for ElasticSearch

        :Returns: String

        :param log_message: The entire log message from the DNS service
        :type log_message: String
        """
        the_date = log_message.split(' ')[0]
        time_with_milisec = log_message.split(' ')[1]
        the_time = time_with_milisec.split('.')[0]
        timestamp = '{} {}'.format(the_date, the_time)
        pattern = '%d-%b-%Y %H:%M:%S'
        time_struct = time.strptime(timestamp, pattern)
        es_timestamp = time.strftime('%Y/%m/%d %H:%M:%S', time_struct)
        return es_timestamp

    @staticmethod
    def is_update(log_message):
        """Determine if the log message is related to a Dynamic DNS update

        :Returns: String

        :param log_message: The entire log message from the DNS server
        :type log_message: String
        """
        if 'ddns_update:' in log_message:
            return True
        else:
            return False

    @staticmethod
    def is_query(log_message):
        """Determine if the log message is a DNS query (i.e. a lookup of a A,AAAA,NS,SRV reocrd)

        :Returns: String

        :param log_message: The entire log message from the DNS server
        :type log_message: String
        """
        if "query:" in log_message:
            return True
        else:
            return False

    @staticmethod
    def get_client_ip(log_message):
        """Extract the IP of the client talking to the DNS server

        :Returns: String

        :param log_message: The entire log message from the DNS server
        :type log_message: String
        """
        chunks = log_message.split(' ')
        if chunks[2] != 'client':
            ip =  ''
        else:
            ip_and_port = chunks[4]
            ip = ip_and_port.split('#')[0]
        return ip

    def format_info(self, info):
        """Extract the handy bits of data into a JSON document"""
        document = {
            'service' : info['name'],
            'log' : info['log'],
            'timestamp' : self.get_timestamp(info['log']),
            'query' : self.is_query(info['log']),
            'update' : self.is_update(info['log']),
            'client_ip' : self.get_client_ip(info['log']),
        }
        return ujson.dumps(document)


if __name__ == '__main__':
    process_logs(worker_cls=DnsLogWorker,
                 topic='dns',
                 server=environ['KAFKA_SERVER'],
                 work_group='worker',
                 name='DnsLogProcessor')
