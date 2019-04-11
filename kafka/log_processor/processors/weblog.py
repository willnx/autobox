# -*- coding; UTF-8 -*-
"""Defines how to process and upload API log file information for vLab analytics"""
import time
import ipaddress
from os import environ

import ujson

from log_processor.worker import LogWorker, InvalidToken
from log_processor.manager import process_logs


class WebLogWorker(LogWorker):
    """Handles processing the web logs and then uploading data to ElasticSearch"""
    @staticmethod
    def format_timestamp(timestamp):
        """Covernt an Apache-style timestamp to one ElasticSearch likes"""
        # [09/Apr/2019:16:34:39
        pattern = '[%m/%b/%Y:%H:%M:%S'
        time_struct = time.strptime(timestamp, pattern)
        # Formatting the timestmap like this, instead of an EPOCH, means that
        # ElasticSearch will infer the correct type as "date"
        # which Grafana depends on
        formatted = time.strftime('%Y/%m/%d %H:%M:%S', time_struct)
        return formatted

    def format_info(self, info):
        """Extract the handy bits of data into a JSON document"""
        # The logs should adhere to standard Apache web log format
        # 10.200.217.90 - unset [08/Apr/2019:22:21:57 -0000] "GET /api/1/inf/onefs/task/2b311e03-455c-4409-b8c7-425961533a44? HTTP/1.1" 200 248 "None" "vLab CLI 2019.03.28 rid=85c1c19d38e0485da38d4d0a9da2f43f"
        # The point of the "source" tag is to handle
        document = {'source' : info['name'],
                    'timestamp' : None,
                    'user' : None,
                    'client_ip' : None,
                    'method' : None,
                    'url' : None,
                    'status_code' : None,
                    'user_agent' : None,
                    'transaction_id': None,
                    'log' : info['log']}
        raw = info['log'].split()
        by_quotes = info['log'].split('"')
        try:
             ipaddress.ip_address(raw[0])
        except ValueError:
            # must be a traceback, or some other log
            pass
        else:
            document['timestamp'] = self.format_timestamp(raw[3])
            document['user'] = raw[2]
            document['client_ip'] = raw[0]
            document['method'] = raw[5].replace('"', '')
            document['url'] = raw[6]
            document['status_code'] = raw[8]
            # the vLab CLI overloads the User Agent with a transaction id
            document['user_agent'] = by_quotes[5].split('=')[0].replace('rid', '')
            try:
                document['transaction_id'] = by_quotes[5].split('=')[1]
            except IndexError:
                pass
        return ujson.dumps(document)

if __name__ == '__main__':
    process_logs(worker_cls=WebLogWorker,
                 topic='web',
                 server=environ['KAFKA_SERVER'],
                 work_group='web',
                 name='WebLogProcessor')
