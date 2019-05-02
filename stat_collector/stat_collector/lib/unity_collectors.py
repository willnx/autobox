# -*- coding: UTF-8 -*-
"""Defines a group of objects for collecting stats/metrics from an EMC Unity SAN"""
import time
import random
import datetime
import threading
from abc import abstractmethod

TIMESTAMP_PATTERN = '%Y-%m-%dT%H:%M:%S'


class UnityStat:
    _STAT_NAME = None

    def __init__(self, unity):
        self.unity = unity
        self._stat_id = "Not initialized"
        self._stat_init_endpoint = '/api/types/metricRealTimeQuery/instances'
        self._stat_query_endpoint = '/api/types/metricQueryResult/instances'
        self._stat_param = {}
        self._stat_init_done = False

    def __repr__(self):
        return 'UnityStat(Server={}, stat={}, id={})'.format(self.unity.ip_addr,
                                                             self._STAT_NAME,
                                                             self._stat_id)

    def _init_stat(self):
        """The Unity API requires you to say 'hey, I want to collect this stat'"""
        body = {'paths': [self._STAT_NAME], 'interval':60}
        resp = self.unity.post(self._stat_init_endpoint, json=body)
        data = resp.json()
        self._stat_id = data['content']['id']
        self._stat_param = {"filter" : "queryId EQ {}".format(self._stat_id)}
        self._stat_init_done = True

    @abstractmethod
    def process(self, stat):
        pass

    def query(self):
        if not self._stat_init_done:
            self._init_stat()
        resp = self.unity.get(self._stat_query_endpoint, params=self._stat_param)
        entries = resp.json()['entries']
        stats = {}
        for data in entries:
            # Timestamp includes milisconds, ex: 2019-04-29T15:26:00.000Z
            timestamp = data['content']['timestamp'].split('.')[0]
            epoch = int(time.mktime(time.strptime(timestamp, TIMESTAMP_PATTERN))) - time.altzone
            values = data['content']['values']
            stats[epoch] = values
        return stats


class UnityLunLatency(UnityStat):
    _STAT_NAME = 'sp.*.storage.lun.*.totalIoTime'

    def process(self, stat):
        fields = {}
        tags = {'kind' : 'unity'}
        for san_head in stat.keys():
            for lun, latency in stat[san_head].items():
                fields['latency'] = latency
                tags['name'] = '{}_{}'.format(san_head, lun)
                yield fields, tags


class UnityLunIO(UnityStat):
    _STAT_NAME = 'sp.*.storage.lun.*.currentIOCount'

    def process(self, stat):
        tags = {'kind' : "unity"}
        fields = {}
        for san_head, lun_data in stat.items():
            for lun, iops in lun_data.items():
                name = '{}_{}'.format(san_head, lun)
                tags['name'] = name
                fields['iops'] = iops
                yield fields, tags


class UnityNetBytesIn(UnityStat):
    _STAT_NAME = 'sp.*.net.device.*.bytesIn'

    def process(self, stat):
        tags = {'kind': "unity"}
        fields = {}
        for nics in stat.values():
            for nic, bytes_in in nics.items():
                if bytes_in:
                    tags['name'] = nic
                    fields['bytes_in'] = bytes_in
                    yield fields, tags
                else:
                    continue


class UnityNetBytesOut(UnityStat):
    _STAT_NAME = 'sp.*.net.device.*.bytesOut'

    def process(self, stat):
        tags = {'kind': "unity"}
        fields = {}
        for nics in stat.values():
            for nic, bytes_out in nics.items():
                if bytes_out:
                    tags['name'] = nic
                    fields['bytes_out'] = bytes_out
                    yield fields, tags
                else:
                    continue


class UnityMemoryUsedBytes(UnityStat):
    _STAT_NAME = 'sp.*.memory.summary.totalUsedBytes'

    def process(self, stat):
        tags = {'kind' : 'unity'}
        fields = {}
        for san_head, ram in stat.items():
            tags['name'] = san_head
            fields['ram_active'] = ram
            yield fields, tags


class UnityCollector(threading.Thread):
    """A thread for collecting stats from the Unity SAN"""
    def __init__(self, influx, unity, stat):
        super().__init__()
        self.stat = stat
        self.keep_running = True
        self.influx = influx
        self.loop_interval = 60

    def run(self):
        time.sleep(random.randint(0, 15))
        while self.keep_running:
            start_time = time.time()
            stats = self.stat.query()
            for timestamp, data in stats.items():
                for fields, tags in self.stat.process(data):
                    self.influx.write(fields=fields, tags=tags, timestamp=timestamp)
            delta = time.time() - start_time
            sleep_for = min(abs(self.loop_interval - delta), self.loop_interval)
            time.sleep(sleep_for)
