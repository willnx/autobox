# -*- coding: UTF_8 -*-
import time
import datetime
import threading
from random import randint
from abc import abstractmethod

import ujson
from vlab_inf_common.vmware import vim

from stat_collector.lib.std_logger import get_logger


class UserCollector(threading.Thread):
    """Obtain usage information for a given user"""
    def __init__(self, vcenter, username, users_dir, influx, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.vcenter = vcenter
        self.username = username
        self.users_dir = users_dir
        self.interval = 300
        self.keep_running = True
        self.log = get_logger(self.name)
        self.influx = influx

    def run(self):
        """Defines how the thread collects data"""
        time.sleep(randint(0, 30)) # so all threads don't pound vCenter all at once
        while self.keep_running:
            loop_start = time.time()
            user_usage = self.get_usage()
            try:
                self.influx.write(fields=user_usage['fields'], tags=user_usage['tags'])
            except Exception as doh:
                self.keep_running = False
                self.log.error('Unexpected exception')
                self.log.exception(doh)
            loop_time = loop_start - time.time()
            # Avoids sub-second, negative, and values greater than the loop interval
            sleep_for = min(self.interval, int(abs(loop_time - self.interval)))
            time.sleep(sleep_for)

    @property
    def folder(self):
        parent_dir = self.vcenter.get_by_name(vim.Folder, self.users_dir)
        for folder in parent_dir.childEntity:
            if folder.name == self.username:
                return folder
        else:
            raise RuntimeError('Unable to find a folder for {} under {}'.format(self.username, self.users_dir))

    def get_usage(self):
        answer = {'fields': {'total_vms' : 0, 'powered_on': 0, "username" : '"{}"'.format(self.username)},
                  'tags' : {'user' : self.username}}
        for entity in self.folder.childEntity:
            if isinstance(entity, vim.VirtualMachine):
                answer['fields']['total_vms'] += 1

                if entity.runtime.powerState.lower().endswith('on'):
                    answer['fields']['powered_on'] += 1
                try:
                    meta_data = ujson.loads(entity.config.annotation)
                except ValueError:
                    # meta data isn't written until after the VM has finished being created
                    component = 'deploying'
                else:
                    component = meta_data['component']
                answer['fields'].setdefault(component, 0)
                answer['fields'][component] += 1
        return answer


class PerfCollector:
    """Obtain performance metrics for objects within vSphere

    :param vcenter: A connection to a vCenter server
    :type vcenter: vlab_inf_common.vmware.vCenter

    :param entity: The object within vSphere to pull metrics about
    :type entity_name: pyVmomi.VmomiSupport.LazyType

    :param counter_name: The name of the stat to pull from vCenter
    """
    def __init__(self, vcenter, entity, counter_name):
        self.vcenter = vcenter
        self.last_collected = datetime.datetime.now()
        self.entity = entity
        self.counter_name = counter_name
        self._metric_id = None

    def __repr__(self):
        return 'PerfCollector(name={}, stat={}, last={})'.format(self.entity.name, self.counter_name, self.last_collected.strftime('%Y/%m/%d %H:%M:%S'))

    @property
    def _counters(self):
        return self.vcenter.content.perfManager.perfCounter

    @property
    def perf_manager(self):
        return self.vcenter.content.perfManager

    @property
    def counters(self):
        """vCenter forces the client to create a mapping of perf counter indexes to human-friendly names"""
        answer = {}
        for counter in self._counters:
            full_name = "{}.{}.{}".format(counter.groupInfo.key,
                                          counter.nameInfo.key,
                                          counter.rollupType)
            answer[full_name] = counter.key
        return answer

    @property
    def metric_id(self):
        if self._metric_id is None:
            answer = vim.PerformanceManager.MetricId(counterId=self.counters[self.counter_name], instance="")
            # pyVmomi wants this object as an iterable, even though it doesn't return it as one...
            self._metric_id = [answer]
        return self._metric_id

    def query(self):
        """Generate the query spec, and collect some data.
        The returned object is a mapping of EPOCH timestamp to stat value.

        :Returns: Dictionary
        """
        query_spec = vim.PerformanceManager.QuerySpec(entity=self.entity,
                                                     metricId=self.metric_id,
                                                     startTime=self.last_collected,
                                                     endTime=datetime.datetime.now(),
                                                     maxSample=100)
        data = self.perf_manager.QueryPerf(querySpec=[query_spec])
        stats = {}
        if data:
            data = data[0]
            # the god damn time stamp and value are in two different arrays on the
            # same object, and **you** have to coordinate the index...
            for shit_index in range(len(data.sampleInfo)):
                try:
                    # no idea why they make the "value" object an array of objects
                    # each with an attribute of "value" that is an array of the literal
                    # values. Don't blame me for the magic number, blame the shitty
                    # dev at VMware that came up with this shitstorm data structure...
                    value = data.value[0].value[shit_index]
                except IndexError:
                    # If no data points are returned, this shit data structure returns
                    # an iterable object...
                    pass
                else:
                    timestamp = data.sampleInfo[shit_index].timestamp.strftime('%s')
                    timestamp = int(timestamp) - time.timezone # Now it's EPOCH
                    stats[timestamp] = value
                    self.last_collected = datetime.datetime.now()
        return stats


class CollectorThread(threading.Thread):
    """A worker thread for collecting stats

    When sub-classing this object you must define:

    -   self._stats   : The stat key names to collect
    -   self._kind    : The human name for the thing you're collecting stats about.
                        For example, ``VM`` is for virtual machines, ``ESXi`` would be
                        for hosts in vCenter.
    - ``find_entity`` : How to find the specific object in vCenter that you want to
                        collect stats from. Must set ``self._entity`` as well as
                        return the object.
    """
    def __init__(self, vcenter, influxdb, name):
        super().__init__()
        self.vcenter = vcenter
        self.influxdb = influxdb
        self.entity_name = name
        self.keep_running = True
        self._entity = None
        self._kind = None
        self._loop_interval = 300
        self._stats = [] # subclasses set this value
        self.collectors = []

    def setup_collectors(self):
        self.collectors = [PerfCollector(self.vcenter, self.entity(), x) for x in self._stats]

    def entity(self):
        """The object reference to the thing you're collecting stats about"""
        if self._entity is None:
            self._entity = self.find_entity()
        return self._entity

    @abstractmethod
    def find_entity(self):
        """How to find the object reference to the thing you're collecting stats about"""
        pass

    def collect_stats(self):
        """Collect and upload stats"""
        if self._stats and not self.collectors:
            self.setup_collectors()
        for collector in self.collectors:
            data = collector.query()
            for timestamp, value in data.items():
                fields = {collector.counter_name : value}
                tags = {'name' : self.entity_name, 'kind': "{}".format(self._kind)}
                self.influxdb.write(fields=fields, tags=tags, timestamp=timestamp)

    def run(self):
        """Defines how the thread collects, processes, and uploads stats"""
        while self.keep_running:
            start = time.time()
            self.collect_stats()
            delta = time.time() - start
            if delta > self._loop_interval:
                sleep_for = 0
            else:
                sleep_for = min(abs(self._loop_interval - delta), self._loop_interval)
            time.sleep(sleep_for)


class VMCollector(CollectorThread):
    """Collect stats specific a Virtual Machine from vCenter"""
    def __init__(self, vcenter, influxdb, name, parent_dir):
        super().__init__(vcenter, influxdb, name)
        self.parent_dir = parent_dir
        self._kind = 'VM'
        self._stats = ['cpu.usage.average',
                       'net.bytesRx.average',
                       'net.bytesTx.average',
                       'mem.active.average',
                       'disk.usage.average',
                       'disk.read.average',
                       'disk.write.average',]
        self.setup_collectors()

    def find_entity(self):
        folder = self.vcenter.get_by_name(vim.Folder, self.parent_dir)
        for entity in folder.childEntity:
            if entity.name == self.entity_name:
                self._entity = entity
                return entity
        else:
            raise RuntimeError('Unable to find {} named {} in folder {}'.format(self._kind, self.entity_name, self.parent_dir))


class ESXiCollector(CollectorThread):
    """Collect stats specific to an ESXi host from vCenter"""
    def __init__(self, vcenter, influxdb, name):
        super().__init__(vcenter, influxdb, name)
        self._kind = 'ESXi'
        self._stats = ['cpu.usage.average',
                       'cpu.capacity.provisioned.average',
                       'net.bytesRx.average',
                       'net.bytesTx.average',
                       'mem.active.average',
                       'power.power.average',
                       ]
        self.setup_collectors()

    def find_entity(self):
        try:
            entity = self.vcenter.host_systems[self.entity_name]
        except Exception:
            raise RuntimeError('No such {} by name {}'.format(self._kind, self.entity_name))
        else:
            self._entity = entity
            return entity
