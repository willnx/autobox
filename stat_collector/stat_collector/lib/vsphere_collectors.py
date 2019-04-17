# -*- coding: UTF_8 -*-
import time
import datetime
import threading
from random import randint

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

    :param entity_name: The name of the object within vSphere to pull metrics about
    :type entity_name: String

    :param entity_type: The category/type of the object; ``vim.SomeType``
    :type entity_type: pyVmomi.VmomiSupport.LazyType
    """
    def __init__(self, vcenter, entity_name, entity_type, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.vcenter = vcenter
        self.entity_name = entity_name
        self.entity_type = entity_type
        self.last_collected = datetime.datetime.now()

    @property
    def entity(self):
        """The object ref to the thing in vCenter"""
        return self.vcenter.get_by_name(self.entity_type, self.entity_name)

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

    def metric_id(self, counter_name):
        answer = vim.PerformanceManager.MetricId(counterId=self.counters[counter_name], instance="")
        # pyVmomi wants this object as an iterable, even though it doesn't return it as one...
        return [answer]

    def query(self, metric_name):
        """TODO"""
        query_spec = vim.PerformanceManager.QuerySpec(entity=self.entity,
                                                     metricId=self.metric_id(metric_name),
                                                     startTime=self.last_collected)
        shit = self.perf_manager.QueryPerf(querySpec=[query_spec])
        stats = {}
        for shit_datastructure in shit:
            try:
                value = shit_datastructure.value[0].value[0]
            except IndexError:
                # If no data points are returned, this shit data structure returns
                # an iterable object...
                pass
            else:
                timestamp = shit_datastructure.sampleInfo[0].timestamp.strftime('%s') # Now it's EPOCH
                stats[timestamp] = value
        return stats
