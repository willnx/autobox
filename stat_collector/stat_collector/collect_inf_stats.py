# -*- coding: UTF-8 -*-
"""Defines how to collect stats from VMWare"""
import os
import time

from vlab_inf_common.vmware import vCenter, vim

from stat_collector.lib.unity import Unity
from stat_collector.lib.influxdb import InfluxDB
from stat_collector.lib.std_logger import get_logger
from stat_collector.lib.vsphere_collectors import VMCollector, ESXiCollector
from stat_collector.lib.unity_collectors import UnityCollector, UnityLunLatency, UnityLunIO, UnityNetBytesIn, UnityNetBytesOut, UnityMemoryUsedBytes


CHECK_INTERVAL = 600
VM_PARENT_DIR = 'system'


def important_vms():
    """The names of VMs to collect stats from

    :Returns: Set
    """
    return set(['vlabServer', 'vCenter', 'vLabData', 'vLabAutoBox'])


def unity_stats():
    """The name of the specific stats to collect from the Unity SAN

    :Returns: Set
    """
    return set(['lun_latency', 'lun_io', 'net_bytes_in', 'net_bytes_out', 'ram_active'])


def unity_stat_map(name):
    """Associated the specific Unity stat object, with it's human-reference name

    :Returns: stat_collector.lib.unity_collectors.UnityStat

    :param name: The name of a specific stat to collect
    :type name: String
    """
    map = {
        'lun_latency' : UnityLunLatency,
        'lun_io' : UnityLunIO,
        'net_bytes_in' : UnityNetBytesIn,
        'net_bytes_out' : UnityNetBytesOut,
        'ram_active' : UnityMemoryUsedBytes,
    }
    return map[name]


def spawn_collector(vcenter, influx, unity, name, collectors, kind='vms'):
    """Create and start a thread for collecting perf stats from VMware

    :Returns: Dictionary

    :param vcenter: An established connection to a vCenter server
    :type vcenter: vlab_inf_common.vmware.vCenter

    :param influx: An established connection to an InfluxDB server
    :type influx: stat_collector.lib.influxdb.InfluxDB

    :param unity: A connection to the EMC Unity API
    :type unity: stat_collector.lib.unity.Unity

    :param name: The human reference to the stat
    :type name: String

    :param collectors: The threads that have been created to collect stats from VMware
    :type collectors: Dictionary

    :name kind: The category of stat being collected
    :type kind: String
    """
    if kind == 'vms':
        collector = VMCollector(vcenter, influx, name, VM_PARENT_DIR)
    elif kind == 'esxi_hosts':
        collector = ESXiCollector(vcenter, influx, name)
    else:
        stat_obj = unity_stat_map(name)
        stat = stat_obj(unity)
        collector = UnityCollector(influx, unity, stat)
    collector.start()
    collectors[kind][name] = collector
    return collectors


def create_collectors(vcenter, influx, unity, collectors, log):
    """Create all threads for collecting perf stats from VMware

    :Returns: Dictionary

    :param vcenter: An established connection to a vCenter server
    :type vcenter: vlab_inf_common.vmware.vCenter

    :param influx: An established connection to an InfluxDB server
    :type influx: stat_collector.lib.influxdb.InfluxDB

    :param unity: A connection to the EMC Unity API
    :type unity: stat_collector.lib.unity.Unity

    :param collectors: The threads that have been created to collect stats from VMware
    :type collectors: Dictionary

    :param log: An object for logging program messages for humans
    :type log: logging.LoggerAdapter
    """
    esxi_hosts = set(vcenter.host_systems.keys())
    for esxi in esxi_hosts:
        if not collectors['esxi_hosts'].get(esxi, None):
            log.info("Creating a collector for: {}".format(esxi))
            spawn_collector(vcenter, influx, unity, esxi, collectors, kind='esxi_hosts')
    vms = important_vms()
    for vm in vms:
        if not collectors['vms'].get(vm, None):
            log.info("Creating a collector for: {}".format(vm))
            spawn_collector(vcenter, influx, unity, vm, collectors, kind='vms')
    for unity_stat in unity_stats():
        if not collectors['unity'].get(unity_stat, None):
            log.info("Creating a collector for Unity {}".format(unity_stat))
            spawn_collector(vcenter, influx, unity, unity_stat, collectors, kind='unity')
    return collectors


def respawn_collectors(vcenter, influx, unity, collectors, log):
    """Check for dead threads, and remake as needed.

    :Returns: Dictionary

    :param vcenter: An established connection to a vCenter server
    :type vcenter: vlab_inf_common.vmware.vCenter

    :param influx: An established connection to an InfluxDB server
    :type influx: stat_collector.lib.influxdb.InfluxDB

    :param unity: A connection to the EMC Unity API
    :type unity: stat_collector.lib.unity.Unity

    :param collectors: The threads that have been created to collect stats from VMware
    :type collectors: Dictionary

    :param log: An object for logging program messages for humans
    :type log: logging.LoggerAdapter
    """
    for vm_name, collector in collectors['vms'].items():
        if not collector.is_alive():
            log.error('Found dead collector for {} named {}'.format('VM', vm_name))
            spawn_collector(vcenter, influx, unity, vm_name, collectors, kind='vms')
    for esxi_name, collector in collectors['esxi_hosts'].items():
        if not collector.is_alive():
            log.error('Found dead collector for {} named {}'.format('ESXi', vm_name))
            spawn_collector(vcenter, influx, unity, esxi_name, collectors, kind='esxi_hosts')
    for unity_stat, collector in collectors['unity'].items():
        if not collector.is_alive():
            log.error("Found dead collector for {} named {}".format('Unity', unity_stat))
            spawn_collector(vcenter, influx, unity, unity_stat, collectors, kind='unity')
    return collectors


def main(influx_server, influx_user, influx_password, vcenter_server, vcenter_user, vcenter_password,
         unity_server, unity_user, unity_password):
    """"Entry point for collecting stats from VMWare"""
    log = get_logger('main')
    log.info('Starting Infrastructure stat collection')
    log.info('Collecting from vCenter: {}'.format(vcenter_server))
    log.info('Collecting from Unity SAN: {}'.format(unity_server))
    log.info('Writing to Influx: {}'.format(influx_server))

    vcenter = vCenter(host=vcenter_server, user=vcenter_user, password=vcenter_password)
    influx = InfluxDB(server=influx_server, user=influx_user, password=influx_password, measurement='system')
    unity = Unity(unity_server, unity_user, unity_password)
    collectors = {'vms' : {}, 'esxi_hosts' : {}, 'unity' : {}}
    while True:
        loop_start = time.time()
        collectors = create_collectors(vcenter, influx, unity, collectors, log)
        collectors = respawn_collectors(vcenter, influx, unity, collectors, log)
        loop_time = time.time() - loop_start
        # Avoids sub-second, negative, and values greater than the loop interval
        sleep_for = min(CHECK_INTERVAL, int(abs(loop_time - CHECK_INTERVAL)))
        time.sleep(sleep_for)


if __name__ == '__main__':
    main(influx_server=os.environ.get('INFLUX_SERVER', None),
         influx_user=os.environ.get('INFLUX_USER', None),
         influx_password=os.environ.get('INFLUX_PASSWORD', None),
         vcenter_server=os.environ.get('VCENTER_SERVER', None),
         vcenter_user=os.environ.get('VCENTER_USER', None),
         vcenter_password=os.environ.get('VCENTER_PASSWORD', None),
         unity_server=os.environ.get('UNITY_SERVER', None),
         unity_user=os.environ.get('UNITY_USER', None),
         unity_password=os.environ.get('UNITY_PASSWORD', None)
         )
