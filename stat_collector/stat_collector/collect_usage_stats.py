# -*- coding: UTF-8 -*-
"""Entry point for collecting usage stats about user's labs"""
import os
import time

from vlab_inf_common.vmware import vCenter, vim

from stat_collector.lib.influxdb import InfluxDB
from stat_collector.lib.std_logger import get_logger
from stat_collector.lib.vsphere_collectors import UserCollector

CHECK_INTERVAL = 600
USERS_DIR_NAME = 'users'


def spawn_collector(vcenter, username, influx):
    """A small abstraction to start UserCollector threads

    :Returns: stat_collector.lib.vsphere_collectors.UserCollector

    :param vcenter: An established connection to a vCenter server
    :type vcenter: vlab_inf_common.vmware.vCenter

    :param username: The name of the user to collect usage stats about
    :type username: String

    :param influx: An established connection to an InfluxDB server
    :type influx: stat_collector.lib.influxdb.InfluxDB
    """
    collector = UserCollector(vcenter, username, USERS_DIR_NAME, influx)
    collector.start()
    return collector


def lookup_users(vcenter):
    """Obtain a list of current vLab users

    :Returns: Set

    :param vcenter: An established connection to a vCenter server
    :type vcenter: vlab_inf_common.vmware.vCenter
    """
    users = set()
    parent_dir = vcenter.get_by_name(vim.Folder, USERS_DIR_NAME)
    for folder in parent_dir.childEntity:
        users.add(folder.name)
    return users


def do_work(vcenter, influx, user_collectors, log):
    """
    :param vcenter: An established connection to a vCenter server
    :type vcenter: vlab_inf_common.vmware.vCenter

    :param username: The name of the user to collect usage stats about
    :type username: String

    :param influx: An established connection to an InfluxDB server
    :type influx: stat_collector.lib.influxdb.InfluxDB
    """
    users = lookup_users(vcenter)
    for user in users:
        if user not in user_collectors:
            log.info('Spawning collector for {}'.format(user))
            user_collectors[user] = spawn_collector(vcenter, user, influx)

    deleted_accounts = set(user_collectors.keys()) - users
    for deleted_account in deleted_accounts:
        log.info("Deleting collector for {}".format(deleted_account))
        user_collectors[deleted_account].keep_running = False
        user_collectors.pop(deleted_account)

    for user, collector in user_collectors.items():
        if not collector.is_alive:
            log.error('Dead collector for: {}'.format(user))
            user_collectors[user] = spawn_collector(vcenter, user, influx)
    return user_collectors


def main(influx_server, influx_user, influx_password, vcenter_server, vcenter_user, vcenter_password):
    """Run the main loop for collecting user usage stats"""
    log = get_logger('main')
    log.info("Starting user stat collection")
    log.info('Collecting from vCenter: {}'.format(vcenter_server))
    log.info('Writing to Influx: {}'.format(influx_server))
    vcenter = vCenter(host=vcenter_server, user=vcenter_user, password=vcenter_password)
    influx = InfluxDB(server=influx_server, user=influx_user, password=influx_password, measurement='usage')
    user_collectors = {}
    while True:
        loop_start = time.time()
        user_collectors = do_work(vcenter, influx, user_collectors, log)
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
         vcenter_password=os.environ.get('VCENTER_PASSWORD', None))
