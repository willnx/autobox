# -*- coding: UTF-8 -*-
"""Defines how to run cronner"""
import time

from setproctitle import setproctitle

from cronner.scheduler import schedule


def main():
    """Entry point logic for running cron-like tasks"""
    while True:
        schedule.run_pending()
        time.sleep(1)


if __name__ == '__main__':
    setproctitle('Cronner')
    main()
