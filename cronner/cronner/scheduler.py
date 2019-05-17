# -*- coding: UTF-8 -*-
"""Define when specific tasks should be ran"""
import schedule

from cronner.elasticsearch import prune_indices, add_field_data


schedule.every().day.at("01:00").do(prune_indices)
schedule.every(10).minutes.do(add_field_data)
