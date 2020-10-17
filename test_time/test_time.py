#!/usr/bin/env python
# -*-coding:utf-8 -*-

import os
import sys

from datetime import datetime
import datetime as date_time

import calendar

start_date = '202002'
# 日期格式
day_format = '%Y%m'

last_year_str = str(datetime.strptime(start_date, day_format).year - 1) + start_date[-2:]

last_year_time = datetime.strptime(last_year_str, day_format)

this_month_start = str(date_time.datetime(last_year_time.year, last_year_time.month, 1))[0:10].replace('-', '')
this_month_end = str(date_time.datetime(last_year_time.year, last_year_time.month,
                                        calendar.monthrange(last_year_time.year, last_year_time.month)[1]))[
                 0:10].replace('-', '')

print this_month_start, this_month_end
