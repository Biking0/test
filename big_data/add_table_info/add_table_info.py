#!/usr/bin/env python
# -*-coding:utf-8 -*-
# ***************************************************************************
# 文件名称：add_table_info.py
# 功能描述：添加统计信息
# 输 入 表：
# 输 出 表：
# 创 建 者：hyn
# 创建日期：20200901
# 修改日志：
# 修改日期：
# ***************************************************************************
# 程序调用格式：python add_table_info.py
# ***************************************************************************

import os
import sys
from datetime import datetime
import datetime as date_time

day = 'statis_date'

new_hive = "beeline -u 'jdbc:hive2://172.19.40.241:10000/csap' -n ocdp -p 1q2w1q@W -e ' "

start_date = '20200825'
end_date = '20200930'
# 日期格式
day_format = '%Y%m%d'


def add_table_info(table_name_list):
    add_partition = open('./add_table_info.sql', 'a+')

    # 表名
    for i in range(len(table_name_list)):
        add_info_sql = "ANALYZE TABLE " + table_name_list[i] + "  COMPUTE STATISTICS;"

        add_partition.write(add_info_sql + '\n')


def read_table_name():
    f = open('./test_table_name.txt', 'r')

    # day_list = ['']
    multi_list = []
    for line in f.readlines():
        line = line.strip('\n')

        # print 1, ' #########################'
        # print line
        # get_table_struct(line)
        multi_list.append(line)

    add_table_info(multi_list)


# create_date()
read_table_name()
