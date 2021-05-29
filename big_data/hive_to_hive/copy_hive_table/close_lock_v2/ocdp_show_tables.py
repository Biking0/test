#!/usr/bin/env python
# -*-coding:utf-8 -*-
# ***************************************************************************
# 文件名称：ocdp_show_tables.py
# 功能描述：导出ocdp集群所有表名
# 输 入 表：
# 输 出 表：
# 创 建 者：hyn
# 创建日期：20200617
# 修改日志：
# 修改日期：
# ***************************************************************************
# 程序调用格式：python ocdp_show_tables.py
# ***************************************************************************

import os
import sys

# 功能
# 1.show_tables到列表变量
# 2.过滤符号排序，输出到文件

# 连接ocdp集群
excute_ocdp_sh = "beeline -u 'jdbc:hive2://hua-dlzx2-a0202:10000/csap' -n ocdp -p 1q2w1q@W -e \" "


def show_tables():
    show_tables_sql = 'show tables;'
    show_tables_sh = excute_ocdp_sh + '\"' + show_tables_sql + '\"'

    print show_tables_sh
    show_tables_list = os.popen(show_tables_sh).readlines()
    # print show_tables_list

    result_list = []
    for i in range(len(show_tables_list)):

        show_tables_list[i] = show_tables_list[i].replace('\n', '').replace(' ', '').replace('|', '')
        if ('+' in show_tables_list[i]) or ('tab_name' in show_tables_list[i]):
            continue
        result_list.append(show_tables_list[i])

    # result_list=result_list.sorted()

    result_list= sorted(result_list)
    ocdp_table_name=open('./ocdp_table_name.txt','w')
    for i in range(len(result_list)):
        ocdp_table_name.write(result_list[i]+'\n')


show_tables()
