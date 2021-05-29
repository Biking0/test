#!/usr/bin/env python
# -*-coding:utf-8 -*-
# ***************************************************************************
# 文件名称：conn_db.py
# 功能描述：迁移Hive表
# 输 入 表：
# 输 出 表：
# 创 建 者：hyn
# 创建日期：20200808
# 修改日志：
# 修改日期：
# ***************************************************************************
# 程序调用格式：python conn_db.py
# ***************************************************************************

import os
import sys
from datetime import datetime
import datetime as date_time
import pymysql

mysql_sh = "mysql -h ritds-dataos.mysql.svc.cs1-hua.hpc -P 20001 -u dataos_dev -pqXliH9*Ro#qDGomY dataos_dev -e ' "


# 连接
def conn_db():
    conn = pymysql.connect(host="ritds-dataos.mysql.svc.cs1-hua.hpc", port=20001, user="dataos_dev",
                           passwd="qXliH9*Ro#qDGomY", db="dataos_dev", charset="utf8")

    return conn


def select(sql):
    conn = conn_db()
    cursor = conn.cursor()

    cursor.execute(sql)

    result = cursor.fetchall()
    cursor.close()
    print type(result)

    # print result
    return result


def insert(sql):
    conn = conn_db()
    cursor = conn.cursor()

    cursor.execute(sql)

    result = cursor.fetchall()

    conn.commit()
    cursor.close()
    conn.close()

    print type(result)

    # print result
    return result


# print select("show tables;")

# insert("insert into test (id) values ('123')")
