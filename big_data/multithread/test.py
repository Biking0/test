#!/usr/bin/env python
# -*-coding:utf-8 -*-
# ***************************************************************************
# 文件名称：sy_hive_data_check.py
# 功能描述：hive表数据稽核
# 输 入 表：
# 输 出 表：
# 创 建 者：hyn
# 创建日期：20200624
# 修改日志：
# 修改日期：
# ***************************************************************************
# 程序调用格式：python sy_hive_data_check.py
# ***************************************************************************

import threading
import time


def sing(num, a):
    print 'a', a
    for i in range(num):
        print "sing%d" % i
        time.sleep(0.5)


def dance(num):
    for i in range(num):
        print "dancing%d" % i
        time.sleep(0.5)


def main():
    """创建启动线程"""
    a = 2
    b = 3
    t_sing = threading.Thread(target=sing, args=(5, a))
    # t_sing = threading.Thread(target=sing, args=(5, b))
    # t_dance = threading.Thread(target=dance, args=(6,b))
    t_dance = threading.Thread(target=sing, args=(6, b))
    t_sing.start()
    t_dance.start()


if __name__ == '__main__':
    main()
