# encoding=utf8
# Scheduled to run on Friday
# by hyn
# 20200220

import time
import datetime

while True:
    # 在周五指定时间运行
    today = datetime.datetime.now().weekday() + 1
    print today

    # 周五当天每小时检测是否到15点以后，非周五每5个小时检测一次
    if today == 4:
        while True:

            # 获取周五当天15点时间
            today_str = time.strftime('%Y-%m-%d', time.localtime(time.time()))
            stamptime_15 = time.mktime(time.strptime(today_str + ' 15:00:00', '%Y-%m-%d %H:%M:%S'))

            # 获取当前时间戳
            stamptime_now = time.time()

            # 是否到当天15点以后
            print stamptime_15
            print stamptime_now

            if stamptime_now > stamptime_15:
                print 'start run'

                # 调用程序,python sh

                #python_sh =

                # 执行完程序退出
                break

            time.sleep(3)
            print 'sleep 3'

    week = datetime.datetime.strptime("20200220", "%Y%m%d").weekday() + 1
    print week
    time.sleep(2)
    print 'sleep 2'
