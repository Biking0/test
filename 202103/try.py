#!/usr/bin/env python
# -*-coding:utf-8 -*-

import os
import sys
import time
import datetime
import config
import random
import traceback

# import pubUtil

import threading
from Queue import Queue

excute_desc_sh = config.excute_ocdp_sh



class data_check():

    def __init__(self, table_name, input_date):
        self.table_name = table_name
        self.input_date = input_date
        # table_chek_name = table_name+'_chk'

     #获取稽核表内要稽核的表名

    def get_table_list(self,table_name,static_date):
        check_table_list = []
        get_list_sql = "use csap;select check_table_name from check_table_list where statis_date=\'"+static_date+"\'and ofter_chk = 0;"
        # get_list_sql = "use csap;select check_table_name from check_table_list where statis_date=\'"+static_date+"\' or statis_date = \'"+static_date2+"\' or statis_date = \'"+static_date3+"\'and ofter_chk = 0;"
        print get_list_sql
        get_table_list_sh = excute_desc_sh + ' \" ' + get_list_sql + ' \" '
        print get_table_list_sh
        table_list = os.popen(get_table_list_sh).readlines()
        for i in range(len(table_list)):
            table_name = table_list[i].replace('+', '').replace('-', '').replace('\n', '').replace('|','')
            if len(table_name)>1 and table_name.strip()!='check_table_name':
                check_table_list.append(table_name.strip())
        print check_table_list
        return  check_table_list




    # 解析表名
    def read_table_name(self):
        # f = open('/home/hive/hyn/data_check/test_table_name.txt', 'r')
        # i = 1
        static_date = self.input_date


        f = [self.table_name]

        multi_list = []

        for line in f:
            line = line.strip('\n').replace('\t', '').replace(' ', '')

            print 1, ' #########################'
            print line
            multi_list = self.get_table_list(line, static_date)


            # 开始解析
            # create_desc(line)

            # 连续读取目标表
            # break

        self.multi_thread(multi_list)


    # 分区检测，构造分区，根据需要稽核的时间段，循环生成相应的分区，判断是否为分区表,line(table_name)
    def check_partition(self, Partition_list, result_list, end_string, table_name):
        partition = ''

        # 无分区表
        if len(Partition_list) == 0:
            pass

        else:
            # 月分区，取上个月，前一个周期
            if Partition_list[0] == 'partition_month':
                today = datetime.date.today()
                first = today.replace(day=1)
                print
                self.input_date
                last_month = first - datetime.timedelta(days=1)
                last_month = last_month.strftime("%Y%m")
                # print '# last_month', last_month
                partition = 'partition_month=' + str(self.input_date)[0:6].replace('-', '')

            # 日分区，取前一天，前一个周期
            elif Partition_list[0] == 'statis_date':
                today = datetime.date.today()

                yestoday = today + datetime.timedelta(days=-1)

                # print '# yestoday', yestoday

                partition = 'statis_date=' + str(self.input_date).replace('-', '')

            elif Partition_list[0] == 'statis_month':
                today = datetime.date.today()
                first = today.replace(day=1)
                last_month = first - datetime.timedelta(days=1)
                last_month = last_month.strftime("%Y%m")
                # print '# last_month', last_month
                partition = 'statis_month=' + str(self.input_date)[0:6].replace('-', '')

            # 其他分区，先不检测，记录到文件
            else:
                pass
                # chk_error = open(config.new_path + 'chk_error.txt', 'a+')
                # chk_error.write(str(Partition_list))
                # chk_error.close()

        # 创建查询sql
        self.create_sql(table_name, result_list, partition, end_string)

    # 创建sql，进行查询,输入表名，int字段
    def create_sql(self, table_name, table_int_list, Partition_list, end_string):
        sql_part1 = ''
        sql_part3 = ''
        table_chk_name = table_name + '_chk'

        # end_string为空，该表无string类型字段
        if end_string == '':
            sql_part4 = ",'no_string_col'"
        else:
            sql_part4 = ",sum(length(" + end_string + "))"

        # 无分区
        if Partition_list == '':
            partition = 'no_partition'
            sql_part1 = "select 'DATA_SOURCE','" + table_name + "','" + partition + "', count(*)" + sql_part4
            sql_part3 = ",'REMARK',from_unixtime(unix_timestamp()) " + " from " + table_name + " ;"
            # table_chk构建
            sql_chk_part1 = "select 'DATA_SOURCE','" + table_chk_name + "','" + partition + "', count(*)" + sql_part4
            sql_chk_part3 = ",'REMARK',from_unixtime(unix_timestamp()) " + " from " + table_chk_name + " ;"



        else:
            # select 'DATA_SOURCE',table_name,'partition',count(*),concat(nvl(sum(id),''),nvl(sum(name),'')),'REMARK',from_unixtime(unix_timestamp()) from table_name where patitions='';
            sql_part1 = "select 'DATA_SOURCE','" + table_name + "','" + Partition_list + "', count(*)" + sql_part4

            # todo 无分区表，增量数据无法稽核，全表可稽核
            sql_part3 = ",'REMARK',from_unixtime(unix_timestamp()) " + " from " + table_name + " where " + Partition_list + ";"

            sql_chk_part1 = "select 'DATA_SOURCE','" + table_chk_name + "','" + Partition_list + "', count(*)" + sql_part4

            # todo 无分区表，增量数据无法稽核，全表可稽核
            sql_chk_part3 = ",'REMARK',from_unixtime(unix_timestamp()) " + " from " + table_chk_name + " where " + Partition_list + ";"

        table_int_str = ''
        for i in range(len(table_int_list)):
            table_int_str = table_int_str + "nvl(sum(%s),''),'_'," % (table_int_list[i])

        # print 'table_int_str', table_int_str

        sql_part2 = ",concat(%s)" % (table_int_str[0:-5])

        sql = sql_part1 + sql_part2 + sql_part3
        sql_chk = sql_chk_part1 + sql_part2 + sql_chk_part3
        reuslt_sql_check = "use csap;insert into table " \
                           "hive_check select t1.data_source,t1.des_tbl,t1.cyclical,(t1.count1-t2.count1) as count1," \
                           "(t1.end_string_sum - t2.end_string_sum)as end_string_sum,(t1.sum1 -t2.sum1) as sum1," \
                           "t1.remark,t1.chk_dt,t1.static_date from hive_check_1 t1,hive_check_1 t2 where " \
                           "t1.des_tbl =\'" + table_name + "\' and t2.des_tbl=\'" + table_name + "_chk\'" + " and t1.cyclical=t2.cyclical " \
                                                                                                           "and t1.cyclical=" + "\'" + Partition_list + "\'" + ";"

        after_check_to1 = "insert overwrite table check_table_list select check_table_name,case when ofter_chk= 0 then 1 end,  statis_date from check_table_list where check_table_name=\'"+ table_name +" and statis_date =\'"+self.input_date+"\';"                                                                                                  "and t1.cyclical=" + "\'" + Partition_list + "\'" + ";"


        get_to_hive_check_result = "use csap;insert into table " \
                           "hive_check_result select data_source,des_tbl,cyclical,1," \
                           "case when count1<>0 or end_string_sum<>0 or end_string_sum<>null or sum1<>0 or sum1<>null then 0 end," \
                           "remark,chk_dt,static_date from hive_check where " \
                           "des_tbl =\'" + table_name + "\'and cyclical=" + "\'" + Partition_list + "\'" + ";"
        print
        'sql select :', sql
        print
        'sql_chk:', sql_chk
        print
        'resut_sql_chek', reuslt_sql_check
        print
        'resut_sql_chek', after_check_to1
        print
        'resut_sql_chek', get_to_hive_check_result

        # 执行查询
        select_sql_sh = excute_desc_sh + ' \" ' + sql + ' \"'
        # print select_sql_sh

        self.insert_table(table_name, sql)
        self.insert_table(table_chk_name, sql_chk)
        self.inset_chk_result(reuslt_sql_check)
        self.after_check_to1(after_check_to1)
        self.to_check_result(get_to_hive_check_result)

        # 删除表结构文本文件
        delete_sh = 'rm ' + table_name + '.txt'
        # os.popen(delete_sh).readlines()

    # 构造出sql，将查询结果插入稽核结果表中
    def insert_table(self, table_name, sql):
        # 随机插入1-10稽核结果表
        # table_num = str(random.randint(1, 10))
        table_num = '1'
        chk_table_name = 'hive_check_' + table_num
        insert_sql = " use csap; insert into table " + chk_table_name + " partition (static_date=" + time.strftime(
            "%Y%m%d",
            time.localtime(
                time.time())) + ") " + sql
        # print insert_sql

        # 执行插入语句
        insert_sql_sh = excute_desc_sh + ' \" ' + insert_sql + ' \" '
        print
        insert_sql_sh
        os.popen(insert_sql_sh).readlines()

        # export_chk_result(table_name)

    # 稽核结果入表
    def inset_chk_result(self, sql):
        resut_sql_chek = excute_desc_sh + ' \" ' + sql + ' \" '
        print resut_sql_chek
        os.popen(resut_sql_chek).readlines()

    #是否稽核位置置零
    def after_check_to1(self, sql):
        insert_after_check_sql_sh = excute_desc_sh + ' \" ' + sql + ' \" '
        print insert_after_check_sql_sh
        os.popen(insert_after_check_sql_sh).readlines()

    #结果汇总
    def to_check_result(self, sql):
        get_to_hive_check_result = excute_desc_sh + ' \" ' + sql + ' \" '
        print get_to_hive_check_result
        os.popen(get_to_hive_check_result).readlines()

    # 导出稽核结果表到文件excute_ocdp_sh
    def export_chk_result(self, table_name):
        export_sql = 'use csap; select DES_TBL,CYCLICAL,COUNT1,SUM1,REMARK from chk_result;'

        export_sh = excute_desc_sh + ' \" ' + export_sql + ' \" ' + ' >> chk_result.txt'

        print
        'export_sh', export_sh

        os.popen(export_sh).readlines()



    # 解析表结构
    def desc_parser(self, table_name):
        # path = './table_desc/tb_ods_cu_mimsg_fans_day.txt'
        # path = './table_desc/'+table_name+'.txt'
        path = config.new_path + table_name + '.txt'
        # 读取文件
        desc_list = open(path, 'r').readlines()
        # 表结构列表
        result_desc = []
        # int字段结果集
        result_list = []
        # 最后一个string字段名
        end_string = ''
        # 分区信息
        Partition_list = []
        for i in range(len(desc_list)):
            list = desc_list[i].replace('+', '').replace('-', '').replace('\n', '').split('|')
            # print(list)
            # print(list)
            lis = []
            if len(list) > 1:
                # print('list:',list)

                for j in range(len(list)):
                    if len(list[j]) > 1:
                        lis.append(list[j].strip())

                    # print(list[j].strip())
            if len(lis) > 1:
                result_desc.append(lis)
        # print(result_desc)
        # 获取int型字段名
        for i in range(len(result_desc)):
            if result_desc[i][1] == 'int':
                result_list.append(result_desc[i][0])
                break

        # 获取分区信息
        for i in range(len(result_desc)):
            if 'Partition' in result_desc[i][0]:
                # print(result_desc[i][0])
                for j in range(i + 1, len(result_desc)):
                    if '#' not in result_desc[j][0]:
                        Partition_list.append(result_desc[j][0])

        # 获取string类型字段名
        # list反转
        result_desc.reverse()
        for i in range(len(result_desc)):
            if '#' not in result_desc[i][0] and result_desc[i][0] not in Partition_list and result_desc[i][
                1] == 'string':
                end_string = result_desc[i][0]
                break

        print
        Partition_list, result_list, end_string, table_name
        self.check_partition(Partition_list, result_list, end_string, table_name)

    # 生成desc表结构文件
    def create_desc(self, table_name):
        # 生产环境
        desc_sh = excute_desc_sh + " \' desc  " + table_name + ' \' > ' + config.new_path + table_name + '.txt'

        print
        '###################################'
        # 测试环境
        # desc_sh = "beeline -u 'jdbc:hive2://172.22.248.19:10000/default' -n csap -p @WSX2wsx -e 'desc  " + table_name + ' \' > ./' + table_name + '.txt'

        print
        '############', desc_sh

        os.popen(desc_sh).readlines()
        self.desc_parser(table_name)

    # 遍历列表
    def read_list(self, num, data_queque, result_queque):
        for i in range(data_queque.qsize()):
            try:
                if not data_queque.empty():
                    # 出队列
                    table_name = data_queque.get()

                    # print 'table_name', table_name
                    self.create_desc(table_name)

            except Exception as e:
                print
                traceback.print_exc()
                print
                e
                f = open('./error_info.log', 'a+')
                f.write(str(e))
                f.close()
                continue

    # 多线程
    def multi_thread(self, multi_list):
        print
        'multi_list', multi_list

        data_queque = Queue()
        result_queque = Queue()

        # 数据放入队列
        for i in range(len(multi_list)):
            data_queque.put(multi_list[i])

        # 设置并发数
        a = 1
        # list分块，调用多线程
        for i in range(a):
            # list分块，调用多线程
            multi1 = threading.Thread(target=self.read_list, args=(5, data_queque, result_queque))

            multi1.start()


# read_table_name()


# 启动
if __name__ == '__main__':

    input_length = len(sys.argv)
    print
    'input_data: ', len(sys.argv)

    if input_length == 3:

        # 批次号，分批处理ss
        table_name = sys.argv[1]
        input_date = sys.argv[2]
        statis_date = [input_date,str(int(input_date)-1),str(int(input_date)-2)]
        for i in range(3):
            run = data_check(table_name, statis_date[i])
            run.read_table_name()


        # run = data_check(table_name, input_date)
        #
        # # run.desc_parser(table_name)
        # run.read_table_name()
    else:
        print
        '输入参数有误'
