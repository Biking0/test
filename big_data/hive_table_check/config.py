# encoding=utf-8
# hive_table_check
# by hyn
# 20200224

# hadoop path
bdi_hadoop_path='hadoop fs -du -s -h  /user/hive/warehouse/asiainfoh.db/'

# table_name_list
# �����������ӳ�ʱ��

# Сʱ���ӳٵ�λ��Сʱ
hour_table=[
['dw_locl_wh_lacci_4g_time_yyyymmddhh',2,'�人����Сʱ'],
['dw_loc_event_dt_yyyymmddhh',3,'234g�ں�Сʱ�ۻ�������']
]

# �ձ��ӳٵ�λ��Сʱ
day_table=[
['dw_loc_event_dt_yyyymmddhh',3,'test']
]
