#!/usr/bin/env python
# -*-coding:utf-8 -*-
# *******************************************************************************
# �ļ����ƣ�send_sms.sh
# ������������ض��ŷ���ͨ�ýű������ڽű��ڲ����øýű�
# ����������������ݣ����˲���
# �� �� ��
# �� �� �ߣ�hyn
# �������ڣ�20191010
# �޸���־��
# �޸����ڣ�
# *******************************************************************************
# ������ø�ʽ��sh send_sms.sh sms_info ALL
# *******************************************************************************

# ���������ļ�·��
sms_info_path=$1
# �����û�����
#all_info='HYN'
all_info='ALL'

# ��ȡ�������ݣ����س��滻Ϊ�ո�
sms_info=`cat $sms_info_path | tr '\n' ' '`

# ���ŷ���ʱ��
send_time="��ǰʱ�䣺`date +"%m-%d %H:%M"`��"

echo '�������ݣ�',$sms_info $send_time

# ���ݿ��¼����
exec_mysql_ng="mysql -h10.97.192.180 -ungtassuite -pAj7y32h! ngtassuite"

# ���Ͷ��ţ������������Ϣ
${exec_mysql_ng} -e "INSERT INTO tb_sys_sms_send_cur(serv_number, send_date,text,opt_user,opt_date ) SELECT serv_number, now(), '$sms_info', opt_user, now() FROM tb_sys_sms_phone WHERE opt_user = 'ocetl' and sms_id = '$all_info';" 2>/dev/null