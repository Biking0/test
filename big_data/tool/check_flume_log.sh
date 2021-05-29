#!/bin/bash
# ***************************************************************************
# �ļ����ƣ�check_flume_log.sh
# ����������shell����ͷ
# �� �� ��
# �� �� ��
# �� �� �ߣ�
# �������ڣ�
# �޸���־��
# �޸����ڣ�
# ***************************************************************************
# ������ø�ʽ��sh code_head.sh
# ***************************************************************************

EXEC_MYSQL_NG="mysql -h10.97.192.180 -ungtassuite -pAj7y32h! ngtassuite"
function send_sms()
{
        ${EXEC_MYSQL_NG} -e "INSERT INTO tb_sys_sms_send_cur(serv_number, send_date,text,opt_user,opt_date ) SELECT serv_number, now(), '$1', opt_user, now() FROM tb_sys_sms_phone WHERE opt_user = 'ocetl' and sms_id = '$2';" 2>/dev/null
}



for i in flume-agent1 flume-agent2
do
  ssh 10.218.146.103 "tail -3000 /var/log/flume/$i.log > /home/ocdp/wangcc/$i.txt"

  num=`ssh 10.218.146.103 "grep waiting /home/ocdp/wangcc/$i.txt | wc -l"`

  error_num=`ssh 10.218.146.103 "grep ERROR /home/ocdp/wangcc/$i.txt | wc -l"`

  current_time=`date +"%Y-%m-%d %H:%M"`

  if [ $num -eq 0 -a $error_num -eq 0 ] ;then
          echo "$i ��־��������,��ǰʱ��Ϊ��$current_time"
          #send_sms "$i ��־��������,��ǰʱ��Ϊ��$current_time" "w01"
  else
     echo "$i ���30������־�ļ����� $num ��waiting������ $error_num ��ERROR����,��鿴, ��ǰʱ��Ϊ�� $current_time "
     send_sms "$i���30������־�ļ�����$num��waiting������$error_num��ERROR����,��鿴, ��ǰʱ��Ϊ��$current_time" "ALL"

        fi

done
