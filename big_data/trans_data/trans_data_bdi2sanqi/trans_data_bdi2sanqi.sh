#!/bin/bash
# ***************************************************************************
# �ļ����ƣ�trans_data_bdi2sanqi.sh
# ����������������ƽ̨hdfs����ͬ��������hdfs��Ⱥ�������±��ձ�Сʱ��
# 1.������ƽ̨hdfs����ͬ��������hdfs��Ⱥ
# 2.֧���±��ձ�Сʱ��
# 3.hdfs�����ļ�������ɺ󣬲��������ݵ�hive�⣬�Զ���ӷ���
# 4.ע����Ŀ�꼯Ⱥ������Ѵ��ڣ���Ҫ��ɾ���������������ʧ��
# 5.ɾ��������alter table dw_ay_spots_user_yyyymmdd drop partition(day_id=20191214);
# �� �� ��source_table_name
# �� �� ��target_table_name
# �� �� �ߣ�wxq
# �������ڣ�20190604
# �޸���־��bdiĿ¼Ȩ��
# �޸����ڣ�20200325
# ***************************************************************************
# ������ø�ʽ��sh trans_data_bdi2sanqi.sh source_table_name 20190604 target_table_name
# ***************************************************************************

# ִ�л����Լ��û���֤
source /opt/fi_client/bigdata_env
kinit -kt /home/ocdp/user.keytab asiainfouser1

# д��־
Write_Log_File()
{
    now_time=`date +"%Y-%m-%d %H:%M:%S"`
    echo "${now_time} ${1}"
    echo "${now_time} ${1}" >> ${Log_File}
}

# ���������������֧��Сʱ���ձ��±�
SOURCEDIR=$1
DATETIME=$2
TODIR=$3 
if [ ${#DATETIME} -eq 10 ]; then
  SHOWMONTH=${DATETIME:0:6} 
  SHOWDAY=${DATETIME:0:8} 
  SHOWHOUR=${DATETIME}  
  source_dir=${SOURCEDIR}/month_id=${SHOWMONTH}/day_id=${SHOWDAY}/hour_id=${SHOWHOUR}
  target_dir=${TODIR}/month_id=${SHOWMONTH}/day_id=${SHOWDAY}
  partition_dir="month_id=${SHOWMONTH},day_id=${SHOWDAY},hour_id=${SHOWHOUR}"
elif [ ${#DATETIME} -eq 8 ]; then
  SHOWMONTH=${DATETIME:0:6} 
  SHOWDAY=${DATETIME}
  source_dir=${SOURCEDIR}/month_id=${SHOWMONTH}/day_id=${SHOWDAY} 
  target_dir=${TODIR}
  partition_dir="month_id=${SHOWMONTH},day_id=${SHOWDAY}"
elif [ ${#DATETIME} -eq 6 ]; then
  SHOWMONTH=${DATETIME}
  source_dir=${SOURCEDIR}/month_id=${SHOWMONTH} 
  target_dir=${TODIR}
  partition_dir="month_id=${SHOWMONTH}"
else 
  echo "datetime is not valid"
  exit 1
fi

# �������ݼ��ز���

echo "source_dir: ${source_dir}"
echo "target_dir: ${target_dir}"
echo "to_dir: ${TODIR}"
echo "partition_dir:${partition_dir}"


#��־�ļ��������Զ��壬�����õ��ǽű�����+${DATETIME}.log
sh_name=`echo ${0} | awk -F "/" '{print $NF}' | awk -F "." '{print $1}'`
Log_Path="/hdfs/data01/trans"

Log_File="${Log_Path}/${sh_name}_${SOURCEDIR}_TO_${TODIR}_${DATETIME}.log"
echo ${Log_File}

#�ж�namenode�������ip��
hadoop fs -ls -d hdfs://10.218.59.8:25000/user/hive/warehouse/hwcdm.db/ &>/dev/null
if [ $? -eq 0 ]
then
    Write_Log_File "10.218.59.8 is active"
    DATA_FROM='10.218.59.8'
else
    Write_Log_File "10.218.59.7 is active"
    DATA_FROM='10.218.59.7'
fi

hadoop fs -ls -d hdfs://10.93.171.97:8020/user/hive/warehouse/ &>/dev/null
if [ $? -eq 0 ]
then
    Write_Log_File "10.93.171.97 is active"
    DATA_TO='10.93.171.97'
else
    Write_Log_File "10.93.171.100 is active"
    DATA_TO='10.93.171.100'
fi

while true
do
    # �ж������Ƿ�׼������ ,��ȡ֪ͨ�ļ��Ƿ���� /asiainfo/dependent/tas_gprs_lte_app_20190601.txt
    file_num=`hadoop fs -ls  hdfs://$DATA_FROM:25000/asiainfo/dependent/${SOURCEDIR}_${DATETIME}.txt | wc -l`
    echo ${file_num}
    if [ ${file_num} -eq 1 ]
    then 
        Write_Log_File "hadoop fs -mkdir -p hdfs://$DATA_TO:8020/user/hive/warehouse/${target_dir} >> ${Log_File}"
        hadoop fs -mkdir -p hdfs://$DATA_TO:8020/user/hive/warehouse/${target_dir} >> ${Log_File}

        Write_Log_File "hadoop distcp -i hdfs://$DATA_FROM:25000/user/hive/warehouse/asiainfoh.db/${source_dir}  hdfs://$DATA_TO:8020/user/hive/warehouse/${target_dir} >> ${Log_File}"
        hadoop distcp -i hdfs://$DATA_FROM:25000/user/hive/warehouse/asiainfoh.db/${source_dir}  hdfs://$DATA_TO:8020/user/hive/warehouse/${target_dir} >> ${Log_File}
        kdestroy
        Write_Log_File "hadoop fs -chmod -R 777 hdfs://$DATA_TO:8020/user/hive/warehouse/${target_dir} >> ${Log_File}"
        hadoop fs -chmod -R 777 hdfs://$DATA_TO:8020/user/hive/warehouse/${target_dir} >> ${Log_File}      
        # hadoop fs -chown -R ocetl:ocdp hdfs://$DATA_TO:8020/user/hive/warehouse/${target_dir}

                # �������ݵ�hive��
                #Write_Log_File "beeline -u jdbc:hive2://ocdpdn80:10000/default -n ocetl -p demo -e load data inpath /user/hive/warehouse/${target_dir} overwrite into table ${TODIR} partition(${partition_dir}) >> ${Log_File}"
                #beeline -u "jdbc:hive2://ocdpdn80:10000/default" -n ocetl -p demo -e "load data inpath '/user/hive/warehouse/${target_dir}' overwrite into table ${TODIR} partition(${partition_dir})"
                beeline -u "jdbc:hive2://ocdpdn80:10000/default" -n ocetl -p demo -e "load data inpath '/user/hive/warehouse/${target_dir}/day_id=${SHOWDAY}' overwrite into table ${TODIR} partition(${partition_dir})"
        if [ $? -eq 0 ]
        then
            Write_Log_File "$SOURCEDIR������װ�سɹ�"
            break
        else
            Write_Log_File "$SOURCEDIR������װ��ʧ��"
            exit 1
        fi
    else
        Write_Log_File "xxxxxδ׼���������ȴ�5����"
        sleep 300
    fi
done
