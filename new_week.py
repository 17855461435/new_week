import pandas as pd
import pymysql
import openpyxl
from datetime import datetime, date, timedelta


def connect_mysql(sql):
    """
    连接mysql数据库，并返回查询结果
    """
    host = '192.168.0.166'
    user = 'zhangpeng'
    password = 'zhangpeng1234'
    database = 'mydatabase'
    port = 3307

    pd.set_option('display.max_columns', 70)
    pd.set_option('display.width', 1000)

    try:
        db = pymysql.connect(host=host, user=user, password=password, database=database, port=port)
        print(f'数据库{database}连接成功')
    except pymysql.Error as err:
        print(f'数据库{database}连接失败' + str(err))
    # cursor = db.cursor()
    result_sql = pd.read_sql(sql, db)
    print('查询中...')
    db.close()
    print('查询完成！已断开数据库连接')
    return result_sql


# 时间范围
start_time = '2022-10-10'
end_time = '2022-10-17'   # 一周数据
# 读取excel文件
wb = openpyxl.load_workbook('周报数据.xlsx')

# 均单数
sql_avg = f"""
SELECT a.time,'东城北' 区域,a.订单数 订单数,a.人数 人数   from (
SELECT DATE(startTime) time ,groupName  区域,COUNT(workorderNumber) 订单数, COUNT(distinct dutyPerson)  人数 from 
  mj_work_order_2  WHERE   startTime  >= '{start_time}'  and  startTime  < '{end_time}'    and groupname  like '%东城北%' 
  GROUP BY time ORDER BY time
) a GROUP BY 1
UNION  all
SELECT a.time,'东城南' 区域,a.订单数 订单数,a.人数 人数  from (
SELECT DATE(startTime) time ,groupName  区域,COUNT(workorderNumber) 订单数, COUNT(distinct dutyPerson)  人数 from 
  mj_work_order_2  WHERE   startTime  >= '{start_time}'  and  startTime  < '{end_time}'    and groupname  like '%东城南%' 
  GROUP BY time ORDER BY time
) a  GROUP BY 1
UNION  all
SELECT a.time,'都江堰' 区域,a.订单数 订单数,a.人数 人数  from (
SELECT DATE(startTime) time ,groupName  区域,COUNT(workorderNumber) 订单数, COUNT(distinct dutyPerson)  人数 from 
  mj_work_order_2  WHERE  startTime  >= '{start_time}'  and  startTime  < '{end_time}'   and groupname  like '%都江堰%' 
  GROUP BY time ORDER BY time
) a  GROUP BY 1
UNION all
SELECT a.time,'高新' 区域,a.订单数 订单数,a.人数 人数 from (
SELECT DATE(startTime) time ,groupName  区域,COUNT(workorderNumber) 订单数, COUNT(distinct dutyPerson)  人数 from 
  mj_work_order_2  WHERE  startTime  >= '{start_time}'  and  startTime  < '{end_time}'   and groupname  like '%高新%' 
  GROUP BY time ORDER BY time
) a  GROUP BY 1
UNION  all
SELECT a.time,'龙泉' 区域,a.订单数 订单数,a.人数 人数  from (
SELECT DATE(startTime) time ,groupName  区域,COUNT(workorderNumber) 订单数, COUNT(distinct dutyPerson)  人数 from 
  mj_work_order_2  WHERE  startTime  >= '{start_time}'  and  startTime  < '{end_time}'    and groupname  like '%龙泉%' 
  GROUP BY time ORDER BY time
) a  GROUP BY 1
UNION  all
SELECT a.time,'郫都' 区域,a.订单数 订单数,a.人数 人数  from (
SELECT DATE(startTime) time ,groupName  区域,COUNT(workorderNumber) 订单数, COUNT(distinct dutyPerson)  人数 from 
  mj_work_order_2  WHERE  startTime  >= '{start_time}'  and  startTime  < '{end_time}'    and groupname  like '%郫都%' 
  GROUP BY time ORDER BY time
) a  GROUP BY 1
UNION  all
SELECT a.time,'温江' 区域,a.订单数 订单数,a.人数 人数  from (
SELECT DATE(startTime) time ,groupName  区域,COUNT(workorderNumber) 订单数, COUNT(distinct dutyPerson)  人数 from 
  mj_work_order_2  WHERE   startTime  >= '{start_time}'  and  startTime  < '{end_time}'   and groupname  like '%温江%' 
  GROUP BY time ORDER BY time
) a  GROUP BY 1
UNION  all
SELECT a.time,'新都' 区域,a.订单数 订单数,a.人数 人数  from (
SELECT DATE(startTime) time ,groupName  区域,COUNT(workorderNumber) 订单数, COUNT(distinct dutyPerson)  人数 from 
  mj_work_order_2  WHERE   startTime  >= '{start_time}'  and  startTime  < '{end_time}'  and groupname  like '%新都%' 
  GROUP BY time ORDER BY time
) a  GROUP BY 1
UNION  all
SELECT a.time,'城西' 区域,a.订单数 订单数,a.人数 人数  from (
SELECT DATE(startTime) time ,groupName  区域,COUNT(workorderNumber) 订单数, COUNT(distinct dutyPerson)  人数 from 
  mj_work_order_2  WHERE   startTime  >= '{start_time}'  and  startTime  < '{end_time}'   and groupname  like '%城西%' 
  GROUP BY time ORDER BY time
) a  GROUP BY 1 
"""


avg = connect_mysql(sql_avg)
ws1 = wb['均单数']
a1 = avg['time']
a2 = avg['区域']
a3 = avg['订单数']
a4 = avg['人数']

for i, 时间, 区域, 订单数, 人数 in zip(range(2, len(avg)+2), a1, a2, a3, a4):
    ws1[f'A{i}'] = 时间
    ws1[f'B{i}'] = 区域
    ws1[f'C{i}'] = 订单数
    ws1[f'D{i}'] = 人数

wb.save(filename='周报数据.xlsx')
print('均单数查询完成...........')

# 区域各类型工单
sql_work = f"""
SELECT  type,groupname,count(*) num from 
(SELECT  workorderNumber,type,'充电' type2,startTime,endTime,dutyPerson,groupName,mileageStatistics from 
mj_work_order_2_charge WHERE   startTime  >= '{start_time}'  and  startTime  < '{end_time}' and `status` like  '%完成%'
union all 
SELECT  workorderNumber,type,'清洁' type2,startTime,endTime,dutyPerson,groupName,mileageStatistics from 
mj_work_order_2_clean WHERE  startTime  >= '{start_time}'  and  startTime  < '{end_time}'and `status` like  '%完成%'
union all 
SELECT  workorderNumber,type,'保养' type2,startTime,endTime,dutyPerson,groupName,mileageStatistics from 
mj_work_order_2_maintain WHERE   startTime  >= '{start_time}'  and  startTime  < '{end_time}' and `status` like  '%完成%'
union all 
SELECT  workorderNumber,type,moveType,startTime,endTime,dutyPerson,groupName,mileageStatistics from 
mj_work_order_2_move WHERE  startTime  >= '{start_time}'  and  startTime  < '{end_time}' and `status` like  '%完成%'
union all 
SELECT  workorderNumber,type,workDescription,startTime,endTime,dutyPerson,groupName,mileageStatistics 
from mj_work_order_2_other WHERE   startTime  >= '{start_time}'  and  startTime  < '{end_time}' and `status` like  '%完成%'
union all 
SELECT  workorderNumber,type,'巡检' type2,startTime,endTime,dutyPerson,groupName,mileageStatistics from 
mj_work_order_2_patrol WHERE  startTime  >= '{start_time}'  and  startTime  < '{end_time}' and `status` like  '%完成%'
union all 
SELECT  workorderNumber,type,'维修' type2,startTime,endTime,dutyPerson,groupName,mileageStatistics from 
mj_work_order_2_repair WHERE   startTime  >= '{start_time}'  and  startTime  < '{end_time}'and `status` like  '%完成%'
union all 
SELECT  workorderNumber,type,saveType,startTime,endTime,dutyPerson,groupName,mileageStatistics from 
mj_work_order_2_save WHERE   startTime  >= '{start_time}'  and  startTime  < '{end_time}' and `status` like  '%完成%')  a
where  groupName  not   REGEXP '商旅组|客服|商务|星辰便利电|维修站|车务|成都楼兰|稽查|市场|成都培训'  and  groupName is not null 
GROUP BY groupname,type  ORDER BY groupname,type
"""
work = connect_mysql(sql_work)
ws2 = wb['区域各类型工单']
a1 = work['type']
a2 = work['groupname']
a3 = work['num']
for i, type, groupname, num in zip(range(2, len(work)+2), a1, a2, a3):
    ws2[f'A{i}'] = groupname
    ws2[f'B{i}'] = type
    ws2[f'C{i}'] = num
wb.save(filename='周报数据.xlsx')
print('区域各类型工单完成..........')

# 各类工单时长里程
sql_time = f"""
SELECT type,type2,round(sum(UNIX_TIMESTAMP(endTime)-UNIX_TIMESTAMP(startTime))/3600,1)  num from
(SELECT  workorderNumber,type,'充电' type2,startTime,endTime,dutyPerson,groupName,mileageStatistics from
mj_work_order_2_charge WHERE   startTime  >= '{start_time}'  and  startTime  < '{end_time}'  and `status` like  '%完成%'
union 
SELECT  workorderNumber,type,'清洁' type2,startTime,endTime,dutyPerson,groupName,mileageStatistics from
mj_work_order_2_clean WHERE   startTime  >= '{start_time}'  and  startTime  < '{end_time}' and `status` like  '%完成%'
union 
SELECT  workorderNumber,type,'保养' type2,startTime,endTime,dutyPerson,groupName,mileageStatistics from
mj_work_order_2_maintain WHERE  startTime  >= '{start_time}'  and  startTime  < '{end_time}' and `status` like  '%完成%'
union 
SELECT  workorderNumber,type,moveType,startTime,endTime,dutyPerson,groupName,mileageStatistics from
mj_work_order_2_move WHERE  startTime  >= '{start_time}'  and  startTime  < '{end_time}'  and `status` like  '%完成%'
union 
SELECT  workorderNumber,type,workDescription,startTime,endTime,dutyPerson,groupName,mileageStatistics from
mj_work_order_2_other WHERE  startTime  >= '{start_time}'  and  startTime  < '{end_time}'  and `status` like  '%完成%'
union 
SELECT  workorderNumber,type,'巡检' type2,startTime,endTime,dutyPerson,groupName,mileageStatistics from
mj_work_order_2_patrol WHERE  startTime  >= '{start_time}'  and  startTime  < '{end_time}'  and `status` like  '%完成%'
union 
SELECT  workorderNumber,type,'维修' type2,startTime,endTime,dutyPerson,groupName,mileageStatistics from
mj_work_order_2_repair WHERE  startTime  >= '{start_time}'  and  startTime  < '{end_time}'  and `status` like  '%完成%'
union 
SELECT  workorderNumber,type,saveType,startTime,endTime,dutyPerson,groupName,mileageStatistics from
mj_work_order_2_save WHERE  startTime  >= '{start_time}'  and  startTime  < '{end_time}'  and `status` like  '%完成%')  b
GROUP BY type,type2  order by type,num desc
"""
sql_mile = f"""
SELECT type,type2,round(SUM(mileageStatistics),1) num   from
(SELECT  workorderNumber,type,'充电' type2,startTime,endTime,dutyPerson,groupName,mileageStatistics from
mj_work_order_2_charge WHERE   startTime  >= '{start_time}'  and  startTime  < '{end_time}'  and `status` like  '%完成%'
union 
SELECT  workorderNumber,type,'清洁' type2,startTime,endTime,dutyPerson,groupName,mileageStatistics from
mj_work_order_2_clean WHERE   startTime  >= '{start_time}'  and  startTime  < '{end_time}'  and `status` like  '%完成%'
union 
SELECT  workorderNumber,type,'保养' type2,startTime,endTime,dutyPerson,groupName,mileageStatistics from
mj_work_order_2_maintain WHERE  startTime  >= '{start_time}'  and  startTime  < '{end_time}'  and `status` like  '%完成%'
union
SELECT  workorderNumber,type,moveType,startTime,endTime,dutyPerson,groupName,mileageStatistics from
mj_work_order_2_move WHERE  startTime  >= '{start_time}'  and  startTime  < '{end_time}'  and `status` like  '%完成%'
union 
SELECT  workorderNumber,type,workDescription,startTime,endTime,dutyPerson,groupName,mileageStatistics from
mj_work_order_2_other WHERE   startTime  >= '{start_time}'  and  startTime  < '{end_time}'  and `status` like  '%完成%'
union
SELECT  workorderNumber,type,'巡检' type2,startTime,endTime,dutyPerson,groupName,mileageStatistics from
mj_work_order_2_patrol WHERE   startTime  >= '{start_time}'  and  startTime  < '{end_time}'  and `status` like  '%完成%'
union 
SELECT  workorderNumber,type,'维修' type2,startTime,endTime,dutyPerson,groupName,mileageStatistics from
mj_work_order_2_repair WHERE   startTime  >= '{start_time}'  and  startTime  < '{end_time}'  and `status` like  '%完成%'
union 
SELECT  workorderNumber,type,saveType,startTime,endTime,dutyPerson,groupName,mileageStatistics from
mj_work_order_2_save WHERE  startTime  >= '{start_time}'  and  startTime  < '{end_time}'  and `status` like  '%完成%')  b
GROUP BY type,type2  order  by type,num desc
"""
time = connect_mysql(sql_time)
mile = connect_mysql(sql_mile)
ws3 = wb['各类工单时长里程']
a1 = time['type']
a2 = time['type2']
a3 = time['num']
a4 = mile['type']
a5 = mile['type2']
a6 = mile['num']
for i,  j, k, l, m, n, o in zip(range(2, len(time) + 2), a1, a2, a3, a4, a5, a6):
    ws3[f'J{i}'] = j
    ws3[f'K{i}'] = k
    ws3[f'L{i}'] = l
    ws3[f'M{i}'] = m
    ws3[f'N{i}'] = n
    ws3[f'O{i}'] = o
    wb.save(filename='周报数据.xlsx')
print('各类工单时长里程完成..........')

# 年度里程
sql_year_time = f"""
    SELECT DATE(startTime),WEEK(startTime,1) time,sum(UNIX_TIMESTAMP(endTime)-UNIX_TIMESTAMP(startTime))/3600  num  FROM 
    mj_work_order_2 WHERE startTime >= '2022-01-01'   and startTime < '{end_time}'  and `status` like '%完成%' 
    GROUP BY time  order by time
    """
sql_year_mile = f"""
    SELECT DATE(startTime),WEEK(startTime,1) time,sum(mileageStatistics) num  FROM mj_work_order_2 
WHERE startTime >= '2022-01-01'   and startTime < '{end_time}'  and `status` like '%完成%'  and   mileageStatistics >= 0
GROUP BY time ORDER BY time 
    """
year_time = connect_mysql(sql_year_time)
year_mile = connect_mysql(sql_year_mile)
ws4 = wb['总时长里程']
a1 = year_time['time']
a2 = year_time['num']
a3 = year_mile['num']
for i, c, d, e in zip(range(2, len(year_time)+2), a1, a2, a3):
    ws4[f'C{i}'] = c
    ws4[f'D{i}'] = d
    ws4[f'E{i}'] = e
    wb.save(filename='周报数据.xlsx')
print('总时长里程完成...........')

# 城市工单总量
sql_all_amount = f"""
    SELECT  DATE(startTime),WEEK(startTime,1)  time,COUNT(*) num  from mj_work_order_2  WHERE  startTime >= '2022-01-01'  
    and startTime < '{end_time}' and `status` like '%完成%'  GROUP BY time ORDER BY time
    """
all_amount = connect_mysql(sql_all_amount)
ws5 = wb['城市工单总量']
a1 = all_amount['time']
a2 = all_amount['num']
for i, a, b in zip(range(2, len(all_amount) + 2), a1, a2):
    ws5[f'A{i}'] = a
    ws5[f'B{i}'] = b
wb.save(filename='周报数据.xlsx')
print('城市工单总量完成...............')


