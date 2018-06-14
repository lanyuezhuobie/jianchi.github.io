#!/usr/bin/python
#-*-coding:utf-8 -*-


import subprocess
import traceback

import sys
import datetime

day0=sys.argv[1]
day0=datetime.datetime.strptime(day0,'%Y%m%d')
day1=day0.strftime("%Y%m%d")
premonth=(day0-datetime.timedelta(days=30)).strftime("%Y%m%d")

sql="""
use vcomicbi;

insert overwrite table mds_remain_by_day_table partition (dt={day1}) 
select platform,
 app_version,
 source_id,
 dt,
 new_num,
 remain_num,
 day_distance
 from (
select t.platform,  
t.app_version,  
t.source_id,    
t.dt,
t1.new_num,
t.remain_num,
t.day_distance
 from   
(select  
xinzeng.platform,  
xinzeng.app_version,  
xinzeng.source_id,    
xinzeng.dt, 
count(case when cunliu.device_id is not null then 1 else null end) as remain_num,
datediff(  
    from_unixtime(unix_timestamp(cast({day1} as string),'yyyyMMdd')),  
    from_unixtime(unix_timestamp(cast(xinzeng.dt as string),'yyyyMMdd'))  
) as day_distance   
from  
(  
    select device_id,platform,app_version,source_id,dt from dw_day_new_increament where dt<{day1} and dt>={premonth}
) xinzeng  
left outer join   
(  
    select distinct 
 device_id,
 platform,
 app_version,
 source_id,
 '{day1}' as dt
    from ods_user_page_log   
    where dt={day1} 
) cunliu on  
    xinzeng.device_id=cunliu.device_id
group by   
xinzeng.platform,xinzeng.app_version,xinzeng.source_id,xinzeng.dt,  
datediff(  
    from_unixtime(unix_timestamp(cast({day1} as string),'yyyyMMdd')),  
    from_unixtime(unix_timestamp(cast(xinzeng.dt as string),'yyyyMMdd'))  
) ) t
left outer join  
(  
    select platform,app_version,source_id,dt,count(device_id) as new_num from dw_day_new_increament where dt<{day1} group by platform,app_version,source_id,dt
) t1  on  
(  
    t1.platform=t.platform and  
    t1.app_version=t.app_version and  
    t1.source_id=t.source_id and
    t1.dt=t.dt) 
 union
  select 'all','all','all',dt,new_num1,remain_num,day_distance from (
select   
t.dt,
xinzeng1.new_num1,
t.remain_num,
t.day_distance
 from   
(select      
xinzeng.dt, 
count(case when cunliu.device_id is not null then 1 else null end) as remain_num,  
datediff(  
    from_unixtime(unix_timestamp(cast({day1} as string),'yyyyMMdd')),  
    from_unixtime(unix_timestamp(cast(xinzeng.dt as string),'yyyyMMdd'))  
) as day_distance   
from  
(  
    select device_id,dt from dw_day_new_increament where dt<{day1} and dt>={premonth}
) xinzeng  
left outer join   
(  
    select distinct 
 device_id,
 '{day1}' as dt   from ods_user_page_log   
    where dt={day1} 
) cunliu on 
    xinzeng.device_id=cunliu.device_id
group by   
xinzeng.dt,  
datediff(  
    from_unixtime(unix_timestamp(cast({day1} as string),'yyyyMMdd')),  
    from_unixtime(unix_timestamp(cast(xinzeng.dt as string),'yyyyMMdd'))  
) ) t
left outer join  
(  
    select dt,count(device_id) as new_num1 from dw_day_new_increament where dt<{day1} group by dt
) xinzeng1  on  
(  
    xinzeng1.dt=t.dt) ) t1
 union
 select 'all',app_version,source_id,dt,new_num1,remain_num,day_distance from (
select   
t.app_version,
t.source_id,
t.dt,
xinzeng1.new_num1,
t.remain_num,
t.day_distance
 from   
(select      
xinzeng.app_version,
xinzeng.source_id,
xinzeng.dt, 
count(case when cunliu.device_id is not null then 1 else null end) as remain_num,  
datediff(  
    from_unixtime(unix_timestamp(cast({day1} as string),'yyyyMMdd')),  
    from_unixtime(unix_timestamp(cast(xinzeng.dt as string),'yyyyMMdd'))  
) as day_distance   
from  
(  
    select device_id,app_version,source_id,dt from dw_day_new_increament where dt<{day1} and dt>={premonth}
) xinzeng  
left outer join   
(  
    select distinct 
 device_id,
 app_version,
 source_id,
 '{day1}' as dt   from ods_user_page_log   
    where dt={day1} 
) cunliu on 
    xinzeng.device_id=cunliu.device_id 
group by   
xinzeng.app_version,
xinzeng.source_id,
xinzeng.dt,  
datediff(  
    from_unixtime(unix_timestamp(cast({day1} as string),'yyyyMMdd')),  
    from_unixtime(unix_timestamp(cast(xinzeng.dt as string),'yyyyMMdd'))  
) ) t
left outer join  
(  
    select app_version,source_id,dt,count(device_id) as new_num1 from dw_day_new_increament where dt<{day1} group by app_version,source_id,dt
) xinzeng1  on  
(  
 xinzeng1.app_version=t.app_version and
 xinzeng1.source_id=t.source_id and
    xinzeng1.dt=t.dt) ) t1
 union
 select platform,'all',source_id,dt,new_num1,remain_num,day_distance from (
select   
t.platform,
t.source_id,
t.dt,
xinzeng1.new_num1,
t.remain_num,
t.day_distance
 from   
(select      
xinzeng.platform,
xinzeng.source_id,
xinzeng.dt, 
count(case when cunliu.device_id is not null then 1 else null end) as remain_num,  
datediff(  
    from_unixtime(unix_timestamp(cast({day1} as string),'yyyyMMdd')),  
    from_unixtime(unix_timestamp(cast(xinzeng.dt as string),'yyyyMMdd'))  
) as day_distance   
from  
(  
    select device_id,platform,source_id,dt from dw_day_new_increament where dt<{day1} and dt>={premonth}
) xinzeng  
left outer join   
(  
    select distinct 
 device_id,
 platform,
 source_id,
 '{day1}' as dt   from ods_user_page_log   
    where dt={day1} 
) cunliu on 
    xinzeng.device_id=cunliu.device_id 
group by   
xinzeng.platform,
xinzeng.source_id,
xinzeng.dt,  
datediff(  
    from_unixtime(unix_timestamp(cast({day1} as string),'yyyyMMdd')),  
    from_unixtime(unix_timestamp(cast(xinzeng.dt as string),'yyyyMMdd'))  
) ) t
left outer join  
(  
    select platform,source_id,dt,count(device_id) as new_num1 from dw_day_new_increament where dt<{day1} group by platform,source_id,dt
) xinzeng1  on  
(  
 xinzeng1.platform=t.platform and
 xinzeng1.source_id=t.source_id and
    xinzeng1.dt=t.dt) ) t1
 union 
  select platform,app_version,'all',dt,new_num1,remain_num,day_distance from (
select   
t.platform,
t.app_version,
t.dt,
xinzeng1.new_num1,
t.remain_num,
t.day_distance
 from   
(select      
xinzeng.platform,
xinzeng.app_version,
xinzeng.dt, 
count(case when cunliu.device_id is not null then 1 else null end) as remain_num,  
datediff(  
    from_unixtime(unix_timestamp(cast({day1} as string),'yyyyMMdd')),  
    from_unixtime(unix_timestamp(cast(xinzeng.dt as string),'yyyyMMdd'))  
) as day_distance   
from  
(  
    select device_id,platform,app_version,dt from dw_day_new_increament where dt<{day1} and dt>={premonth}
) xinzeng  
left outer join   
(  
    select distinct 
 device_id,
 platform,
 app_version,
 '{day1}' as dt   from ods_user_page_log   
    where dt={day1} 
) cunliu on 
    xinzeng.device_id=cunliu.device_id 
group by   
xinzeng.platform,
xinzeng.app_version,
xinzeng.dt,  
datediff(  
    from_unixtime(unix_timestamp(cast({day1} as string),'yyyyMMdd')),  
    from_unixtime(unix_timestamp(cast(xinzeng.dt as string),'yyyyMMdd'))  
) ) t
left outer join  
(  
    select platform,app_version,dt,count(device_id) as new_num1 from dw_day_new_increament where dt<{day1} group by platform,app_version,dt
) xinzeng1  on  
(  
 xinzeng1.platform=t.platform and
 xinzeng1.app_version=t.app_version and
    xinzeng1.dt=t.dt) ) t1
 union 
  select platform,'all','all',dt,new_num1,remain_num,day_distance from (
select   
t.platform,

t.dt,
xinzeng1.new_num1,
t.remain_num,
t.day_distance
 from   
(select      
xinzeng.platform,
xinzeng.dt, 
count(case when cunliu.device_id is not null then 1 else null end) as remain_num,  
datediff(  
    from_unixtime(unix_timestamp(cast({day1} as string),'yyyyMMdd')),  
    from_unixtime(unix_timestamp(cast(xinzeng.dt as string),'yyyyMMdd'))  
) as day_distance   
from  
(  
    select device_id,platform,dt from dw_day_new_increament where dt<{day1} and dt>={premonth}
) xinzeng  
left outer join   
(  
    select distinct 
 device_id,
 platform,
 '{day1}' as dt   from ods_user_page_log   
    where dt={day1} 
) cunliu on 
    xinzeng.device_id=cunliu.device_id 
group by   
xinzeng.platform,
xinzeng.dt,  
datediff(  
    from_unixtime(unix_timestamp(cast({day1} as string),'yyyyMMdd')),  
    from_unixtime(unix_timestamp(cast(xinzeng.dt as string),'yyyyMMdd'))  
) ) t
left outer join  
(  
    select platform,dt,count(device_id) as new_num1 from dw_day_new_increament where dt<{day1} group by platform,dt
) xinzeng1  on  
(  
 xinzeng1.platform=t.platform and
    xinzeng1.dt=t.dt) ) t1
 union 
  select 'all',app_version,'all',dt,new_num1,remain_num,day_distance from (
select   
t.app_version,

t.dt,
xinzeng1.new_num1,
t.remain_num,
t.day_distance
 from   
(select      
xinzeng.app_version,
xinzeng.dt, 
count(case when cunliu.device_id is not null then 1 else null end) as remain_num,  
datediff(  
    from_unixtime(unix_timestamp(cast({day1} as string),'yyyyMMdd')),  
    from_unixtime(unix_timestamp(cast(xinzeng.dt as string),'yyyyMMdd'))  
) as day_distance   
from  
(  
    select device_id,app_version,dt from dw_day_new_increament where dt<{day1} and dt>={premonth}
) xinzeng  
left outer join   
(  
    select distinct 
 device_id,
 app_version,
 '{day1}' as dt   from ods_user_page_log   
    where dt={day1} 
) cunliu on 
    xinzeng.device_id=cunliu.device_id 
group by   
xinzeng.app_version,
xinzeng.dt,  
datediff(  
    from_unixtime(unix_timestamp(cast({day1} as string),'yyyyMMdd')),  
    from_unixtime(unix_timestamp(cast(xinzeng.dt as string),'yyyyMMdd'))  
) ) t
left outer join  
(  
    select app_version,dt,count(device_id) as new_num1 from dw_day_new_increament where dt<{day1} group by app_version,dt
) xinzeng1  on  
(  
 xinzeng1.app_version=t.app_version and
    xinzeng1.dt=t.dt) ) t1
 union 
  select 'all','all',source_id,dt,new_num1,remain_num,day_distance from (
select   
t.source_id,
t.dt,
xinzeng1.new_num1,
t.remain_num,
t.day_distance
 from   
(select      
xinzeng.source_id,
xinzeng.dt, 
count(case when cunliu.device_id is not null then 1 else null end) as remain_num,  
datediff(  
    from_unixtime(unix_timestamp(cast({day1} as string),'yyyyMMdd')),  
    from_unixtime(unix_timestamp(cast(xinzeng.dt as string),'yyyyMMdd'))  
) as day_distance   
from  
(  
    select device_id,source_id,dt from dw_day_new_increament where dt<{day1} and dt>={premonth}
) xinzeng  
left outer join   
(  
    select distinct 
 device_id,
 source_id,
 '{day1}' as dt   from ods_user_page_log   
    where dt={day1} 
) cunliu on
    xinzeng.device_id=cunliu.device_id
group by   
xinzeng.source_id,
xinzeng.dt,  
datediff(  
    from_unixtime(unix_timestamp(cast({day1} as string),'yyyyMMdd')),  
    from_unixtime(unix_timestamp(cast(xinzeng.dt as string),'yyyyMMdd'))  
) ) t
left outer join  
(  
    select source_id,dt,count(device_id) as new_num1 from dw_day_new_increament where dt<{day1} group by source_id,dt
) xinzeng1  on  
(  
 xinzeng1.source_id=t.source_id and
    xinzeng1.dt=t.dt) ) t1 ) t cluster by dt;
  
""".format(day1=day1,premonth=premonth)

cmd = 'hive -e """'+sql.replace('"', "\'")+'"""' 
print cmd
try:
    p = subprocess.Popen(cmd, shell=True, stdout=subprocess.PIPE)
    while True:
        buff = p.stdout.readline()
        print buff
        if buff == '' :
            break

except Exception,re:
    print "message is:%s" %(str(re))
    traceback.print_exc();