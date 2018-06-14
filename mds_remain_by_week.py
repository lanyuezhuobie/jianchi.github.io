#!/usr/bin/python
#-*-coding:utf-8 -*-


import subprocess
import traceback

import sys
import datetime
import time

day0=sys.argv[1]
day0=datetime.datetime.strptime(day0,'%Y%m%d')
day1=day0.strftime("%Y%m%d")
YearWeek = datetime.datetime.strptime(day1, '%Y%m%d').isocalendar()
the_partition=str(YearWeek[0])+str(YearWeek[1])
the_year=str(YearWeek[0])

sql="""
use vcomicbi;

insert  overwrite table mds_remain_by_week_table  partition (dt={the_partition}) 
select t.platform,t.app_version,t.source_id,case when length(t.week_of_year)=1 then concat({the_year},0,t.week_of_year) else concat({the_year},t.week_of_year) end,t1.new_num,t.remain_num,t.week_distance from 
(select   
xinzeng.platform,  
xinzeng.app_version,  
xinzeng.source_id,    
xinzeng.week_of_year,
count(case when cunliu.device_id is not null then 1 else null end) as remain_num,  
weekofyear(from_unixtime(unix_timestamp('{day1}','yyyymmdd'),'yyyy-mm-dd'))-xinzeng.week_of_year as week_distance  
from  
(  
    select device_id,platform,app_version,source_id,weekofyear(from_unixtime(unix_timestamp(dt,'yyyymmdd'),'yyyy-mm-dd')) as week_of_year from dw_day_new_increament where dt<{day1}
) xinzeng  
left outer join   
(  
    select
	distinct
 device_id,
 platform,
 app_version,
 source_id,
 weekofyear(from_unixtime(unix_timestamp(dt,'yyyymmdd'),'yyyy-mm-dd')) as week_of_year
    from ods_user_page_log   
    where dt<={day1} and weekofyear(from_unixtime(unix_timestamp(dt,'yyyymmdd'),'yyyy-mm-dd'))=weekofyear(from_unixtime(unix_timestamp('{day1}','yyyymmdd'),'yyyy-mm-dd'))
) cunliu on  
(  
    xinzeng.device_id=cunliu.device_id and
    xinzeng.platform=cunliu.platform and  
    xinzeng.app_version=cunliu.app_version and  
    xinzeng.source_id=cunliu.source_id)  
   
group by   
xinzeng.platform,xinzeng.app_version,xinzeng.source_id,xinzeng.week_of_year,  
weekofyear(from_unixtime(unix_timestamp('{day1}','yyyymmdd'),'yyyy-mm-dd'))-xinzeng.week_of_year ) t 
left outer join  
(  
    select platform,app_version,source_id,weekofyear(from_unixtime(unix_timestamp(dt,'yyyymmdd'),'yyyy-mm-dd')) as week_of_year,count(device_id) as new_num from dw_day_new_increament where dt<={day1} group by platform,app_version,source_id,weekofyear(from_unixtime(unix_timestamp(dt,'yyyymmdd'),'yyyy-mm-dd'))
) t1  on  
(  
    t1.platform=t.platform and  
    t1.app_version=t.app_version and  
    t1.source_id=t.source_id and
    t1.week_of_year=t.week_of_year) 
where week_distance>0   union 
select t.platform,t.app_version,'all',concat({the_year},t.week_of_year),t1.new_num,t.remain_num,t.week_distance from 
(select    
xinzeng.platform,
xinzeng.app_version,
xinzeng.week_of_year,
count(case when cunliu.device_id is not null then 1 else null end) as remain_num,  
weekofyear(from_unixtime(unix_timestamp('{day1}','yyyymmdd'),'yyyy-mm-dd'))-xinzeng.week_of_year as week_distance  
from  
(  
    select device_id,platform,app_version,weekofyear(from_unixtime(unix_timestamp(dt,'yyyymmdd'),'yyyy-mm-dd')) as week_of_year from dw_day_new_increament where dt<{day1}
) xinzeng  
left outer join   
(  
    select distinct
 device_id,
 platform,
 app_version,
 weekofyear(from_unixtime(unix_timestamp(dt,'yyyymmdd'),'yyyy-mm-dd')) as week_of_year
    from ods_user_page_log   
    where dt<={day1} and weekofyear(from_unixtime(unix_timestamp(dt,'yyyymmdd'),'yyyy-mm-dd'))=weekofyear(from_unixtime(unix_timestamp('{day1}','yyyymmdd'),'yyyy-mm-dd')) 
) cunliu on xinzeng.device_id=cunliu.device_id and 
xinzeng.platform=cunliu.platform and 
xinzeng.app_version=cunliu.app_version
   
group by xinzeng.platform,xinzeng.app_version,xinzeng.week_of_year,weekofyear(from_unixtime(unix_timestamp('{day1}','yyyymmdd'),'yyyy-mm-dd'))-xinzeng.week_of_year  
) t 
left outer join  
(  
    select platform,app_version,weekofyear(from_unixtime(unix_timestamp(dt,'yyyymmdd'),'yyyy-mm-dd')) as week_of_year,count(device_id) as new_num from dw_day_new_increament where dt<={day1} group by platform,app_version,weekofyear(from_unixtime(unix_timestamp(dt,'yyyymmdd'),'yyyy-mm-dd'))
) t1 on t1.week_of_year=t.week_of_year and t.platform=t1.platform and t.app_version=t1.app_version
where week_distance>0   union
select t.platform,'all',t.source_id,concat({the_year},t.week_of_year),t1.new_num,t.remain_num,t.week_distance from 
(select    
xinzeng.platform,
xinzeng.source_id,
xinzeng.week_of_year,
count(case when cunliu.device_id is not null then 1 else null end) as remain_num,  
weekofyear(from_unixtime(unix_timestamp('{day1}','yyyymmdd'),'yyyy-mm-dd'))-xinzeng.week_of_year as week_distance  
from  
(  
    select device_id,platform,source_id,weekofyear(from_unixtime(unix_timestamp(dt,'yyyymmdd'),'yyyy-mm-dd')) as week_of_year from dw_day_new_increament where dt<{day1}
) xinzeng  
left outer join   
(  
    select distinct
 device_id,
 platform,
 source_id,
 weekofyear(from_unixtime(unix_timestamp(dt,'yyyymmdd'),'yyyy-mm-dd')) as week_of_year
    from ods_user_page_log   
    where dt<={day1} and weekofyear(from_unixtime(unix_timestamp(dt,'yyyymmdd'),'yyyy-mm-dd'))=weekofyear(from_unixtime(unix_timestamp('{day1}','yyyymmdd'),'yyyy-mm-dd')) 
) cunliu on xinzeng.device_id=cunliu.device_id and 
xinzeng.platform=cunliu.platform and 
xinzeng.source_id=cunliu.source_id
   
group by xinzeng.platform,xinzeng.source_id,xinzeng.week_of_year,weekofyear(from_unixtime(unix_timestamp('{day1}','yyyymmdd'),'yyyy-mm-dd'))-xinzeng.week_of_year  
) t 
left outer join  
(  
    select platform,source_id,weekofyear(from_unixtime(unix_timestamp(dt,'yyyymmdd'),'yyyy-mm-dd')) as week_of_year,count(device_id) as new_num from dw_day_new_increament where dt<={day1} group by platform,source_id,weekofyear(from_unixtime(unix_timestamp(dt,'yyyymmdd'),'yyyy-mm-dd'))
) t1 on t1.week_of_year=t.week_of_year and t1.platform=t.platform and t1.source_id=t.source_id
where week_distance>0   union
select 'all',t.app_version,t.source_id,concat({the_year},t.week_of_year),t1.new_num,t.remain_num,t.week_distance from 
(select    
xinzeng.app_version,
xinzeng.source_id,
xinzeng.week_of_year,
count(case when cunliu.device_id is not null then 1 else null end) as remain_num,  
weekofyear(from_unixtime(unix_timestamp('{day1}','yyyymmdd'),'yyyy-mm-dd'))-xinzeng.week_of_year as week_distance  
from  
(  
    select device_id,app_version,source_id,weekofyear(from_unixtime(unix_timestamp(dt,'yyyymmdd'),'yyyy-mm-dd')) as week_of_year from dw_day_new_increament where dt<{day1}
) xinzeng  
left outer join   
(  
    select distinct
 device_id,
 app_version,
 source_id,
 weekofyear(from_unixtime(unix_timestamp(dt,'yyyymmdd'),'yyyy-mm-dd')) as week_of_year
    from ods_user_page_log   
    where dt<={day1} and weekofyear(from_unixtime(unix_timestamp(dt,'yyyymmdd'),'yyyy-mm-dd'))=weekofyear(from_unixtime(unix_timestamp('{day1}','yyyymmdd'),'yyyy-mm-dd')) 
) cunliu on xinzeng.device_id=cunliu.device_id and 
xinzeng.app_version=cunliu.app_version and 
xinzeng.source_id=cunliu.source_id
   
group by xinzeng.app_version,xinzeng.source_id,xinzeng.week_of_year,weekofyear(from_unixtime(unix_timestamp('{day1}','yyyymmdd'),'yyyy-mm-dd'))-xinzeng.week_of_year  
) t 
left outer join  
(  
    select app_version,source_id,weekofyear(from_unixtime(unix_timestamp(dt,'yyyymmdd'),'yyyy-mm-dd')) as week_of_year,count(device_id) as new_num from dw_day_new_increament where dt<={day1} group by app_version,source_id,weekofyear(from_unixtime(unix_timestamp(dt,'yyyymmdd'),'yyyy-mm-dd'))
) t1 on t1.week_of_year=t.week_of_year and t1.app_version=t.app_version and t1.source_id=t.source_id
where week_distance>0   union
select 'all','all',t.source_id,concat({the_year},t.week_of_year),t1.new_num,t.remain_num,t.week_distance from 
(select    
xinzeng.source_id,
xinzeng.week_of_year,
count(case when cunliu.device_id is not null then 1 else null end) as remain_num,  
weekofyear(from_unixtime(unix_timestamp('{day1}','yyyymmdd'),'yyyy-mm-dd'))-xinzeng.week_of_year as week_distance  
from  
(  
    select device_id,source_id,weekofyear(from_unixtime(unix_timestamp(dt,'yyyymmdd'),'yyyy-mm-dd')) as week_of_year from dw_day_new_increament where dt<{day1}
) xinzeng  
left outer join   
(  
    select distinct
 device_id,
 source_id,
 weekofyear(from_unixtime(unix_timestamp(dt,'yyyymmdd'),'yyyy-mm-dd')) as week_of_year
    from ods_user_page_log   
    where dt<={day1} and weekofyear(from_unixtime(unix_timestamp(dt,'yyyymmdd'),'yyyy-mm-dd'))=weekofyear(from_unixtime(unix_timestamp('{day1}','yyyymmdd'),'yyyy-mm-dd')) 
) cunliu on xinzeng.device_id=cunliu.device_id and 
xinzeng.source_id=cunliu.source_id 
   
group by xinzeng.source_id,xinzeng.week_of_year,weekofyear(from_unixtime(unix_timestamp('{day1}','yyyymmdd'),'yyyy-mm-dd'))-xinzeng.week_of_year  
) t 
left outer join  
(  
    select source_id,weekofyear(from_unixtime(unix_timestamp(dt,'yyyymmdd'),'yyyy-mm-dd')) as week_of_year,count(device_id) as new_num from dw_day_new_increament where dt<={day1} group by source_id,weekofyear(from_unixtime(unix_timestamp(dt,'yyyymmdd'),'yyyy-mm-dd'))
) t1 on t1.source_id=t.source_id and t1.week_of_year=t.week_of_year
where week_distance>0   union
select 'all',t.app_version,'all',concat({the_year},t.week_of_year),t1.new_num,t.remain_num,t.week_distance from 
(select    
xinzeng.app_version,
xinzeng.week_of_year,
count(case when cunliu.device_id is not null then 1 else null end) as remain_num,  
weekofyear(from_unixtime(unix_timestamp('{day1}','yyyymmdd'),'yyyy-mm-dd'))-xinzeng.week_of_year as week_distance  
from  
(  
    select device_id,app_version,weekofyear(from_unixtime(unix_timestamp(dt,'yyyymmdd'),'yyyy-mm-dd')) as week_of_year from dw_day_new_increament where dt<{day1}
) xinzeng  
left outer join   
(  
    select distinct
 device_id,
 app_version,
 weekofyear(from_unixtime(unix_timestamp(dt,'yyyymmdd'),'yyyy-mm-dd')) as week_of_year
    from ods_user_page_log   
    where dt<={day1} and weekofyear(from_unixtime(unix_timestamp(dt,'yyyymmdd'),'yyyy-mm-dd'))=weekofyear(from_unixtime(unix_timestamp('{day1}','yyyymmdd'),'yyyy-mm-dd')) 
) cunliu on xinzeng.device_id=cunliu.device_id and 
xinzeng.app_version=cunliu.app_version 
   
group by xinzeng.app_version,xinzeng.week_of_year,weekofyear(from_unixtime(unix_timestamp('{day1}','yyyymmdd'),'yyyy-mm-dd'))-xinzeng.week_of_year  
) t 
left outer join  
(  
    select app_version,weekofyear(from_unixtime(unix_timestamp(dt,'yyyymmdd'),'yyyy-mm-dd')) as week_of_year,count(device_id) as new_num from dw_day_new_increament where dt<={day1} group by app_version,weekofyear(from_unixtime(unix_timestamp(dt,'yyyymmdd'),'yyyy-mm-dd'))
) t1 on t.app_version=t1.app_version and t1.week_of_year=t.week_of_year
where week_distance>0   union
select t.platform,'all','all',concat({the_year},t.week_of_year),t1.new_num,t.remain_num,t.week_distance from 
(select    
xinzeng.platform,
xinzeng.week_of_year,
count(case when cunliu.device_id is not null then 1 else null end) as remain_num,  
weekofyear(from_unixtime(unix_timestamp('{day1}','yyyymmdd'),'yyyy-mm-dd'))-xinzeng.week_of_year as week_distance  
from  
(  
    select device_id,platform,weekofyear(from_unixtime(unix_timestamp(dt,'yyyymmdd'),'yyyy-mm-dd')) as week_of_year from dw_day_new_increament where dt<{day1}
) xinzeng  
left outer join   
(  
    select distinct
 device_id,
 platform,
 weekofyear(from_unixtime(unix_timestamp(dt,'yyyymmdd'),'yyyy-mm-dd')) as week_of_year
    from ods_user_page_log   
    where dt<={day1} and weekofyear(from_unixtime(unix_timestamp(dt,'yyyymmdd'),'yyyy-mm-dd'))=weekofyear(from_unixtime(unix_timestamp('{day1}','yyyymmdd'),'yyyy-mm-dd')) 
) cunliu on xinzeng.device_id=cunliu.device_id and 
xinzeng.platform=cunliu.platform 
   
group by xinzeng.platform,xinzeng.week_of_year,weekofyear(from_unixtime(unix_timestamp('{day1}','yyyymmdd'),'yyyy-mm-dd'))-xinzeng.week_of_year  
) t 
left outer join  
(  
    select platform,weekofyear(from_unixtime(unix_timestamp(dt,'yyyymmdd'),'yyyy-mm-dd')) as week_of_year,count(device_id) as new_num from dw_day_new_increament where dt<={day1} group by platform,weekofyear(from_unixtime(unix_timestamp(dt,'yyyymmdd'),'yyyy-mm-dd'))
) t1 on t1.platform=t.platform and t1.week_of_year=t.week_of_year
where week_distance>0   union
select 'all','all','all',concat({the_year},t.week_of_year),t1.new_num,t.remain_num,t.week_distance from 
(select    
xinzeng.week_of_year,
count(case when cunliu.device_id is not null then 1 else null end) as remain_num,  
weekofyear(from_unixtime(unix_timestamp('{day1}','yyyymmdd'),'yyyy-mm-dd'))-xinzeng.week_of_year as week_distance  
from  
(  
    select device_id,weekofyear(from_unixtime(unix_timestamp(dt,'yyyymmdd'),'yyyy-mm-dd')) as week_of_year from dw_day_new_increament where dt<{day1}
) xinzeng  
left outer join   
(  
    select distinct
 device_id,
 weekofyear(from_unixtime(unix_timestamp(dt,'yyyymmdd'),'yyyy-mm-dd')) as week_of_year
    from ods_user_page_log   
    where dt<={day1} and weekofyear(from_unixtime(unix_timestamp(dt,'yyyymmdd'),'yyyy-mm-dd'))=weekofyear(from_unixtime(unix_timestamp('{day1}','yyyymmdd'),'yyyy-mm-dd')) 
) cunliu on xinzeng.device_id=cunliu.device_id  
   
group by xinzeng.week_of_year,weekofyear(from_unixtime(unix_timestamp('{day1}','yyyymmdd'),'yyyy-mm-dd'))-xinzeng.week_of_year  
) t 
left outer join  
(  
    select weekofyear(from_unixtime(unix_timestamp(dt,'yyyymmdd'),'yyyy-mm-dd')) as week_of_year,count(device_id) as new_num from dw_day_new_increament where dt<={day1} group by weekofyear(from_unixtime(unix_timestamp(dt,'yyyymmdd'),'yyyy-mm-dd'))
) t1 on t1.week_of_year=t.week_of_year 

""".format(day1=day1,the_partition=the_partition,the_year=the_year) 

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