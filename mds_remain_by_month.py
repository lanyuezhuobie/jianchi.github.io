#!/usr/bin/python
#-*-coding:utf-8 -*-


import subprocess
import traceback

import sys
import datetime

day0=sys.argv[1]
day0=datetime.datetime.strptime(day0,'%Y%m%d')
day1=day0.strftime("%Y%m%d")
the_month=day0.month
if len(str(the_month))==1:
	the_month='0'+str(the_month)
the_year=day0.year
the_partition=str(the_year)+str(the_month)

sql="""
use vcomicbi;

insert overwrite table mds_remain_by_month_table  partition (dt={the_partition}) 
select coalesce(t.platform,null),coalesce(t.app_version,null),coalesce(t.source_id,null),case when length(t.month)=1 then concat({the_year},0,t.month) else concat({the_year},t.month) end,t1.new_num,t.remain_num,t.month_distance from 
(select
xinzeng.platform,  
xinzeng.app_version,  
xinzeng.source_id,    
xinzeng.month,  
count(case when cunliu.device_id is not null then 1 else null end) as remain_num,  
month(from_unixtime(unix_timestamp('{day1}','yyyymmdd'),'yyyy-mm-dd'))-xinzeng.month as month_distance 
from  
(  
    select device_id,platform,app_version,source_id,month(from_unixtime(unix_timestamp(dt,'yyyymmdd'),'yyyy-mm-dd')) as month from dw_day_new_increament where dt<{day1}
) xinzeng  
left outer join   
(  
    select
 distinct device_id,
 platform,
 app_version,
 source_id,
 month(from_unixtime(unix_timestamp(dt,'yyyymmdd'),'yyyy-mm-dd')) as month
    from ods_user_page_log   
    where dt<={day1} and month(from_unixtime(unix_timestamp(dt,'yyyymmdd'),'yyyy-mm-dd'))=month(from_unixtime(unix_timestamp('{day1}','yyyymmdd'),'yyyy-mm-dd'))
) cunliu on  
(  
    xinzeng.device_id=cunliu.device_id and
    xinzeng.platform=cunliu.platform and  
    xinzeng.app_version=cunliu.app_version and  
    xinzeng.source_id=cunliu.source_id)  
group by   
xinzeng.platform,xinzeng.app_version,xinzeng.source_id,xinzeng.month,  
month(from_unixtime(unix_timestamp('{day1}','yyyymmdd'),'yyyy-mm-dd'))-xinzeng.month ) t 
left outer join 
(  
    select platform,app_version,source_id,month(from_unixtime(unix_timestamp(dt,'yyyymmdd'),'yyyy-mm-dd')) as month,count(device_id) as new_num from dw_day_new_increament where dt<={day1} group by platform,app_version,source_id,month(from_unixtime(unix_timestamp(dt,'yyyymmdd'),'yyyy-mm-dd'))
) t1  on  
(  
    t1.platform=t.platform and  
    t1.app_version=t.app_version and  
    t1.source_id=t.source_id and
    t1.month=t.month) 
 where t.month_distance>0   
union 
select coalesce(t.platform,null),coalesce(t.app_version,null),'all',case when length(t.month)=1 then concat({the_year},0,t.month) else concat({the_year},t.month) end,t1.new_num,t.remain_num,t.month_distance from 
(select    
xinzeng.platform,
xinzeng.app_version,
xinzeng.month,
count(case when cunliu.device_id is not null then 1 else null end) as remain_num,  
month(from_unixtime(unix_timestamp('{day1}','yyyymmdd'),'yyyy-mm-dd'))-xinzeng.month as month_distance  
from  
(  
    select device_id,platform,app_version,month(from_unixtime(unix_timestamp(dt,'yyyymmdd'),'yyyy-mm-dd')) as month from dw_day_new_increament where dt<{day1}
) xinzeng  
left outer join   
(  
    select
 distinct device_id,
 platform,
 app_version,
 month(from_unixtime(unix_timestamp(dt,'yyyymmdd'),'yyyy-mm-dd')) as month
    from ods_user_page_log   
    where dt<={day1} and month(from_unixtime(unix_timestamp(dt,'yyyymmdd'),'yyyy-mm-dd'))=month(from_unixtime(unix_timestamp('{day1}','yyyymmdd'),'yyyy-mm-dd')) 
) cunliu on xinzeng.device_id=cunliu.device_id and 
xinzeng.platform=cunliu.platform and 
xinzeng.app_version=cunliu.app_version
group by xinzeng.platform,xinzeng.app_version,xinzeng.month,month(from_unixtime(unix_timestamp('{day1}','yyyymmdd'),'yyyy-mm-dd'))-xinzeng.month  
) t 
left outer join  
(  
    select platform,app_version,month(from_unixtime(unix_timestamp(dt,'yyyymmdd'),'yyyy-mm-dd')) as month,count(device_id) as new_num from dw_day_new_increament where dt<={day1} group by platform,app_version,month(from_unixtime(unix_timestamp(dt,'yyyymmdd'),'yyyy-mm-dd'))
) t1 on t1.month=t.month and t.platform=t1.platform and t.app_version=t1.app_version
where t.month_distance>0   
union
select coalesce(t.platform,null),'all',coalesce(t.source_id,null),case when length(t.month)=1 then concat({the_year},0,t.month) else concat({the_year},t.month) end,t1.new_num,t.remain_num,t.month_distance from 
(select    
xinzeng.platform,
xinzeng.source_id,
xinzeng.month,
count(case when cunliu.device_id is not null then 1 else null end) as remain_num,  
month(from_unixtime(unix_timestamp('{day1}','yyyymmdd'),'yyyy-mm-dd'))-xinzeng.month as month_distance  
from  
(  
    select device_id,platform,source_id,month(from_unixtime(unix_timestamp(dt,'yyyymmdd'),'yyyy-mm-dd')) as month from dw_day_new_increament where dt<{day1}
) xinzeng  
left outer join   
(  
    select
 distinct device_id,
 platform,
 source_id,
 month(from_unixtime(unix_timestamp(dt,'yyyymmdd'),'yyyy-mm-dd')) as month
    from ods_user_page_log   
    where dt<={day1} and month(from_unixtime(unix_timestamp(dt,'yyyymmdd'),'yyyy-mm-dd'))=month(from_unixtime(unix_timestamp('{day1}','yyyymmdd'),'yyyy-mm-dd')) 
) cunliu on xinzeng.device_id=cunliu.device_id and 
xinzeng.platform=cunliu.platform and 
xinzeng.source_id=cunliu.source_id
group by xinzeng.platform,xinzeng.source_id,xinzeng.month,month(from_unixtime(unix_timestamp('{day1}','yyyymmdd'),'yyyy-mm-dd'))-xinzeng.month  
) t 
left outer join  
(  
    select platform,source_id,month(from_unixtime(unix_timestamp(dt,'yyyymmdd'),'yyyy-mm-dd')) as month,count(device_id) as new_num from dw_day_new_increament where dt<={day1} 
	group by platform,source_id,month(from_unixtime(unix_timestamp(dt,'yyyymmdd'),'yyyy-mm-dd'))
) t1 on t1.month=t.month and t1.platform=t.platform and t1.source_id=t.source_id
where t.month_distance>0   
union 
select 'all',coalesce(t.app_version,null),coalesce(t.source_id,null),case when length(t.month)=1 then concat({the_year},0,t.month) else concat({the_year},t.month) end,t1.new_num,t.remain_num,t.month_distance from 
(select    
xinzeng.app_version,
xinzeng.source_id,
xinzeng.month,
count(case when cunliu.device_id is not null then 1 else null end) as remain_num,  
month(from_unixtime(unix_timestamp('{day1}','yyyymmdd'),'yyyy-mm-dd'))-xinzeng.month as month_distance  
from  
(  
    select device_id,app_version,source_id,month(from_unixtime(unix_timestamp(dt,'yyyymmdd'),'yyyy-mm-dd')) as month from dw_day_new_increament where dt<{day1}
) xinzeng  
left outer join   
(  
    select
 distinct device_id,
 app_version,
 source_id,
 month(from_unixtime(unix_timestamp(dt,'yyyymmdd'),'yyyy-mm-dd')) as month
    from ods_user_page_log   
    where dt<={day1} and month(from_unixtime(unix_timestamp(dt,'yyyymmdd'),'yyyy-mm-dd'))=month(from_unixtime(unix_timestamp('{day1}','yyyymmdd'),'yyyy-mm-dd')) 
) cunliu on xinzeng.device_id=cunliu.device_id and 
xinzeng.app_version=cunliu.app_version and 
xinzeng.source_id=cunliu.source_id
group by xinzeng.app_version,xinzeng.source_id,xinzeng.month,month(from_unixtime(unix_timestamp('{day1}','yyyymmdd'),'yyyy-mm-dd'))-xinzeng.month  
) t 
left outer join  
(  
    select app_version,source_id,month(from_unixtime(unix_timestamp(dt,'yyyymmdd'),'yyyy-mm-dd')) as month,count(device_id) as new_num from dw_day_new_increament where dt<={day1} 
	group by app_version,source_id,month(from_unixtime(unix_timestamp(dt,'yyyymmdd'),'yyyy-mm-dd'))
) t1 on t1.month=t.month and t1.app_version=t.app_version and t1.source_id=t.source_id 
where t.month_distance>0   
union 
select 'all','all',t.source_id,case when length(t.month)=1 then concat({the_year},0,t.month) else concat({the_year},t.month) end,t1.new_num,t.remain_num,t.month_distance from 
(select    
xinzeng.source_id,
xinzeng.month,
count(case when cunliu.device_id is not null then 1 else null end) as remain_num,  
month(from_unixtime(unix_timestamp('{day1}','yyyymmdd'),'yyyy-mm-dd'))-xinzeng.month as month_distance  
from  
(  
    select device_id,source_id,month(from_unixtime(unix_timestamp(dt,'yyyymmdd'),'yyyy-mm-dd')) as month from dw_day_new_increament where dt<{day1}
) xinzeng  
left outer join   
(  
    select
 distinct device_id,
 source_id,
 month(from_unixtime(unix_timestamp(dt,'yyyymmdd'),'yyyy-mm-dd')) as month
    from ods_user_page_log   
    where dt<={day1} and month(from_unixtime(unix_timestamp(dt,'yyyymmdd'),'yyyy-mm-dd'))=month(from_unixtime(unix_timestamp('{day1}','yyyymmdd'),'yyyy-mm-dd')) 
) cunliu on xinzeng.device_id=cunliu.device_id and 
xinzeng.source_id=cunliu.source_id 
group by xinzeng.source_id,xinzeng.month,month(from_unixtime(unix_timestamp('{day1}','yyyymmdd'),'yyyy-mm-dd'))-xinzeng.month  
) t 
left outer join  
(  
    select source_id,month(from_unixtime(unix_timestamp(dt,'yyyymmdd'),'yyyy-mm-dd')) as month,count(device_id) as new_num from dw_day_new_increament where dt<={day1} 
	group by source_id,month(from_unixtime(unix_timestamp(dt,'yyyymmdd'),'yyyy-mm-dd'))
) t1 on t1.source_id=t.source_id and t1.month=t.month 
where t.month_distance>0   
union 
select 'all',t.app_version,'all',case when length(t.month)=1 then concat({the_year},0,t.month) else concat({the_year},t.month) end,t1.new_num,t.remain_num,t.month_distance from 
(select    
xinzeng.app_version,
xinzeng.month,
count(case when cunliu.device_id is not null then 1 else null end) as remain_num,  
month(from_unixtime(unix_timestamp('{day1}','yyyymmdd'),'yyyy-mm-dd'))-xinzeng.month as month_distance  
from  
(  
    select device_id,app_version,month(from_unixtime(unix_timestamp(dt,'yyyymmdd'),'yyyy-mm-dd')) as month from dw_day_new_increament where dt<{day1}
) xinzeng  
left outer join   
(  
    select
 distinct device_id,
 app_version,
 month(from_unixtime(unix_timestamp(dt,'yyyymmdd'),'yyyy-mm-dd')) as month
    from ods_user_page_log   
    where dt<={day1} and month(from_unixtime(unix_timestamp(dt,'yyyymmdd'),'yyyy-mm-dd'))=month(from_unixtime(unix_timestamp('{day1}','yyyymmdd'),'yyyy-mm-dd')) 
) cunliu on xinzeng.device_id=cunliu.device_id and 
xinzeng.app_version=cunliu.app_version 
group by xinzeng.app_version,xinzeng.month,month(from_unixtime(unix_timestamp('{day1}','yyyymmdd'),'yyyy-mm-dd'))-xinzeng.month  
) t 
left outer join  
(  
    select app_version,month(from_unixtime(unix_timestamp(dt,'yyyymmdd'),'yyyy-mm-dd')) as month,count(device_id) as new_num from dw_day_new_increament where dt<={day1} 
	group by app_version,month(from_unixtime(unix_timestamp(dt,'yyyymmdd'),'yyyy-mm-dd'))
) t1 on t.app_version=t1.app_version and t1.month=t.month 
where t.month_distance>0   
union 
select t.platform,'all','all',case when length(t.month)=1 then concat({the_year},0,t.month) else concat({the_year},t.month) end,t1.new_num,t.remain_num,t.month_distance from 
(select    
xinzeng.platform,
xinzeng.month,
count(case when cunliu.device_id is not null then 1 else null end) as remain_num,  
month(from_unixtime(unix_timestamp('{day1}','yyyymmdd'),'yyyy-mm-dd'))-xinzeng.month as month_distance  
from  
(  
    select device_id,platform,month(from_unixtime(unix_timestamp(dt,'yyyymmdd'),'yyyy-mm-dd')) as month from dw_day_new_increament where dt<{day1}
) xinzeng  
left outer join   
(  
    select
 distinct device_id,
 platform,
 month(from_unixtime(unix_timestamp(dt,'yyyymmdd'),'yyyy-mm-dd')) as month
    from ods_user_page_log   
    where dt<={day1} and month(from_unixtime(unix_timestamp(dt,'yyyymmdd'),'yyyy-mm-dd'))=month(from_unixtime(unix_timestamp('{day1}','yyyymmdd'),'yyyy-mm-dd')) 
) cunliu on xinzeng.device_id=cunliu.device_id and 
xinzeng.platform=cunliu.platform 
group by xinzeng.platform,xinzeng.month,month(from_unixtime(unix_timestamp('{day1}','yyyymmdd'),'yyyy-mm-dd'))-xinzeng.month  
) t 
left outer join  
(  
    select platform,month(from_unixtime(unix_timestamp(dt,'yyyymmdd'),'yyyy-mm-dd')) as month,count(device_id) as new_num from dw_day_new_increament where dt<={day1} 
	group by platform,month(from_unixtime(unix_timestamp(dt,'yyyymmdd'),'yyyy-mm-dd'))
) t1 on t1.platform=t.platform and t1.month=t.month 
where t.month_distance>0   
union 
select 'all','all','all',case when length(t.month)=1 then concat({the_year},0,t.month) else concat({the_year},t.month) end,t1.new_num,t.remain_num,t.month_distance from 
(select    
xinzeng.month,
count(case when cunliu.device_id is not null then 1 else null end) as remain_num,  
month(from_unixtime(unix_timestamp('{day1}','yyyymmdd'),'yyyy-mm-dd'))-xinzeng.month as month_distance  
from  
(  
    select device_id,month(from_unixtime(unix_timestamp(dt,'yyyymmdd'),'yyyy-mm-dd')) as month from dw_day_new_increament where dt<{day1}
) xinzeng  
left outer join   
(  
    select
 distinct device_id,
 month(from_unixtime(unix_timestamp(dt,'yyyymmdd'),'yyyy-mm-dd')) as month
    from ods_user_page_log   
    where dt<={day1} and month(from_unixtime(unix_timestamp(dt,'yyyymmdd'),'yyyy-mm-dd'))=month(from_unixtime(unix_timestamp('{day1}','yyyymmdd'),'yyyy-mm-dd')) 
) cunliu on xinzeng.device_id=cunliu.device_id  
group by xinzeng.month,month(from_unixtime(unix_timestamp('{day1}','yyyymmdd'),'yyyy-mm-dd'))-xinzeng.month  
) t 
left outer join  
(  
    select month(from_unixtime(unix_timestamp(dt,'yyyymmdd'),'yyyy-mm-dd')) as month,count(device_id) as new_num from dw_day_new_increament where dt<={day1} 
	group by month(from_unixtime(unix_timestamp(dt,'yyyymmdd'),'yyyy-mm-dd'))
) t1 on t1.month=t.month
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
