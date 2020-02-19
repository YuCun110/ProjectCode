#课堂练习

第1题
我们有如下的用户访问数据
userId	visitDate	visitCount
u01 2017/1/21	5
u02	2017/1/23	6
u03	2017/1/22	8
u04	2017/1/20	3
u01	2017/1/23	6
u01	2017/2/21	8
U02	2017/1/23	6
U01	2017/2/22	4
要求使用SQL统计出每个用户的累积访问次数，如下表所示：
id	  月份	小计 累积
u01	2017-01	 11	  11
u01	2017-02	 12	  23
u02	2017-01	 12	  12
u03	2017-01	 8	  8
u04	2017-01	 3    3
#解：
#1)将日期转化为月份
select userId,date_format(regexp_replace(visitDate,'/','-'),'yyyy-MM') vs_date,visitCount
from user_vd;t1
#2)按照用户和月份进行分组统计人次
select userId,vs_date,
    sum(visitCount) sum_counts
from (
    select userId,date_format(regexp_replace(visitDate,'/','-'),'yyyy-MM') vs_date,visitCount
    from user_vd
) t1
group by userid,vs_date;t2
#3)开窗，根据用户累加访问次数
select userId,vs_date,sum_counts,
    sum(sum_counts) over(partition by userId) sumCounts
from (
    select userId,vs_date,
        sum(visitCount) sum_counts
    from (
        select userId,date_format(regexp_replace(visitDate,'/','-'),'yyyy-MM') vs_date,visitCount
        from user_vd
    ) t1
    group by userid,vs_date
) t2;



第2题
有50W个京东店铺，每个顾客访客访问任何一个店铺的任何一个商品时都会产生一条访问日志，
访问日志存储的表名为Visit，访客的用户id为user_id，被访问的店铺名称为shop，请统计：
1）每个店铺的UV（访客数）
2）每个店铺访问次数top3的访客信息。输出店铺名称、访客id、访问次数
数据集
u1	a
u2	b
u1	b
u1	a
u3	c
u4	b
u1	a
u2	c
u5	b
u4	b
u6	c
u2	c
u1	b
u2	a
u2	a
u3	a
u5	a
u5	a
u5	a
#解：
（1）每个店铺的UV（访客数）
##法一：(数据量小时，运行速度慢)
#1）按照访客和店铺进行分组统计频次
select
    user_id,
    shop,
    count(*) per_count
from jd_visit
group by user_id,shop;t1
#2)统计访客数
select
    shop,
    count(*) uv
from (
    select
        user_id,
        shop,
        count(*) per_count
    from jd_visit
    group by user_id,shop
) t1
group by shop;t2

##法二：(数据量大时，会影响reduce的工作量)
#1)使用distinct去重
select distinct user_id,shop
from jd_visit;t1
#2）统计访客数
select
    shop,
    count(*) uv
from (
    select distinct user_id,shop
    from jd_visit
) t1
group by shop;t2

（2）每个店铺访问次数top3的访客信息。输出店铺名称、访客id、访问次数
#1）按照访客和店铺进行分组统计频次
select
    user_id,
    shop,
    count(*) per_count
from jd_visit
group by user_id,shop;t1
#2）开窗，计算每个店铺访客的排名
select
    shop,
    user_id,
    per_count,
    rank() over(partition by shop order by per_count) rk
from (
    select
        user_id,
        shop,
        count(*) per_count
    from jd_visit
    group by user_id,shop
) t1;t2
#3)筛选top3
select
    *
from (
    select
        shop,
        user_id,
        per_count,
        rank() over(partition by shop order by per_count) rk
    from (
        select
            user_id,
            shop,
            count(*) per_count
        from jd_visit
        group by user_id,shop
    ) t1
) t2
where rk<=3;t3