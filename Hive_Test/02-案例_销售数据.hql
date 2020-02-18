/*
营销表数据
id	brand	startdate	enddate
1	nike	20190901	20190905
2	nike	20190903	20190906
3	nike	20190909	20190915
4	oppo	20190804	20190805
5	oppo	20190804	20190815
6	vivo	20190810	20190821
7	vivo	20190811	20190812
8	vivo	20190813	20190820
营销表数据中日期存在重复的情况，
例如id为1的enddate为20190905，id为2的startdate为20190903
即id为1的和id为2的存在重复的营销日期，求出每个品牌的促销天数(重复不算)，最终结果应为：
brand	all_days
nike	13
oppo	12
vivo	18
*/

--#解：
--#1）将下次活动的开始时间移动到当前活动
--##a.时间格式化
select
    id,
    brand,
    concat(substring(startdate,0,4),'-',substring(startdate,5,2),'-',substring(startdate,7)) sd,
    concat(substring(enddate,0,4),'-',substring(enddate,5,2),'-',substring(enddate,7)) ed
from marketing;--t1

--##b.移动时间
select
    id,
    brand,
    sd,
    ed,
    lead(sd,1,0) over(partition by brand order by id) secend_start
from (
    select
        id,
        brand,
        concat(substring(startdate,0,4),'-',substring(startdate,5,2),'-',substring(startdate,7)) sd,
        concat(substring(enddate,0,4),'-',substring(enddate,5,2),'-',substring(enddate,7)) ed
    from marketing
) t1;--t2

--#2）将下次活动的结束时间移动到当前活动
select
    brand,
    sd,
    ed,
    secend_start,
    lead(ed,1,0) over(partition by brand order by id) secend_end
from (
    select
        id,
        brand,
        sd,
        ed,
        lead(sd,1,0) over(partition by brand order by id) secend_start
    from (
        select
            id,
            brand,
            concat(substring(startdate,0,4),'-',substring(startdate,5,2),'-',substring(startdate,7)) sd,
            concat(substring(enddate,0,4),'-',substring(enddate,5,2),'-',substring(enddate,7)) ed
        from marketing
    ) t1
) t2;--t3


--#2）求出中间相隔天数
select
    brand,
    sd,
    ed,
    secend_start,
    secend_end,
    if(secend_start = 0,
        datediff(ed,sd) + 1
    ,
    case when ed < secend_start then datediff(ed,sd) + 1
         when ed >= secend_start and ed <= secend_end then datediff(secend_start,sd)
         when ed >= secend_start and ed > secend_end then datediff(secend_start,sd) + 1
         end
    ) per_count_day
from (
    select
        brand,
        sd,
        ed,
        secend_start,
        lead(ed,1,0) over(partition by brand order by id) secend_end
    from (
        select
            id,
            brand,
            sd,
            ed,
            lead(sd,1,0) over(partition by brand order by id) secend_start
        from (
            select
                id,
                brand,
                concat(substring(startdate,0,4),'-',substring(startdate,5,2),'-',substring(startdate,7)) sd,
                concat(substring(enddate,0,4),'-',substring(enddate,5,2),'-',substring(enddate,7)) ed
            from marketing
        ) t1
    ) t2
)t3;--t4

--#3）求出总天数
select
    brand,
    sum(per_count_day)
from (
    select
        brand,
        sd,
        ed,
        secend_start,
        secend_end,
        if(secend_start = 0,
            datediff(ed,sd) + 1
        ,
        case when ed < secend_start then datediff(ed,sd) + 1
            when ed >= secend_start and ed <= secend_end then datediff(secend_start,sd)
            when ed >= secend_start and ed > secend_end then datediff(secend_start,sd) + 1
            end
        ) per_count_day
    from (
        select
            brand,
            sd,
            ed,
            secend_start,
            lead(ed,1,0) over(partition by brand order by id) secend_end
        from (
            select
                id,
                brand,
                sd,
                ed,
                lead(sd,1,0) over(partition by brand order by id) secend_start
            from (
                select
                    id,
                    brand,
                    concat(substring(startdate,0,4),'-',substring(startdate,5,2),'-',substring(startdate,7)) sd,
                    concat(substring(enddate,0,4),'-',substring(enddate,5,2),'-',substring(enddate,7)) ed
                from marketing
            ) t1
        ) t2
    ) t3
) t4
group by brand;--t5

------------------------------------------------------------------------------------------------
(乱序问题：通过startdate和enddate，或id)
#1）时间格式化：
select
    id,
    brand,
    concat(substring(startdate,0,4),'-',substring(startdate,5,2),'-',substring(startdate,7)) startdate,
    concat(substring(enddate,0,4),'-',substring(enddate,5,2),'-',substring(enddate,7)) enddate
from marketing;t1

#2）将上一次活动的结束时间移动到当前行
select
    id,
    brand,
    startdate,
    enddate,
    lag(enddate,1,startdate) over(partition by brand order by id) last_end
from (
    select
        id,
        brand,
        concat(substring(startdate,0,4),'-',substring(startdate,5,2),'-',substring(startdate,7)) startdate,
        concat(substring(enddate,0,4),'-',substring(enddate,5,2),'-',substring(enddate,7)) enddate
    from marketing
)t1;t2

#3）上面所有活动的结束时间的最大值，以及标记第一行
select
    id,
    brand,
    startdate,
    enddate,
    --last_end,
    max(last_end) over(partition by brand order by id) max_last_date,
    lag(enddate,1) over(partition by brand order by id) flag
from (
    select
    id,
    brand,
    startdate,
    enddate,
    lag(enddate,1,startdate) over(partition by brand order by id) last_end
from (
    select
        id,
        brand,
        concat(substring(startdate,0,4),'-',substring(startdate,5,2),'-',substring(startdate,7)) startdate,
        concat(substring(enddate,0,4),'-',substring(enddate,5,2),'-',substring(enddate,7)) enddate
    from marketing
)t1
) t2;t3

#4）更新活动时间（判断有效的开始时间）
select
    brand,
    if(flag is null,startdate,if(max_last_date>=startdate,date_add(max_last_date,1),startdate)) startdate,
    enddate,
    max_last_date
from (
    select
        id,
        brand,
        startdate,
        enddate,
        --last_end,
        max(last_end) over(partition by brand order by id) max_last_date,
        lag(enddate,1) over(partition by brand order by id) flag
    from (
        select
        id,
        brand,
        startdate,
        enddate,
        lag(enddate,1,startdate) over(partition by brand order by id) last_end
    from (
        select
            id,
            brand,
            concat(substring(startdate,0,4),'-',substring(startdate,5,2),'-',substring(startdate,7)) startdate,
            concat(substring(enddate,0,4),'-',substring(enddate,5,2),'-',substring(enddate,7)) enddate
        from marketing
    )t1
    ) t2
) t3;t4

#5）计算活动之间的有效相隔时间
select
    brand,
    startdate,
    enddate,
    if(startdate>enddate,0,datediff(enddate,startdate) + 1) diff_date
from (
    select
        brand,
        if(flag is null,startdate,if(max_last_date>=startdate,date_add(max_last_date,1),startdate)) startdate,
        enddate,
        max_last_date
    from (
        select
            id,
            brand,
            startdate,
            enddate,
            --last_end,
            max(last_end) over(partition by brand order by id) max_last_date,
            lag(enddate,1) over(partition by brand order by id) flag
        from (
            select
            id,
            brand,
            startdate,
            enddate,
            lag(enddate,1,startdate) over(partition by brand order by id) last_end
        from (
            select
                id,
                brand,
                concat(substring(startdate,0,4),'-',substring(startdate,5,2),'-',substring(startdate,7)) startdate,
                concat(substring(enddate,0,4),'-',substring(enddate,5,2),'-',substring(enddate,7)) enddate
            from marketing
        )t1
        ) t2
    ) t3
) t4;t5

#6）求出最终结果
select
    brand,
    sum(diff_date)
from (
    select
        brand,
        startdate,
        enddate,
        if(startdate>enddate,0,datediff(enddate,startdate) + 1) diff_date
    from (
        select
            brand,
            if(flag is null,startdate,if(max_last_date>=startdate,date_add(max_last_date,1),startdate)) startdate,
            enddate,
            max_last_date
        from (
            select
                id,
                brand,
                startdate,
                enddate,
                --last_end,
                max(last_end) over(partition by brand order by id) max_last_date,
                lag(enddate,1) over(partition by brand order by id) flag
            from (
                select
                id,
                brand,
                startdate,
                enddate,
                lag(enddate,1,startdate) over(partition by brand order by id) last_end
            from (
                select
                    id,
                    brand,
                    concat(substring(startdate,0,4),'-',substring(startdate,5,2),'-',substring(startdate,7)) startdate,
                    concat(substring(enddate,0,4),'-',substring(enddate,5,2),'-',substring(enddate,7)) enddate
                from sale02
            )t1
            ) t2
        ) t3
    ) t4
) t5
group by brand;
