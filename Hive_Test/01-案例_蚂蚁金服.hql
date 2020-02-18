/*
背景说明：
以下表记录了用户每天的蚂蚁森林低碳生活领取的记录流水。
table_name：user_low_carbon
user_id data_dt  low_carbon
用户     日期      减少碳排放（g）
u_001	2017/1/1	10
u_001	2017/1/2	150
u_001	2017/1/2	110
u_001	2017/1/2	10
u_001	2017/1/4	50
u_001	2017/1/4	10
u_001	2017/1/6	45
u_001	2017/1/6	90
u_002	2017/1/1	10
u_002	2017/1/2	150
u_002	2017/1/2	70
u_002	2017/1/3	30
u_002	2017/1/3	80
u_002	2017/1/4	150
u_002	2017/1/5	101
u_002	2017/1/6	68
···
蚂蚁森林植物换购表，用于记录申领环保植物所需要减少的碳排放量
table_name:  plant_carbon
plant_id plant_name low_carbon
植物编号	植物名	换购植物所需要的碳
p001	梭梭树	17
p002	沙柳	19
p003	樟子树	146
p004	胡杨	215
*/
----题目
/*1.蚂蚁森林植物申领统计
问题：假设2017年1月1日开始记录低碳数据（user_low_carbon），假设2017年10月1日之前满足申领条件的用户都申领了一颗p004-胡杨，
剩余的能量全部用来领取“p002-沙柳” 。
统计在10月1日累计申领“p002-沙柳” 排名前10的用户信息；以及他比后一名多领了几颗沙柳。
得到的统计结果如下表样式：
user_id  plant_count less_count(比后一名多领了几颗沙柳)
u_101    1000         100
u_088    900          400
u_103    500          …
*/
--#解：
--#1）计算10月1日之前用户的全部能量
select
    user_id,
    sum(low_carbon) sum_carbon
from user_low_carbon
where date_format(regexp_replace(data_dt,'/','-'),'yyyy-MM')<='2017-10-01'
group by user_id;--t1

--#2）分别取出胡杨和沙柳的所需的能量
select low_carbon carbon_hu
from plant_carbon
where plant_id = 'p004';--t2

select low_carbon carbon_sha
from plant_carbon
where plant_id = 'p002';--t3

--#3）关联plant_carbon表，计算种树个数
select
    user_id,
    floor((sum_carbon-carbon_hu)/carbon_sha) plant_count
from (
    select
        user_id,
        sum(low_carbon) sum_carbon
    from user_low_carbon
    where date_format(regexp_replace(data_dt,'/','-'),'yyyy-MM')<='2017-10-01'
    group by user_id
) t1,(
    select low_carbon carbon_hu
    from plant_carbon
    where plant_id = 'p004'
) t2,(
    select low_carbon carbon_sha
    from plant_carbon
    where plant_id = 'p002'
) t3
order by plant_count desc
limit 11;--t4

--#4）将领取沙柳排名下一位用户的个数，移动到当前用户
select
    user_id,
    plant_count,
    lead(plant_count,1,0) over(order by plant_count desc) next_count
from (
    select
        user_id,
        floor((sum_carbon-carbon_hu)/carbon_sha) plant_count
    from (
        select
            user_id,
            sum(low_carbon) sum_carbon
        from user_low_carbon
        where date_format(regexp_replace(data_dt,'/','-'),'yyyy-MM')<='2017-10-01'
        group by user_id
    ) t1,(
        select low_carbon carbon_hu
        from plant_carbon
        where plant_id = 'p004'
    )t2,(
        select low_carbon carbon_sha
        from plant_carbon
        where plant_id = 'p002'
    )t3
    order by plant_count desc
    limit 11
) t4;--t5

--#5)领取沙柳排名前10的用户，以及他比后一名多领了几颗沙柳
select
    user_id,
    plant_count,
    plant_count - next_count less_count
from (
    select
    user_id,
    plant_count,
    lead(plant_count,1,0) over(order by plant_count desc) next_count
    from (
        select
            user_id,
            floor((sum_carbon-carbon_hu)/carbon_sha) plant_count
        from (
            select
                user_id,
                sum(low_carbon) sum_carbon
            from user_low_carbon
            where date_format(regexp_replace(data_dt,'/','-'),'yyyy-MM')<='2017-10-01'
            group by user_id
        ) t1,(
            select low_carbon carbon_hu
            from plant_carbon
            where plant_id = 'p004'
        )t2,(
            select low_carbon carbon_sha
            from plant_carbon
            where plant_id = 'p002'
        )t3
        order by plant_count desc
        limit 11
    ) t4
) t5
limit 10;

/*
2、蚂蚁森林低碳用户排名分析
问题：查询user_low_carbon表中每日流水记录，条件为：
用户在2017年，连续三天（或以上）的天数里，
每天减少碳排放（low_carbon）都超过100g的用户低碳流水。
需要查询返回满足以上条件的user_low_carbon表中的记录流水。
例如用户u_002符合条件的记录如下，因为2017/1/2~2017/1/5连续四天的碳排放量之和都大于等于100g：
seq（key） user_id data_dt  low_carbon
xxxxx10    u_002  2017/1/2  150
xxxxx11    u_002  2017/1/2  70
xxxxx12    u_002  2017/1/3  30
xxxxx13    u_002  2017/1/3  80
xxxxx14    u_002  2017/1/4  150
xxxxx14    u_002  2017/1/5  101
备注：统计方法不限于sql、procedure、python,java等
table_name：user_low_carbon
user_id data_dt  low_carbon
用户     日期      减少碳排放（g）
u_001	2017/1/1	10
u_001	2017/1/2	150
u_001	2017/1/2	110
u_001	2017/1/2	10
u_001	2017/1/4	50
u_001	2017/1/4	10
u_001	2017/1/6	45
u_001	2017/1/6	90
u_002	2017/1/1	10
u_002	2017/1/2	150
u_002	2017/1/2	70
u_002	2017/1/3	30
u_002	2017/1/3	80
u_002	2017/1/4	150
u_002	2017/1/5	101
u_002	2017/1/6	68
···
*/
--#解：
--#1）按照用户和日期分组，统计用户每天产生的能量
select
    user_id,
    data_dt,
    sum(low_carbon) sum_carbon
from user_low_carbon
group by user_id,data_dt;--t1
--#2）校对日期格式
select
    user_id,
    regexp_replace(data_dt,'/','-') data_dd,
    sum_carbon
from (
    select
        user_id,
        data_dt,
        sum(low_carbon) sum_carbon
    from user_low_carbon
    group by user_id,data_dt
) t1;--t2

--#2）开窗，列出该用户第三次上线的日期，
select
user_id,
data_dd,
sum_carbon,
    lead(data_dd,2,0) over(partition by user_id order by data_dd) third_data
from (
    select
        user_id,
        regexp_replace(data_dt,'/','-') data_dd,
        sum_carbon
    from (
        select
            user_id,
            data_dt,
            sum(low_carbon) sum_carbon
        from user_low_carbon
        group by user_id,data_dt
    ) t1
) t2;--t3

--#3)检查用户是否连续三天上线,连续三天中的能量是否大于100g
select
    user_id,
    data_dd,
    sum_carbon,
    count(*) over(partition by user_id) counts
from (
    select
        user_id,
        data_dd,
        sum_carbon,
        lead(data_dd,2,0) over(partition by user_id order by data_dd) third_data
    from (
        select
            user_id,
            regexp_replace(data_dt,'/','-') data_dd,
            sum_carbon
        from (
            select
                user_id,
                data_dt,
                sum(low_carbon) sum_carbon
            from user_low_carbon
            group by user_id,data_dt
        ) t1
    ) t2
) t3
where datediff(third_data,data_dd) = 2 and sum_carbon >=100;--t4
--#4）求出能量连续三天能量超过100g的用户列表
select
    user_id
from (
    select
        user_id,
        data_dd,
        sum_carbon,
        count(*) over(partition by user_id) counts
    from (
        select
        user_id,
        data_dd,
        sum_carbon,
        lead(data_dd,2,0) over(partition by user_id order by data_dd) third_data
        from (
            select
                user_id,
                regexp_replace(data_dt,'/','-') data_dd,
                sum_carbon
            from (
                select
                    user_id,
                    data_dt,
                    sum(low_carbon) sum_carbon
                from user_low_carbon
                group by user_id,data_dt
            ) t1
        ) t2
    ) t3
    where datediff(third_data,data_dd) = 2 and sum_carbon >=100
) t4
where counts>=3
group by user_id;--t5

--#5）根据列表，筛选出用户
select *
from user_low_carbon
where user_id in 
(
select
    user_id
from (
    select
        user_id,
        data_dd,
        sum_carbon,
        count(*) over(partition by user_id) counts
    from (
        select
        user_id,
        data_dd,
        sum_carbon,
        lead(data_dd,2,0) over(partition by user_id order by data_dd) third_data
        from (
            select
                user_id,
                regexp_replace(data_dt,'/','-') data_dd,
                sum_carbon
            from (
                select
                    user_id,
                    data_dt,
                    sum(low_carbon) sum_carbon
                from user_low_carbon
                group by user_id,data_dt
            ) t1
        ) t2
    ) t3
    where datediff(third_data,data_dd) = 2 and sum_carbon >=100
) t4
where counts>=3
group by user_id
) ;--t6
-------------------------------------------------------------------------------------------------------------
--解法一：
--1）更改日期格式，筛选满足当前100g能量：
select
    user_id,
    regexp_replace(data_dt,'/','-') data_dt,
    sum(low_carbon) sum_low_carbon
from
    user_low_carbon
group by user_id,data_dt
having sum_low_carbon>100;t1

--2）将当前用户，前两天和后两天的信息移动到当前行
select
    user_id,
    data_dt,
    lag(data_dt,2,'1970-01-01') over(partition by user_id order by data_dt) lag2,
    lag(data_dt,1,'1970-01-01') over(partition by user_id order by data_dt) lag1,
    lead(data_dt,1,'9999-99-99') over(partition by user_id order by data_dt) lead1,
    lead(data_dt,2,'9999-99-99') over(partition by user_id order by data_dt) lead2
from (
    select
        user_id,
        regexp_replace(data_dt,'/','-') data_dt,
        sum(low_carbon) sum_low_carbon
    from
        user_low_carbon
    group by user_id,data_dt
    having sum_low_carbon>100
)t1;t2

--3）计算上下日期的差值
select
    user_id,
    data_dt,
    datediff(lag2,data_dt) lag2,
    datediff(lag1,data_dt) lag1,
    datediff(lead1,data_dt) lead1,
    datediff(lead2,data_dt) lead2
from (
    select
        user_id,
        data_dt,
        lag(data_dt,2,'1970-01-01') over(partition by user_id order by data_dt) lag2,
        lag(data_dt,1,'1970-01-01') over(partition by user_id order by data_dt) lag1,
        lead(data_dt,1,'9999-99-99') over(partition by user_id order by data_dt) lead1,
        lead(data_dt,2,'9999-99-99') over(partition by user_id order by data_dt) lead2
    from (
        select
            user_id,
            regexp_replace(data_dt,'/','-') data_dt,
            sum(low_carbon) sum_low_carbon
        from
            user_low_carbon
        group by user_id,data_dt
        having sum_low_carbon>100
    )t1
)t2;t3

--4）判断日期是否是连续三天
select
    user_id,
    data_dt
from (
    select
        user_id,
        data_dt,
        datediff(lag2,data_dt) lag2,
        datediff(lag1,data_dt) lag1,
        datediff(lead1,data_dt) lead1,
        datediff(lead2,data_dt) lead2
    from (
        select
            user_id,
            data_dt,
            lag(data_dt,2,'1970-01-01') over(partition by user_id order by data_dt) lag2,
            lag(data_dt,1,'1970-01-01') over(partition by user_id order by data_dt) lag1,
            lead(data_dt,1,'9999-99-99') over(partition by user_id order by data_dt) lead1,
            lead(data_dt,2,'9999-99-99') over(partition by user_id order by data_dt) lead2
        from (
            select
                user_id,
                regexp_replace(data_dt,'/','-') data_dt,
                sum(low_carbon) sum_low_carbon
            from
                user_low_carbon
            group by user_id,data_dt
            having sum_low_carbon>100
        )t1
    )t2
)t3
where (lag2 = -2 and lag1 = -1) or (lag1 = -1 and lead1 = 1) or (lead1 = 1 and lead2 = 2);t4

-------------------------------------------------------------------------------------------------------------
--解法二：
#1）更改日期格式，筛选满足当前100g能量：
select
    user_id,
    regexp_replace(data_dt,'/','-') data_dt,
    sum(low_carbon) sum_low_carbon
from
    user_low_carbon
group by user_id,data_dt
having sum_low_carbon>100;t1

#2）按照用户和日期，对数据进行排序：
select
    user_id,
    data_dt,
    rank() over(partition by user_id order by data_dt) rk
from (
    select
        user_id,
        regexp_replace(data_dt,'/','-') data_dt,
        sum(low_carbon) sum_low_carbon
    from
        user_low_carbon
    group by user_id,data_dt
    having sum_low_carbon>100
) t1;t3

#3）利用等差数列，求日期和排名之间的差值
select
    user_id,
    data_dt,
    rk,
    date_sub(data_dt,rk) diff_date
from (
    select
        user_id,
        data_dt,
        rank() over(partition by user_id order by data_dt) rk
    from (
        select
            user_id,
            regexp_replace(data_dt,'/','-') data_dt,
            sum(low_carbon) sum_low_carbon
        from
            user_low_carbon
        group by user_id,data_dt
        having sum_low_carbon>100
    ) t1
) t3;t4

#4）对差值相同的用户计数：
select
    user_id,
    data_dt,
    count(*) over(partition by user_id,diff_date) count_date
from (
    select
        user_id,
        data_dt,
        rk,
        date_sub(data_dt,rk) diff_date
    from (
        select
            user_id,
            data_dt,
            rank() over(partition by user_id order by data_dt) rk
        from (
            select
                user_id,
                regexp_replace(data_dt,'/','-') data_dt,
                sum(low_carbon) sum_low_carbon
            from
                user_low_carbon
            group by user_id,data_dt
            having sum_low_carbon>100
        ) t1
    ) t3
) t4;t5

#5）筛选连续三天以上的用户
select
    user_id,
    data_dt
from (
    select
        user_id,
        data_dt,
        count(*) over(partition by user_id,diff_date) count_date
    from (
        select
            user_id,
            data_dt,
            rk,
            date_sub(data_dt,rk) diff_date
        from (
            select
                user_id,
                data_dt,
                rank() over(partition by user_id order by data_dt) rk
            from (
                select
                    user_id,
                    regexp_replace(data_dt,'/','-') data_dt,
                    sum(low_carbon) sum_low_carbon
                from
                    user_low_carbon
                group by user_id,data_dt
                having sum_low_carbon>100
            ) t1
        ) t3
    ) t4
) t5
where count_date>=3;t6