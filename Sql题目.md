# 1285.找到连续区间的开始和结束数字

https://leetcode-cn.com/problems/find-the-start-and-end-number-of-continuous-ranges/

```sql
with t1 as(
select 
log_id - row_number() over(order by log_id) rk ,
log_id 
from 
Logs )
select 
min(log_id) start_id,
max(log_id) end_id   
from
t1
group by rk
order by start_id
```

连续型值问题都可以考虑：

## Sql 四大排名函数（ROW_NUMBER、RANK、DENSE_RANK、NTILE）

RANK() 排序相同时会重复，总数不会变

DENSE_RANK() 排序相同时会重复，总数会减少

ROW_NUMBER() 会根据顺序计算

一、ROW_NUMBER

　　row_number的用途的非常广泛，排序最好用他，一般可以用来实现web程序的分页，他会为查询出来的每一行记录生成一个序号，依次排序且不会重复，注意使用row_number函数时必须要用over子句选择对某一列进行排序才能生成序号。row_number用法实例:

```sql
select ROW_NUMBER() OVER(order by [SubTime] desc) as row_num,* from [Order]
```




二、RANK

　　rank函数用于返回结果集的分区内每行的排名， 行的排名是相关行之前的排名数加一。简单来说rank函数就是对查询出来的记录进行排名，与row_number函数不同的是，rank函数考虑到了over子句中排序字段值相同的情况，如果使用rank函数来生成序号，over子句中排序字段值相同的序号是一样的，后面字段值不相同的序号将跳过相同的排名号排下一个，也就是相关行之前的排名数加一，可以理解为根据当前的记录数生成序号，后面的记录依此类推。可能我描述的比较苍白，理解起来也比较吃力，我们直接上代码，rank函数的使用方法与row_number函数完全相同。

```sql
select RANK() OVER(order by [UserId]) as rank,* from [Order] 
```

　


三、DENSE_RANK

　　dense_rank函数的功能与rank函数类似，dense_rank函数在生成序号时是连续的，而rank函数生成的序号有可能不连续。dense_rank函数出现相同排名时，将不跳过相同排名号，rank值紧接上一次的rank值。在各个分组内，rank()是跳跃排序，有两个第一名时接下来就是第四名，dense_rank()是连续排序，有两个第一名时仍然跟着第二名。将上面的Sql语句改由dense_rank函数来实现。

```sql
select DENSE_RANK() OVER(order by [UserId]) as den_rank,* from [Order]
```


四、NTILE

　　ntile函数可以对序号进行分组处理，将有序分区中的行分发到指定数目的组中。 各个组有编号，编号从一开始。 对于每一个行，ntile 将返回此行所属的组的编号。这就相当于将查询出来的记录集放到指定长度的数组中，每一个数组元素存放一定数量的记录。ntile函数为每条记录生成的序号就是这条记录所有的数组元素的索引（从1开始）。也可以将每一个分配记录的数组元素称为“桶”。ntile函数有一个参数，用来指定桶数。

```sql
select NTILE(4) OVER(order by [SubTime] desc) as ntile,* from [Order]
```



# 1841.League Statistics

```sql
with t1 as(
select 
  t.team_id,
  m.home_team_goals,
  m.away_team_goals goal_against,
  count(*)  matches_played_ashome,
    (case when m.home_team_goals > m.away_team_goals then 3
  when m.home_team_goals < m.away_team_goals then 0
  when m.home_team_goals = m.away_team_goals then 1 end) as score
from
Teams t left join Matches m 
on t.team_id = m.home_team_id
group by 
t.team_id
),
--客队得分统计
t2 as (
select 
  t.team_id,
  m. away_team_goals,
  m.home_team_goals goal_against,
  count(*)  matches_played_asaway ,
    (case when m.home_team_goals > m.away_team_goals then 1
  when m.home_team_goals < m.away_team_goals then -1
  when m.home_team_goals = m.away_team_goals then 0 end) as score
from
Teams t left join Matches m 
on t.team_id = m.away_team_id
group by 
t.team_id

)
--总得分以及次数统计
--t3 as (
select 
  t1.team_id,
  t1.matches_played_ashome + t2.matches_played_asaway matches_played,
  t1.score + t2.score points,
  t1.home_team_goals + t2.away_team_goals goal_for,
  t1.goal_against + t2.goal_against goal_against
from
t1 left join t2 
on t1.team_id = t2.team_id;
```

# 180. 连续出现的数字

```sql
表：Logs

+-------------+---------+
| Column Name | Type    |
+-------------+---------+
| id          | int     |
| num         | varchar |
+-------------+---------+
id 是这个表的主键。
```

编写一个 SQL 查询，查找所有至少连续出现三次的数字。

返回的结果表中的数据可以按 **任意顺序** 排列。

```
查询结果格式如下面的例子所示：

 

Logs 表：
+----+-----+
| Id | Num |
+----+-----+
| 1  | 1   |
| 2  | 1   |
| 3  | 1   |
| 4  | 2   |
| 5  | 1   |
| 6  | 2   |
| 7  | 2   |
+----+-----+

Result 表：
+-----------------+
| ConsecutiveNums |
+-----------------+
| 1               |
+-----------------+
1 是唯一连续出现至少三次的数字。
```

答案：

```sql
with t1 as(
SELECT
Num,
lag(Num,1,000) over() bef,
lead(Num,1,000) over() after
FROM 
Logs)
SELECT
distinct Num as ConsecutiveNums
from 
t1 
where Num=bef and num = after
```

# 手写Hql 1

表结构：uid,subject_id,score

求：找出所有科目成绩都大于某一学科平均成绩的学生

数据集如下

1001	01	90

1001	02	90

1001	03	90

1002	01	85

1002	02	85

1002	03	70

1003	01	70

1003	02	70

1003	03	85

1）建表语句

create table score(

​    uid string,

​    subject_id string,

​    score int)

row format delimited fields terminated by '\t'; 

2）求出每个学科平均成绩

```sql
with t1 as
(select
subject_id ,
avg（score） score_avg
from
score
group by 
subject_id),
t2 as
(select
	uid,
	score ,
	score_avg  
from
score
join
t1
on t1.subject_id = score.subject_id),
t3 as 
(
select 
    uid,
    if(score > score_avg,0,1) flag
from 
    t2
)
select
	uid
from
	t3
group by
	uid
having sum(flag)=0
```

# 相互关注用户

关注状态为1，取消关注状态为0

两个用户相互关注代表着包含两个用户的一个新字段分组之后求得的状态和为2

表结构

```sql
+-------+-------+----------------------+---------+
|  u1   |  u2   |        c_time        | status  |
+-------+-------+----------------------+---------+
| 1001  | 1002  | 2020-01-22 10:01:00  | 1       | -- 1001用户关注了 1002 用户
| 1001  | 1002  | 2020-01-22 10:02:00  | 0       | -- 1001用户取消关注了 1002 用户
| 1001  | 1002  | 2020-01-22 10:03:00  | 1       | -- 1001用户再次关注了 1002 用户
| 1002  | 1001  | 2020-01-22 10:04:00  | 1       | -- 1002用户关注了 1001 用户 
| 1001  | 1003  | 2020-01-22 10:05:00  | 1       | -- 根据时间取 u1 → u2 最新的关注状态
| 1001  | 1003  | 2020-01-22 10:06:00  | 0       | -- 再判断 两个用户间是否是互相关注的
| 1003  | 1001  | 2020-01-22 10:07:00  | 1       | -- 如果 1001 和 1002 是相互关注的,
| 1002  | 1003  | 2020-01-22 10:08:00  | 1       | -- 将这两个用户输出，但只保留一组 u1 u2
| 1003  | 1002  | 2020-01-22 10:09:00  | 1       | -- 不能再保留最新的u2 u1了
| 1004  | 1003  | 2020-01-22 10:10:00  | 1       |
| 1004  | 1001  | 2020-01-22 10:11:00  | 1       |
+-------+-------+----------------------+---------+

```

答案

```sql
with t1 as(
select 
    if(u1 < u2,concat(u1,'_',u2),concat(u2,'_',u1)) new_col,
    status,
    row_number() over(partition by u1,u2 order by c_time desc) as row_num
from 
    funcdb.tbl_fans_act)
select 
	cast(split(t1.new_col, ',')[0] as bigint) as u1,
	cast(split(t1.new_col, ',')[1] as bigint) as u2
from t1
where t1.row_num = 1
group by t1.new_col
having sum(status) = 2
```

# 按日期统计每天胜负的场次

```sql
-- 表数据
+-------------+-------+
|   g_date    | g_rs  |
+-------------+-------+
| 2005-05-09  | win   |
| 2005-05-09  | lose  |
| 2005-05-09  | lose  |
| 2005-05-09  | lose  |
| 2005-05-10  | win   |
| 2005-05-10  | lose  |
| 2005-05-10  | lose  |
+-------------+-------+
-- 输出结果
+-------------+------+-------+
|   g_date    | win  | lose  |
+-------------+------+-------+
| 2005-05-09  | 1    | 3     |
| 2005-05-10  | 1    | 2     |
+-------------+------+-------+

```

答案：

直接用sum（if）或者sum（case when then else end）

```sql
select
	sum(case when g_rs = 'win' then 1 else 0 end) as win,
	sum(if(g_rs = 'lose',1,0)) as 'lose'
from
funcdb.tbl_game_rs
group by g_date;
```

