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

# SQL之lag()和lead()函数使用

函数简介

lag()和lead()这两个函数可以查询我们得到的结果集上下偏移相应行数的相应的结果。

## lag()函数：

查询当前行向上偏移n行对应的结果
该函数有三个参数：第一个为待查询的参数列名，第二个为向上偏移的位数，第三个参数为超出最上面边界的默认值。
看如下代码：

–查询向上偏移 1 位的年龄

```sql
SELECT user_id,
       user_age,
       lag(user_age, 1, 0) over(ORDER BY user_id) RESULT
FROM user_info;
```



## lead()函数：

这里写图片描述
查询当前行向下偏移n行对应的结果
该函数有三个参数：第一个为待查询的参数列名，第二个为向下偏移的位数，第三个参数为超出最下面边界的默认值。
如下代码：

–查询向下偏移 2 位的年龄

```sql
SELECT user_id,
       user_age,
       lead(user_age, 2, 0) over(ORDER BY user_id)
  FROM user_info;
```

# SQL With As 用法

WITH AS短语，也叫做子查询部分（subquery factoring），可以定义一个SQL片断，该SQL片断会被整个SQL语句用到。

可以使SQL语句的可读性更高，也可以在UNION ALL的不同部分，作为提供数据的部分。

对于UNION ALL，使用WITH AS定义了一个UNION ALL语句，当该片断被调用2次以上，优化器会自动将该WITH  AS短语所获取的数据放入一个Temp表中。

而提示meterialize则是强制将WITH  AS短语的数据放入一个全局临时表中。很多查询通过该方式都可以提高速度。

## 在使用CTE时应注意如下几点：

1. CTE后面必须直接跟使用CTE的SQL语句（如select、insert、update等），否则，CTE将失效。如下面的SQL语句将无法正常使用CTE：

```sql
with
cr as
(
    select CountryRegionCode from person.CountryRegion where Name like 'C%'
)
select * from person.CountryRegion  -- 应将这条SQL语句去掉
-- 使用CTE的SQL语句应紧跟在相关的CTE后面 --
select * from person.StateProvince where CountryRegionCode in (select * from cr)
```

2. CTE后面也可以跟其他的CTE，但只能使用一个with，多个CTE中间用逗号（,）分隔，如下面的SQL语句所示：

   ```sql
   with
   cte1 as
   (
       select * from table1 where name like 'abc%'
   ),
   cte2 as
   (
       select * from table2 where id > 20
   ),
   cte3 as
   (
       select * from table3 where price < 100
   )
   select a.* from cte1 a, cte2 b, cte3 c where a.id = b.id and a.id = c.id
   ```

   

3. 如果CTE的表达式名称与某个数据表或视图重名，则紧跟在该CTE后面的SQL语句使用的仍然是CTE，当然，后面的SQL语句使用的就是数据表或视图了，如下面的SQL语句所示：

```sql
--  table1是一个实际存在的表
 
with
table1 as
(
    select * from persons where age < 30
)
select * from table1  --  使用了名为table1的公共表表达式
select * from table1  --  使用了名为table1的数据表
```

4. CTE 可以引用自身，也可以引用在同一 WITH 子句中预先定义的 CTE。不允许前向引用。

```sql
--使用递归公用表表达式显示递归的多个级别
WITH DirectReports(ManagerID, EmployeeID, EmployeeLevel) AS 
(
    SELECT ManagerID, EmployeeID, 0 AS EmployeeLevel
    FROM HumanResources.Employee
    WHERE ManagerID IS NULL
    UNION ALL
    SELECT e.ManagerID, e.EmployeeID, EmployeeLevel + 1
    FROM HumanResources.Employee e
        INNER JOIN DirectReports d
        ON e.ManagerID = d.EmployeeID 
)
SELECT ManagerID, EmployeeID, EmployeeLevel 
FROM DirectReports ;
 
--使用递归公用表表达式显示递归的两个级别
WITH DirectReports(ManagerID, EmployeeID, EmployeeLevel) AS 
(
    SELECT ManagerID, EmployeeID, 0 AS EmployeeLevel
    FROM HumanResources.Employee
    WHERE ManagerID IS NULL
    UNION ALL
    SELECT e.ManagerID, e.EmployeeID, EmployeeLevel + 1
    FROM HumanResources.Employee e
        INNER JOIN DirectReports d
        ON e.ManagerID = d.EmployeeID 
)
SELECT ManagerID, EmployeeID, EmployeeLevel 
FROM DirectReports 
WHERE EmployeeLevel <= 2 
 
--使用递归公用表表达式显示层次列表
WITH DirectReports(Name, Title, EmployeeID, EmployeeLevel, Sort)
AS (SELECT CONVERT(varchar(255), c.FirstName + ' ' + c.LastName),
        e.Title,
        e.EmployeeID,
        1,
        CONVERT(varchar(255), c.FirstName + ' ' + c.LastName)
    FROM HumanResources.Employee AS e
    JOIN Person.Contact AS c ON e.ContactID = c.ContactID 
    WHERE e.ManagerID IS NULL
    UNION ALL
    SELECT CONVERT(varchar(255), REPLICATE ('| ' , EmployeeLevel) +
        c.FirstName + ' ' + c.LastName),
        e.Title,
        e.EmployeeID,
        EmployeeLevel + 1,
        CONVERT (varchar(255), RTRIM(Sort) + '| ' + FirstName + ' ' + 
                 LastName)
    FROM HumanResources.Employee as e
    JOIN Person.Contact AS c ON e.ContactID = c.ContactID
    JOIN DirectReports AS d ON e.ManagerID = d.EmployeeID
    )
SELECT EmployeeID, Name, Title, EmployeeLevel
FROM DirectReports 
ORDER BY Sort
 
--使用 MAXRECURSION 取消一条语句
--可以使用 MAXRECURSION 来防止不合理的递归 CTE 进入无限循环。以下示例特意创建了一个无限循环，然后使用 MAXRECURSION 提示将递归级别限制为两级
WITH cte (EmployeeID, ManagerID, Title) as
(
    SELECT EmployeeID, ManagerID, Title
    FROM HumanResources.Employee
    WHERE ManagerID IS NOT NULL
  UNION ALL
    SELECT cte.EmployeeID, cte.ManagerID, cte.Title
    FROM cte 
    JOIN  HumanResources.Employee AS e 
        ON cte.ManagerID = e.EmployeeID
)
--Uses MAXRECURSION to limit the recursive levels to 2
SELECT EmployeeID, ManagerID, Title
FROM cte
OPTION (MAXRECURSION 2)
--在更正代码错误之后，就不再需要 MAXRECURSION。以下示例显示了更正后的代码
WITH cte (EmployeeID, ManagerID, Title)
AS
(
    SELECT EmployeeID, ManagerID, Title
    FROM HumanResources.Employee
    WHERE ManagerID IS NOT NULL
  UNION ALL
    SELECT  e.EmployeeID, e.ManagerID, e.Title
    FROM HumanResources.Employee AS e
    JOIN cte ON e.ManagerID = cte.EmployeeID
)
SELECT EmployeeID, ManagerID, Title
FROM cte
```

5. 不能在 CTE_query_definition 中使用以下子句：

　　（1）COMPUTE 或 COMPUTE BY

　　（2）ORDER BY（除非指定了 TOP 子句）

　　（3）INTO

　　（4）带有查询提示的 OPTION 子句

　　（5）FOR XML

　　（6）FOR BROWSE

6. 如果将 CTE 用在属于批处理的一部分的语句中，那么在它之前的语句必须以分号结尾，如下面的SQL所示：

```sql
declare @s nvarchar(3)
set @s = 'C%'
;  -- 必须加分号
with
t_tree as
(
    select CountryRegionCode from person.CountryRegion where Name like @s
)
select * from person.StateProvince where CountryRegionCode in (select * from t_tree)
```

