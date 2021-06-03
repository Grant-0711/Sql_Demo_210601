# 一、数仓分层





常用命令：

tail -f hive log：监控日志





## 1. DDL数据定义

### 1.1 创建数据库

```sql
CREATE DATABASE [IF NOT EXISTS] database_name
[COMMENT database_comment]
[LOCATION hdfs_path]
[WITH DBPROPERTIES (property_name=property_value, ...)];
```


创建一个数据库，数据库在HDFS上的默认存储路径是/user/hive/warehouse/*.db





























































举例： 

日志：  xxxx.lzo , 一条数据是一行！

ODS：  xxxx.lzo, 1列

也是分区表！以日期作为分区字段！



**DWD(数据明细层)层**：  将ODS层的数据，进行展开，将每个数据的明细抽取！

在抽取数据明细时，会根据要求，对数据进行ETL！

维度退化（降维）！

脱敏操作！

也是分区表！以日期作为分区字段！



**DWS(数据服务层)** ： 为数仓最终产生的数据，提供服务！

通过查询数据服务层提供的数据，或做一些计算，就可以想要的结果！

通常是按天，对所需的数据，进行聚合！

是分区表！以日期作为分区字段！



**DWT(数据主题层)**： 也是数据服务层一种特殊实现！ 将数据按照主题，进行累积聚合！

DWT层是一张全量表！

一般情况不是分区表！



**ADS（数据应用层）**： 存放的是统计出的结果！可以直接提供给应用程序或其他的需求方！

一般情况不是分区表！



**什么时候建分区表，什么时候不建？**

分区表是为了在数据量大，分散数据！将数据分散到多个分区目录中，在过滤时，可以根据分区的字段只选择其中的某些目录进行查询！

总结：  ODS：  O:原始数据

DWD：   D： 明细数据

DWS ：  S： 数据服务

DWT：  T ： 主题！累积的数据服务层！

ADS：  A ：  应用数据

注意： 层与层之间，要严格按照顺序进行数据的摄取和导入！

ODS ----->DWD----->DWS----->DWT

DWS/DWT ---->ADS

## 2.为什么要分层

①复杂问题简单化，方便维护

②减少相同需求的重复开发

③隔离原始（敏感）数据

## 3.数据集市和数据仓库

数据集市是微型的数据仓库，主要面向某个部门或业务线！

数据仓库是面向整个公司！

## 4.表命名

```
哪一层的表_(性质)_表名_后缀
```

哪一层的表： ODS,DWD,DWS,DWT,ADS

性质： fact: 事实表  dim: 维度表

后缀： 如果是临时表，加tmp 如果是用户行为数据，加log

## 5.脚本命名

```
数据源_to_目标_db/log.sh
```

例如： 脚本负责将用户行为数据，从HDFS导入到ODS层：  hdfs_to_ods_log.sh

脚本负责将用户业务数据，从ODS导入到DWD层：  ods_to_dwd_db.sh



## 6.不同层的表

ODS：  log： 1张表        1个字段

 db:    23张表      采集的字段有哪些，ODS层不同的表就有哪些字段！

分区表！

DWD层： log: 要分析的明细有哪些，就创建几张表

5张表： 页面，曝光，错误，事件，启动

db:   维度表----->降维： 将多张维度表合并为1张！   4张： 商品，优惠券，活动，用户！

事实表----->丰富 ： 8个事实，8张表！

①挑选事实业务： 选择感兴趣的事实表

将最细的粒度通过聚合得到粗的粒度！

③挑选维度（丰富）：  一般会基于3W(where ,when , who )原则+感兴趣的维度

④选择度量

导入数据： 典型的星型模型！ 维度建模！

事实表 join 维度表！

分区表！

DWS层：  老板要什么数据，需求是什么，表中就放什么字段！

将需求，按照主题分类，同一个主题的需求放入同一张表！

5个主题！5张表！

宽表！ 从多个事实表和维度表中导入数据！

分区表！

DWT层：  老板要什么数据，需求是什么，表中就放什么字段！

根据主题进行建表！

将需求，按照主题分类，同一个主题的需求放入同一张表！

5个主题！5张表！

宽表！ 从多个事实表和维度表中导入数据！

全量表！

ADS层：  老板要什么数据，需求是什么，表中就放什么字段！

每个需求一张表！如果需求主题一致，可以放入一张表！

全量表！				

# 二、数仓理论

## 1.范式

范式在设计一张表时需要遵守的规范和模式！

## 2.常见的三范式

第一范式： 属性不可切割！

第二范式： 表中除了主键列，其他的非主键列，都必须完全函数依赖于主键列！

​					不能出现部分函数依赖！

第三范式： 不能有传递函数依赖！

设计表时，至少要遵守三范式！

## 3.关系建模和维度建模

关系建模(E-R建模)：用于关系型数据的表模型设计！

特征： ①强调数据的治理(精细)

②强掉数据的整合

③保证消除数据的冗余和保证数据强一致性		

弊端： 如果要求业务的全部信息，需要进行多次的关联



维度建模： 主要面向业务！不在乎冗余和强一致性！业务的实现怎么方便怎么来！

事实表 + 若干维度表

不太遵循范式！将多个维度，降维为一个维度！方便和事实表进行关联！减少关联次数！

主要用在大数据的应用场景上！



## 5.维度表和事实表

事实表：在表中记录一个事实（已经发生的事件，动作，度量）的信息！	

我今天中午去津味源吃了一份20元的套餐，3个菜

事实表基本都有以下元素：

人物  时间  地点          度量

who  when  where   

维度表：用来描述事实中的部分属性，一般都是一些名词



## 6.维度建模的模型

星型模型： 维度表直接关联在事实表上！当查询某个维度时，最多只需要关联一次！

维度建模中使用的最多的！



雪花模型： 类似关系建模！存在维度表间接关联在事实表上！当查询某个维度时，可能需要关联多次！



星座模型： 本质上也是星型模型！是星型模型的一个变种！

可能有多个事实表！维度表还是直接关联在事实表上！存在多个事实表共有一个维度表！



## 7.事实表的分类

**事务型事实表**：如果一类事实，一旦发生，就不会变化。记录这类事实的表，称为事务型事实表！

这个表的特点是表中的数据只会不断新增，不会发生修改！

举例： 支付信息详情表！

事务型事实表  在同步数据时，只同步当天新增的数据！



**周期型快照事实表**：如果某个事实，在一个周期内会不断发生变化，只需要记录在周期结束时，事实的状态，此时这类表称为 周期型快照事实表！

​							举例： 记录一个人身高生长的事实

| 人   | 时间     | 身高 |
| ---- | -------- | ---- |
| jack | 2020-1-1 | 60   |
| jack | 2021-1-1 | 60   |
| jack | 2022-1-1 | 80   |

周期型快照事实表: 事实的记录会有周期，重视周期结束时，事实的状态（结果）！

粒度： 一个人在一个周期 ，身高的事实

在hive中更新方式：  insert into

**累积型快照事实表**：如果一个事实，在其生命周期内，不断变化！只记录在某些时间点的状态变化，且可以查看一个状态的累积变化趋势，称这类表为累积型快照事实表！

​						举例： 记录一个人身高生长的事实

| 人   | 出生 | 3岁时 | 12岁（青春期） | 18（成年时） | 22（成人时） |      |
| ---- | ---- | ----- | -------------- | ------------ | ------------ | ---- |
| jack | 40   | 70    | 120            | 180          | 183          |      |
| tom  | 30   | 60    |                |              |              |      |



粒度： 一个人的身高的事实

在hive中更新方式：    update操作

​		hive默认不支持update!

​					insert overwrite (select  old  +   new )





分类是为了，总结同一类型表导入数据的方式！方便向表中导入数据！







# 三、数仓环境搭建

## 1.环境

Hive on Spark

要求： ①安装Spark，配置Spark on YARN

​						在Hive所在机器安装！

​						配置和导出SPARK_HOME到全局变量！

​			②安装Hive

​					安装和Spark对应版本一起编译的Hive

​					配置元数据到Mysql，修改对中文注释的支持！

​			③配置HIve on spark

```xml
<!--Spark依赖位置-->
<property>
    <name>spark.yarn.jars</name>
    <value>hdfs://hadoop102:8020/spark-jars/*</value>
</property>
  
<!--Hive执行引擎-->
<property>
    <name>hive.execution.engine</name>
    <value>spark</value>
</property>

<!--Hive和spark连接超时时间-->
<property>
    <name>hive.spark.client.connect.timeout</name>
    <value>10000ms</value>
</property>

```

​		上传纯净版的spark的jars中的jar包到/spark-jars

## 2.配置容量调度器为多队列

编辑$HADOOP_HOME/etc/hadoop/capacity-schdualer.xml

```xml
<!--
  Licensed under the Apache License, Version 2.0 (the "License");
  you may not use this file except in compliance with the License.
  You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

  Unless required by applicable law or agreed to in writing, software
  distributed under the License is distributed on an "AS IS" BASIS,
  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
  See the License for the specific language governing permissions and
  limitations under the License. See accompanying LICENSE file.
-->
<configuration>

   <!-- 容量调度器最多可以容纳多少个job-->
  <property>
    <name>yarn.scheduler.capacity.maximum-applications</name>
    <value>10000</value>
    <description>
      Maximum number of applications that can be pending and running.
    </description>
  </property>

  <!-- 当前队列中启动的MRAppMaster进程，所占用的资源可以达到队列总资源的多少
		通过这个参数可以限制队列中提交的Job数量
  -->
  <property>
    <name>yarn.scheduler.capacity.maximum-am-resource-percent</name>
    <value>0.5</value>
    <description>
      Maximum percent of resources in the cluster which can be used to run 
      application masters i.e. controls number of concurrent running
      applications.
    </description>
  </property>

  <!-- 为Job分配资源时，使用什么策略进行计算
  -->
  <property>
    <name>yarn.scheduler.capacity.resource-calculator</name>
    <value>org.apache.hadoop.yarn.util.resource.DefaultResourceCalculator</value>
    <description>
      The ResourceCalculator implementation to be used to compare 
      Resources in the scheduler.
      The default i.e. DefaultResourceCalculator only uses Memory while
      DominantResourceCalculator uses dominant-resource to compare 
      multi-dimensional resources such as Memory, CPU etc.
    </description>
  </property>

   <!-- root队列中有哪些子队列-->
  <property>
    <name>yarn.scheduler.capacity.root.queues</name>
    <value>default,hive</value>
    <description>
      The queues at the this level (root is the root queue).
    </description>
  </property>

  <!-- root队列中default队列占用的容量百分比
		所有子队列的容量相加必须等于100
  -->
  <property>
    <name>yarn.scheduler.capacity.root.default.capacity</name>
    <value>30</value>
    <description>Default queue target capacity.</description>
  </property>
  
  <property>
    <name>yarn.scheduler.capacity.root.hive.capacity</name>
    <value>70</value>
    <description>Default queue target capacity.</description>
  </property>
  
  

    <!-- 队列中用户能使用此队列资源的极限百分比
  -->
  <property>
    <name>yarn.scheduler.capacity.root.default.user-limit-factor</name>
    <value>1</value>
    <description>
      Default queue user limit a percentage from 0.0 to 1.0.
    </description>
  </property>
  
   <property>
    <name>yarn.scheduler.capacity.root.hive.user-limit-factor</name>
    <value>1</value>
    <description>
      Default queue user limit a percentage from 0.0 to 1.0.
    </description>
  </property>
 

  <!-- root队列中default队列占用的容量百分比的最大值
  -->
  <property>
    <name>yarn.scheduler.capacity.root.default.maximum-capacity</name>
    <value>100</value>
    <description>
      The maximum capacity of the default queue. 
    </description>
  </property>
  
   <property>
    <name>yarn.scheduler.capacity.root.hive.maximum-capacity</name>
    <value>100</value>
    <description>
      The maximum capacity of the default queue. 
    </description>
  </property>
  
  

    <!-- root队列中每个队列的状态
  -->
  <property>
    <name>yarn.scheduler.capacity.root.default.state</name>
    <value>RUNNING</value>
    <description>
      The state of the default queue. State can be one of RUNNING or STOPPED.
    </description>
  </property>
  
    <property>
    <name>yarn.scheduler.capacity.root.hive.state</name>
    <value>RUNNING</value>
    <description>
      The state of the default queue. State can be one of RUNNING or STOPPED.
    </description>
  </property>

  
  <!-- 限制向default队列提交的用户-->
  <property>
    <name>yarn.scheduler.capacity.root.default.acl_submit_applications</name>
    <value>*</value>
    <description>
      The ACL of who can submit jobs to the default queue.
    </description>
  </property>
  
  <property>
    <name>yarn.scheduler.capacity.root.hive.acl_submit_applications</name>
    <value>*</value>
    <description>
      The ACL of who can submit jobs to the default queue.
    </description>
  </property>
  

  <property>
    <name>yarn.scheduler.capacity.root.default.acl_administer_queue</name>
    <value>*</value>
    <description>
      The ACL of who can administer jobs on the default queue.
    </description>
  </property>
  
  <property>
    <name>yarn.scheduler.capacity.root.hive.acl_administer_queue</name>
    <value>*</value>
    <description>
      The ACL of who can administer jobs on the default queue.
    </description>
  </property>
  
 

  <property>
    <name>yarn.scheduler.capacity.node-locality-delay</name>
    <value>40</value>
    <description>
      Number of missed scheduling opportunities after which the CapacityScheduler 
      attempts to schedule rack-local containers. 
      Typically this should be set to number of nodes in the cluster, By default is setting 
      approximately number of nodes in one rack which is 40.
    </description>
  </property>

  <property>
    <name>yarn.scheduler.capacity.queue-mappings</name>
    <value></value>
    <description>
      A list of mappings that will be used to assign jobs to queues
      The syntax for this list is [u|g]:[name]:[queue_name][,next mapping]*
      Typically this list will be used to map users to queues,
      for example, u:%user:%user maps all users to queues with the same name
      as the user.
    </description>
  </property>

  <property>
    <name>yarn.scheduler.capacity.queue-mappings-override.enable</name>
    <value>false</value>
    <description>
      If a queue mapping is present, will it override the value specified
      by the user? This can be used by administrators to place jobs in queues
      that are different than the one specified by the user.
      The default is false.
    </description>
  </property>

</configuration>

```

分发到集群，重启YARN！



在hive-site.xml中添加：

```xml
<property>
    <name>mapreduce.job.queuename</name>
    <value>hive</value>
</property>

```

重启Hive!



# 四、ODS层

## 1.日志数据

1张表

### 1.1 建表

```sql
CREATE EXTERNAL TABLE ods_log (`line` string)
PARTITIONED BY (`dt` string) -- 按照时间创建分区
STORED AS -- 指定存储方式，读数据采用LzoTextInputFormat；
  INPUTFORMAT 'com.hadoop.mapred.DeprecatedLzoTextInputFormat'
  OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat'
LOCATION '/warehouse/gmall/ods/ods_log'  -- 指定数据在hdfs上的存储位置
;
```

### 1.2 导入数据

```sql
--数据保持原始，不做任何变动，直接将数据移动到hive的表目录
--load inpath : 从HDFS的一个路径移动到另一个路径
--load local inpath : 从本地的一个路径上传到HDFS
--追加写（一个批次） or 覆盖写 (全量，只影响当前分区) 
--load不走Job，不需要启动SparkAPP
load data inpath '/origin_data/gmall/log/topic_log/2020-12-08' 
overwrite into table ods_log partition(dt='2020-12-08');
```



### 1.3 创建索引

```
hadoop jar /opt/module/hadoop-3.1.3/share/hadoop/common/hadoop-lzo-0.4.20.jar com.hadoop.compression.lzo.DistributedLzoIndexer /warehouse/gmall/ods/ods_log/dt=2020-12-08
```



## 2.引号的使用

如果字符串，最外层为单引号，其中的$无法识别！

如果最外层为双引号，$可以识别！



## 3.-n

判断时，-n通常用于判断一个变量是否赋值，如果赋值，返回true，否则返回false

if [  -n "$xx" ]:   -n用来判断一个字符串是否为空！



# 五、DWD层log数据

## 1.get_json_object

```
get_json_object(json_txt, path)： 从一个JSON字符串中取出指定路径对应的数据！
核心：path怎么写？

$:  代表根对象
. : 获取子元素的操作符
[] : 获取一个数组中子元素的操作符
```



```json
select get_json_object('[{"name":"大郎","sex":"男","age":"25"},{"name":"西门庆","sex":"男","age":"47"}]','$[0].name');

select get_json_object('{"name":"大郎","sex":"男","age":"25"}','$.age');

select get_json_object('{"name":"大郎","sex":"男","age":"25","wife":[{"name":"小潘"}]}','$.wife[0].name');
```



## 2.表的存储

InputFormat: 切片的结果需要看使用什么类型的InputFormat!

| 层   | 表的存储格式 | 是否压缩 | 是否可切片（只看存储格式） |
| ---- | ------------ | -------- | -------------------------- |
| ODS  | TEXTFILE     | lzo      | lzo可切，可切              |
| DWD  | PARQUET      | lzo      | 可切                       |
|      | ORC          |          | 可切                       |

TEXTFILE： 可切！  TEXTFILE+压缩，此时应该看压缩格式是否是可切的！



PARQUET |  ORC :  可切！不管里面使用什么样的压缩都可切！

## 3.dwd_start_log

```sql
insert overwrite table dwd_start_log partition(dt='2020-12-08')
SELECT 
    get_json_object(line,'$.common.ar'),
    get_json_object(line,'$.common.ba'),
    get_json_object(line,'$.common.ch'),
    get_json_object(line,'$.common.md'),
    get_json_object(line,'$.common.mid'),
    get_json_object(line,'$.common.os'),
    get_json_object(line,'$.common.uid'),
    get_json_object(line,'$.common.vc'),
    get_json_object(line,'$.start.entry'),
    get_json_object(line,'$.start.loading_time'),
    get_json_object(line,'$.start.open_ad_id'),
    get_json_object(line,'$.start.open_ad_ms'),
    get_json_object(line,'$.start.open_ad_skip_ms'),
    get_json_object(line,'$.ts')
from ods_log 
--先过滤出当天的启动日志
where dt='2020-12-08' and get_json_object(line,'$.start') is not null;
```



## 4.dwd_page_log

```sql
insert overwrite table dwd_page_log partition(dt='2020-12-08')
-- 基于eclipse开源，使用和eclipse一样  ctrl+alt+下方向，复制当前行到下一行
SELECT 
    get_json_object(line,'$.common.ar'),
    get_json_object(line,'$.common.ba'),
    get_json_object(line,'$.common.ch'),
    get_json_object(line,'$.common.md'),
    get_json_object(line,'$.common.mid'),
    get_json_object(line,'$.common.os'),
    get_json_object(line,'$.common.uid'),
    get_json_object(line,'$.common.vc'),
    
    get_json_object(line,'$.page.during_time'),
    get_json_object(line,'$.page.item'),
    get_json_object(line,'$.page.item_type'),
    get_json_object(line,'$.page.last_page_id'),
    get_json_object(line,'$.page.page_id'),
    get_json_object(line,'$.page.sourceType'),
    
    get_json_object(line,'$.ts')
from ods_log 
--先过滤出当天的页面日志
where dt='2020-12-08' and get_json_object(line,'$.page') is not null;
```



## 5.自定义UDTF

### 5.1 定义函数

传入一个 一行一列：JSON数组的字符串

返回N行一列： 字符串



传入：[{abc},{bcd}]

返回：  {abc},

​			  {bcd }

https://cwiki.apache.org/confluence/display/Hive/DeveloperGuide+UDTF

方式： ①继承GenericUDTF

​			②实现initialize`, `process，可选实现close()

​					initialize: 由hive自己调用，告诉UDTF函数传入的参数类型！返回 object inspector，函数生成的一行数据的类型是什么，就返回这个类型对应的 object inspector！



​					process： 生成UDTF返回的每行内容，生成后，调用forward()，将结果传给其他的运算符运算！

​					最后会调用close()

​			

### 5.2 打包上传

①在$HIVE_HOME/auxlib下，上传jar包

②创建永久函数

​	注意： 函数是有库的范围，自定义的函数，在哪个库定义，只能在哪个库用！

​				或使用 库名.函数名

```
create function 函数名 as '函数全类名';

create function explode_array as 'com.atguigu.wh.functions.MyUDTF';
```



```
[{"name":"大郎","sex":"男","age":"25"},{"name":"西门庆","sex":"男","age":"47"}]
```

​			

## 6.dwd_action_log

```sql
insert overwrite table dwd_action_log partition(dt='2020-12-08')
SELECT 
    get_json_object(line,'$.common.ar'),
    get_json_object(line,'$.common.ba'),
    get_json_object(line,'$.common.ch'),
    get_json_object(line,'$.common.md'),
    get_json_object(line,'$.common.mid'),
    get_json_object(line,'$.common.os'),
    get_json_object(line,'$.common.uid'),
    get_json_object(line,'$.common.vc'),
    
    get_json_object(line,'$.page.during_time'),
    get_json_object(line,'$.page.item'),
    get_json_object(line,'$.page.item_type'),
    get_json_object(line,'$.page.last_page_id'),
    get_json_object(line,'$.page.page_id'),
    get_json_object(line,'$.page.sourceType'),
    
    get_json_object(actionJsonObject,'$.action_id'),
    get_json_object(actionJsonObject,'$.item'),
    get_json_object(actionJsonObject,'$.item_type'),
    get_json_object(actionJsonObject,'$.ts')

from ods_log 
lateral view  explode_array(get_json_object(line,'$.actions')) tmp as actionJsonObject
--先过滤出当天的活动日志
where dt='2020-12-08' and get_json_object(line,'$.actions') is not null;
```

## 7.dwd_display_log

```sql
insert overwrite table dwd_display_log partition(dt='2020-12-08')
-- 基于eclipse开源，使用和eclipse一样  ctrl+alt+下方向，复制当前行到下一行
SELECT 
    get_json_object(line,'$.common.ar'),
    get_json_object(line,'$.common.ba'),
    get_json_object(line,'$.common.ch'),
    get_json_object(line,'$.common.md'),
    get_json_object(line,'$.common.mid'),
    get_json_object(line,'$.common.os'),
    get_json_object(line,'$.common.uid'),
    get_json_object(line,'$.common.vc'),
    
    get_json_object(line,'$.page.during_time'),
    get_json_object(line,'$.page.item'),
    get_json_object(line,'$.page.item_type'),
    get_json_object(line,'$.page.last_page_id'),
    get_json_object(line,'$.page.page_id'),
    get_json_object(line,'$.page.sourceType'),
    get_json_object(line,'$.ts'),
    get_json_object(displayJsonObject,'$.displayType'),
    get_json_object(displayJsonObject,'$.item'),
    get_json_object(displayJsonObject,'$.item_type'),
    get_json_object(displayJsonObject,'$.order')
     
from ods_log 
lateral view  explode_array(get_json_object(line,'$.displays')) tmp as displayJsonObject
--先过滤出当天的曝光日志
where dt='2020-12-08' and get_json_object(line,'$.displays') is not null;
```

## 8.dwd_error_log

```sql
insert overwrite table dwd_error_log partition(dt='2020-12-08')
-- 基于eclipse开源，使用和eclipse一样  ctrl+alt+下方向，复制当前行到下一行
SELECT 
    get_json_object(line,'$.common.ar'),
    get_json_object(line,'$.common.ba'),
    get_json_object(line,'$.common.ch'),
    get_json_object(line,'$.common.md'),
    get_json_object(line,'$.common.mid'),
    get_json_object(line,'$.common.os'),
    get_json_object(line,'$.common.uid'),
    get_json_object(line,'$.common.vc'),
    get_json_object(line,'$.page.item'),
    get_json_object(line,'$.page.item_type'),
    get_json_object(line,'$.page.last_page_id'),
    get_json_object(line,'$.page.page_id'),
    get_json_object(line,'$.page.sourceType'),
      get_json_object(line,'$.start.entry'),
    get_json_object(line,'$.start.loading_time'),
    get_json_object(line,'$.start.open_ad_id'),
    get_json_object(line,'$.start.open_ad_ms'),
    get_json_object(line,'$.start.open_ad_skip_ms'),
     get_json_object(line,'$.actions'),
     get_json_object(line,'$.displays'),
    get_json_object(line,'$.ts'),
    get_json_object(line,'$.err.error_code'),
    get_json_object(line,'$.err.msg')
       
from ods_log 
--先过滤出当天的错误日志
where dt='2020-12-08' and get_json_object(line,'$.err') is not null;
```



# 六、DWD层db数据

## 1.dwd_dim_sku_info

### 1.1 建表

```sql
DROP TABLE IF EXISTS `dwd_dim_sku_info`;
CREATE EXTERNAL TABLE `dwd_dim_sku_info` (
    --ods_sku_info
    `id` string COMMENT '商品id',
    `spu_id` string COMMENT 'spuid',
    `price` decimal(16,2) COMMENT '商品价格',
    `sku_name` string COMMENT '商品名称',
    `sku_desc` string COMMENT '商品描述',
    `weight` decimal(16,2) COMMENT '重量',
    `tm_id` string COMMENT '品牌id',
    
    --ods_base_trademark
    `tm_name` string COMMENT '品牌名称',
    
    --ods_sku_info
    --ods_base_category1,ods_base_category2,ods_base_category3
    `category3_id` string COMMENT '三级分类id',
    `category2_id` string COMMENT '二级分类id',
    `category1_id` string COMMENT '一级分类id',
    `category3_name` string COMMENT '三级分类名称',
    `category2_name` string COMMENT '二级分类名称',
    `category1_name` string COMMENT '一级分类名称',
    --ods_spu_info
    `spu_name` string COMMENT 'spu名称',
    
    --ods_sku_info
    `create_time` string COMMENT '创建时间'
) COMMENT '商品维度表'
PARTITIONED BY (`dt` string)
stored as parquet
location '/warehouse/gmall/dwd/dwd_dim_sku_info/'
tblproperties ("parquet.compression"="lzo");

```

主体：  商品维度表，以商品为主体，顺便查询出商品的其他信息！

ods_sku_info 主表 left join 其他表

### 1.2 导入

```sql
 set hive.exec.dynamic.partition.mode=nonstrict;

-- 静态分区：在插入数据时，不仅指定分区字段名称，还指定分区字段的值
-- 动态分区：在插入数据时，只指定分区字段名称，将查询的最后一个字段的值，作为分区字段的值
INSERT overwrite table dwd_dim_sku_info partition(dt)
SELECT 
    t1.id, spu_id, price, sku_name, sku_desc, weight, t1.tm_id, tm_name, 
    t1.category3_id, 
    t5.category2_id,
    t4.category1_id, category3_name, category2_name, category1_name, 
    spu_name, create_time,dt
from
(SELECT 
    *
from ods_sku_info 
where dt = '2020-12-08' ) t1
left join
(SELECT 
    tm_id ,tm_name
from ods_base_trademark 
where dt = '2020-12-08' ) t2
on t1.tm_id = t2.tm_id
left join
(SELECT 
    id ,spu_name
from ods_spu_info 
where dt = '2020-12-08' ) t3
on t1.spu_id = t3.id
left join
(SELECT 
    id  category3_id,name category3_name, category2_id
from ods_base_category3
where dt = '2020-12-08' ) t6
on t1.category3_id = t6.category3_id
left join
(SELECT 
    id  category2_id,name category2_name, category1_id
from ods_base_category2
where dt = '2020-12-08' ) t5
on t6.category2_id = t5.category2_id
left join
(SELECT 
    id  category1_id,name category1_name
from ods_base_category1 
where dt = '2020-12-08' ) t4
on t5.category1_id = t4.category1_id
```



## 2.dwd_dim_coupon_info

### 2.1 建表

```sql
drop table if exists dwd_dim_coupon_info;
create external table dwd_dim_coupon_info(
    --ods_coupon_info
    `id` string COMMENT '购物券编号',
    `coupon_name` string COMMENT '购物券名称',
    `coupon_type` string COMMENT '购物券类型 1 现金券 2 折扣券 3 满减券 4 满件打折券',
    `condition_amount` decimal(16,2) COMMENT '满额数',
    `condition_num` bigint COMMENT '满件数',
    `activity_id` string COMMENT '活动编号',
    `benefit_amount` decimal(16,2) COMMENT '减金额',
    `benefit_discount` decimal(16,2) COMMENT '折扣',
    `create_time` string COMMENT '创建时间',
    `range_type` string COMMENT '范围类型 1、商品 2、品类 3、品牌',
    `spu_id` string COMMENT '商品id',
    `tm_id` string COMMENT '品牌id',
    `category3_id` string COMMENT '品类id',
    `limit_num` bigint COMMENT '最多领用次数',
    `operate_time`  string COMMENT '修改时间',
    `expire_time`  string COMMENT '过期时间'
) COMMENT '优惠券维度表'
PARTITIONED BY (`dt` string)
stored as parquet
location '/warehouse/gmall/dwd/dwd_dim_coupon_info/'
tblproperties ("parquet.compression"="lzo");

```

主体：优惠券

### 2.2 导入

```sql
INSERT overwrite table dwd_dim_coupon_info partition(dt)
SELECT 
    *
from ods_coupon_info 
where dt = '2020-12-08' 
```



## 3.dwd_dim_activity_info

### 3.1 建表

```sql
drop table if exists dwd_dim_activity_info;
create external table dwd_dim_activity_info(
    `id` string COMMENT '编号',
    `activity_name` string  COMMENT '活动名称',
    `activity_type` string  COMMENT '活动类型',
    `start_time` string  COMMENT '开始时间',
    `end_time` string  COMMENT '结束时间',
    `create_time` string  COMMENT '创建时间'
) COMMENT '活动信息表'
PARTITIONED BY (`dt` string)
stored as parquet
location '/warehouse/gmall/dwd/dwd_dim_activity_info/'
tblproperties ("parquet.compression"="lzo");

```

主体：活动

### 3.2 导入

```sql
INSERT overwrite table dwd_dim_activity_info partition(dt)
SELECT 
    *
from ods_activity_info 
where dt = '2020-12-08' ;
```



## 4.dwd_dim_base_province

### 4.1 建表

```sql
DROP TABLE IF EXISTS `dwd_dim_base_province`;
CREATE EXTERNAL TABLE `dwd_dim_base_province` (
    `id` string COMMENT 'id',
    `province_name` string COMMENT '省市名称',
    `area_code` string COMMENT '地区编码',
    `iso_code` string COMMENT 'ISO编码',
    `region_id` string COMMENT '地区id',
    `region_name` string COMMENT '地区名称'
) COMMENT '地区维度表'
stored as parquet
location '/warehouse/gmall/dwd/dwd_dim_base_province/'
tblproperties ("parquet.compression"="lzo");

```

主体：省份  

省份表 left join 地区表

### 4.2 导入

注意：别名不能和hive和shell中的关键字冲突！冲突可以使用``

```sql
INSERT overwrite table dwd_dim_base_province 
SELECT 
    t1.id ,name,area_code,iso_code,region_id,region_name  
from ods_base_province t1 left join ods_base_region t2
on t1.region_id = t2.id 
```

## 5.dwd_dim_date_info

### 5.1 建表

```sql
DROP TABLE IF EXISTS `dwd_dim_date_info`;
CREATE EXTERNAL TABLE `dwd_dim_date_info`(
    `date_id` string COMMENT '日',
    `week_id` string COMMENT '周',
    `week_day` string COMMENT '周的第几天',
    `day` string COMMENT '每月的第几天',
    `month` string COMMENT '第几月',
    `quarter` string COMMENT '第几季度',
    `year` string COMMENT '年',
    `is_workday` string COMMENT '是否是周末',
    `holiday_id` string COMMENT '是否是节假日'
) COMMENT '时间维度表'
row format delimited fields terminated by '\t'
location '/warehouse/gmall/dwd/dwd_dim_date_info/';
```

### 5.2 导入

```

```



## 6.事实表在导入时建模策略

①挑选感兴趣的事实业务

②确认粒度

③挑选维度

④选择度量

## 7.dwd_fact_order_detail

### 7.1建表

dwd_fact_order_detail：  一笔订单的一个商品是一条！

```sql
create external table dwd_fact_order_detail (
    --ods_order_detail
    `id` string COMMENT '订单编号',
    `order_id` string COMMENT '订单号',
    `user_id` string COMMENT '用户id',
    `sku_id` string COMMENT 'sku商品id',
    `sku_name` string COMMENT '商品名称',
    `order_price` decimal(16,2) COMMENT '商品价格',
    `sku_num` bigint COMMENT '商品数量',
    `create_time` string COMMENT '创建时间',
    `province_id` string COMMENT '省份ID',
    `source_type` string COMMENT '来源类型',
    `source_id` string COMMENT '来源编号',
    -- 从订单表查询，计算得到   以下字段应该用户下单时，由应用服务端App计算得到，将计算的结果写入到mysql数据库，从mysql 数据 同步到 ods层，直接从ods层查询即可，不需要计算！
    `original_amount_d` decimal(20,2) COMMENT '原始价格分摊',
    `final_amount_d` decimal(20,2) COMMENT '购买价格分摊',
    `feight_fee_d` decimal(20,2) COMMENT '分摊运费',
    `benefit_reduce_amount_d` decimal(20,2) COMMENT '分摊优惠'
) COMMENT '订单明细事实表表'
PARTITIONED BY (`dt` string)
stored as parquet
location '/warehouse/gmall/dwd/dwd_fact_order_detail/'
tblproperties ("parquet.compression"="lzo");


```

主体： 一条订单中的详情信息



| orderId | 商品ID | 单价 | 数量 | 购买价格分摊 | 总额 |
| ------- | ------ | ---- | ---- | ------------ | ---- |
| 1       | 2      | 20.3 |      | 20.33        | 61   |
| 1       | 2      | 20.3 |      | 20.33        |      |
| 1       | 3      | 20.4 |      | 20.33        |      |
| 2       | 4      |      |      |              |      |



### 7.2 导入

```sql
insert overwrite table dwd_fact_order_detail partition(dt='2020-12-08')
SELECT 
     id, order_id, user_id, sku_id, sku_name, order_price, 
    sku_num, create_time, province_id, source_type, source_id,
    original_amount_d,
    --所有分摊的涉及到四舍五入的，都需要判断当前商品是否需要参与多退少补   在订单中排第一
    if(rn = 1, final_amount_d +  final_total_amount - sum_final_amount_d ,final_amount_d)  final_amount_d,
    if(rn = 1, feight_fee_d +  feight_fee - sum_feight_fee_d ,feight_fee_d)  feight_fee_d,
    if(rn = 1, benefit_reduce_amount_d +  benefit_reduce_amount - sum_benefit_reduce_amount_d ,benefit_reduce_amount_d)  benefit_reduce_amount_d
from
(SELECT 
--求出每笔订单每个商品的详情，及所在订单的总金额，总优惠金额，运费和原始金额
    t1.*,province_id,
    order_price * sku_num original_amount_d,
    round(order_price * sku_num / original_total_amount * (final_total_amount - feight_fee),2) final_amount_d,
    round(order_price * sku_num / original_total_amount * feight_fee,2) feight_fee_d,
   round(order_price * sku_num / original_total_amount *  benefit_reduce_amount,2)  benefit_reduce_amount_d,
   -- 同一笔订单中，所有商品分摊金额的排名，分摊多的排第一
   ROW_NUMBER() over(PARTITION  by order_id  order by (order_price * sku_num ) desc) rn ,
   -- 所有分摊的优惠的总额
   sum(round(order_price * sku_num / original_total_amount * (final_total_amount - feight_fee),2)) over(PARTITION  by order_id) sum_final_amount_d,
    sum(round(order_price * sku_num / original_total_amount * feight_fee,2)) over(PARTITION  by order_id)  sum_feight_fee_d,
    sum(round(order_price * sku_num / original_total_amount *  benefit_reduce_amount,2)) over(PARTITION  by order_id) sum_benefit_reduce_amount_d,
    benefit_reduce_amount,final_total_amount,original_total_amount,feight_fee
from
(SELECT 
    *
from ods_order_detail 
where dt='2020-12-08') t1
left join
(
SELECT 
    id,province_id,benefit_reduce_amount,final_total_amount,original_total_amount,feight_fee
from ods_order_info 
where dt='2020-12-08'
) t2
on t1.order_id = t2.id  ) t3;
```



## 8.dwd_fact_payment_info

### 8.1 建表

```sql
drop table if exists dwd_fact_payment_info;
create external table dwd_fact_payment_info (
    --ods_payment_info
    `id` string COMMENT 'id',
    `out_trade_no` string COMMENT '对外业务编号',
    `order_id` string COMMENT '订单编号',
    `user_id` string COMMENT '用户编号',
    `alipay_trade_no` string COMMENT '支付宝交易流水编号',
    `payment_amount`    decimal(16,2) COMMENT '支付金额',
    `subject`         string COMMENT '交易内容',
    `payment_type` string COMMENT '支付类型',
    `payment_time` string COMMENT '支付时间',
    --ods_order_info
    `province_id` string COMMENT '省份ID'
) COMMENT '支付事实表表'
PARTITIONED BY (`dt` string)
stored as parquet
location '/warehouse/gmall/dwd/dwd_fact_payment_info/'
tblproperties ("parquet.compression"="lzo");

```

主体： 每笔订单的支付信息

粒度：一条订单的支付信息



跨天支付： 2020-12-08 23:59:59 下单

a)  

mysql:

order_info生成一条记录

| id   | create_time         | operate_time | order_status |
| ---- | ------------------- | ------------ | ------------ |
| 1    | 2020-12-08 23:59:59 |              | 1001         |



order_detail

| order_id | sku_id | create_time         |
| -------- | ------ | ------------------- |
| 1        | 2      | 2020-12-08 23:59:59 |



2020-12-09 00:01:01

mysql:

order_info修改一条记录



| id   | create_time         | operate_time        | order_status |
| ---- | ------------------- | ------------------- | ------------ |
| 1    | 2020-12-08 23:59:59 | 2020-12-09 00:01:01 | 1002         |



payment_info

| id   | order_id | payment_time        |
| ---- | -------- | ------------------- |
| 1001 | 1        | 2020-12-09 00:01:01 |



需求： 

求12-09支付的所有订单的商品信息：

​		ods_payment_info  当天(12-09）的分区的数据  join   ods_order_detail (当天 和 前一天)



求12-09支付的所有订单的订单信息：

​		ods_payment_info  当天(12-09）的分区的数据  join  ods_order_info(当天 )



总结： 跨天支付后，如果要取 支付订单中商品的信息，需要join ods_order_detail (当天 和 前一天)，如果只取订单的信息，只需要  join  ods_order_info(当天 )！





dwd层的跨天支付： 如果求 2020-12-08日支付的订单，从dwd_fact_order_info取，因为涉及到跨天支付，要取

dwd_fact_order_info/2020-12-08 和 2020-12-07



### 8.2 导入

```sql
INSERT overwrite table dwd_fact_payment_info partition(dt='2020-12-08')
SELECT 
    t1.id, out_trade_no, order_id, user_id, alipay_trade_no,
    total_amount payment_amount, subject, payment_type, payment_time, province_id
from
(SELECT 
    *
from ods_payment_info 
where dt='2020-12-08')  t1
left join
(
select
    id ,province_id
from ods_order_info 
where dt='2020-12-08'
) t2
on  t1.order_id = t2.id;
```



## 9.dwd_fact_order_refund_info

### 9.1 建表

```sql
drop table if exists dwd_fact_order_refund_info;
create external table dwd_fact_order_refund_info(
    --ods_order_refund_info
    `id` string COMMENT '编号',
    `user_id` string COMMENT '用户ID',
    `order_id` string COMMENT '订单ID',
    `sku_id` string COMMENT '商品ID',
    `refund_type` string COMMENT '退款类型',
    `refund_num` bigint COMMENT '退款件数',
    `refund_amount` decimal(16,2) COMMENT '退款金额',
    `refund_reason_type` string COMMENT '退款原因类型',
    `create_time` string COMMENT '退款时间'
) COMMENT '退款事实表'
PARTITIONED BY (`dt` string)
stored as parquet
location '/warehouse/gmall/dwd/dwd_fact_order_refund_info/'
tblproperties ("parquet.compression"="lzo");

```

主体： 退款的事实

### 9.2 导入

```sql
INSERT overwrite table dwd_fact_order_refund_info partition(dt)
SELECT 
    *
from ods_order_refund_info 
where dt='2020-12-08';
```



## 10.dwd_fact_comment_info

### 10.1 建表

```sql
drop table if exists dwd_fact_comment_info;
create external table dwd_fact_comment_info(
    `id` string COMMENT '编号',
    `user_id` string COMMENT '用户ID',
    `sku_id` string COMMENT '商品sku',
    `spu_id` string COMMENT '商品spu',
    `order_id` string COMMENT '订单ID',
    `appraise` string COMMENT '评价',
    `create_time` string COMMENT '评价时间'
) COMMENT '评价事实表'
PARTITIONED BY (`dt` string)
stored as parquet
location '/warehouse/gmall/dwd/dwd_fact_comment_info/'
tblproperties ("parquet.compression"="lzo");

```

主体：一条评论的事实

### 10.2 导入数据

```
INSERT overwrite table dwd_fact_comment_info partition(dt)
SELECT 
    *
from ods_comment_info 
where dt='2020-12-08';
```



## 11.事务型事实表导入总结

事务型事实表 中的事实记录只会新增，不会修改！ 从Mysql同步的数据，都是增量同步！ 导入到ODS层！



导入DWD层，只需要从ODS层，取出当天导入的新增的数据即可！



## 12.周期型快照事实表

周期型快照事实表 用来记录一个事实在某个周期结束时的快照状态！

ODS层：全量同步

​		好处：方便

​		坏处：同步时间长，存在数据冗余占用（缩短数据存储的周期）！

## 13.dwd_fact_cart_info

### 13.1 建表

统计的不是用户将商品加入购物车的行为记录！

关心： 截至到某个日期，例如截至到2020-08-08，此时用户的购物车(还有什么)状态！

```sql
drop table if exists dwd_fact_cart_info;
create external table dwd_fact_cart_info(
    `id` string COMMENT '编号',
    `user_id` string  COMMENT '用户id',
    `sku_id` string  COMMENT 'skuid',
    `cart_price` string  COMMENT '放入购物车时价格',
    `sku_num` string  COMMENT '数量',
    `sku_name` string  COMMENT 'sku名称 (冗余)',
    `create_time` string  COMMENT '创建时间',
    `operate_time` string COMMENT '修改时间',
    `is_ordered` string COMMENT '是否已经下单。1为已下单;0为未下单',
    `order_time` string  COMMENT '下单时间',
    `source_type` string COMMENT '来源类型',
    `srouce_id` string COMMENT '来源编号'
) COMMENT '加购事实表'
PARTITIONED BY (`dt` string)
stored as parquet
location '/warehouse/gmall/dwd/dwd_fact_cart_info/'
tblproperties ("parquet.compression"="lzo");

```

ODS是全量同步： 将购物车，从使用APP起，到同步日期那天，所有用户购物车的状态全部同步！



----------------------------------------------------------------

如果数据量大： ODS应该同步新增和变化的数据！

​			2020-08-08 同步，同步的是2020-08-08新加入购物车的记录和对购物车的数据进行修改的记录！



​						 dwd： 截至到2020-08-08，所有用户购物车的状态！

​										需要先取出 2020-08-07之前，DWD层所有购物车的状态。  在DWD层 2020-08-07分区中！

​										和

​										ODS层 2020-08-08新增和变化的数据   进行合并，取时间最新的！将合并后的数据insert overwrite到表中，覆盖！







### 13.2 导入

```sql
INSERT overwrite table dwd_fact_cart_info partition(dt)
SELECT 
    *
from ods_cart_info 
where dt='2020-12-08';
```



## 14.dwd_fact_favor_info

### 14.1 建表

```sql
drop table if exists dwd_fact_favor_info;
create external table dwd_fact_favor_info(
    `id` string COMMENT '编号',
    `user_id` string  COMMENT '用户id',
    `sku_id` string  COMMENT 'skuid',
    `spu_id` string  COMMENT 'spuid',
    `is_cancel` string  COMMENT '是否取消',
    `create_time` string  COMMENT '收藏时间',
    `cancel_time` string  COMMENT '取消时间'
) COMMENT '收藏事实表'
PARTITIONED BY (`dt` string)
stored as parquet
location '/warehouse/gmall/dwd/dwd_fact_favor_info/'
tblproperties ("parquet.compression"="lzo");

```



### 14.2 导入

```sql
INSERT overwrite table dwd_fact_favor_info partition(dt)
SELECT 
    *
from ods_favor_info 
where dt='2020-12-08'
```



逻辑清晰： 数据流向 搞清楚（善于画图）！

​				   掌握场景的业务处理能力（函数，自定义函数）！

​					总结：  表的同步方式！

​									建模时，如何建！

​									如何基于建模优化同步方式！



## 15.累积型快照事实表

累积型快照事实表通常也会使用分区表，但是分区字段会使用 事实生命周期中，最早的时间！

事实发生的时间，作为分区字段！



## 16.dwd_fact_coupon_use

### 16.1 建表

所有累积型快照事实表，都会将事实生命周期起始的时间作为分区字段！

ods_coupon_use :  每日新增和变化！

```sql
drop table if exists dwd_fact_coupon_use;
create external table dwd_fact_coupon_use(
    --ods_coupon_use
    `id` string COMMENT '编号',
    `coupon_id` string  COMMENT '优惠券ID',
    `user_id` string  COMMENT 'userid',
    `order_id` string  COMMENT '订单id',
    `coupon_status` string  COMMENT '优惠券状态',
    `get_time` string  COMMENT '领取时间',
    `using_time` string  COMMENT '使用时间(下单)',
    `used_time` string  COMMENT '使用时间(支付)'
) COMMENT '优惠券领用事实表'
PARTITIONED BY (`dt` string) -- 就是优惠券的领取时间
stored as parquet
location '/warehouse/gmall/dwd/dwd_fact_coupon_use/'
tblproperties ("parquet.compression"="lzo");

```

主体：优惠券领取的事实！

粒度： 一条优惠券领取的事实，在表中应该是一行！



ods_coupon_use: 一条优惠券领取的事实，可能是1行或3行

有一个优惠券，在2020-12-08被领取，在2020-12-09日被下单，在2020-12-10日被支付！

在mysql的coupon_use表中：

1条



在ods层的ods_coupon_use，同一条数据，被同步到3个不同的分区中

ods_coupon_use/2020-12-08/ 1,get_time-2020-12-08, using_time=null,used_time=null

ods_coupon_use/2020-12-09/ 1,get_time-2020-12-08,using_time=2020-12-09,used_time=null

ods_coupon_use/2020-12-10/ 1,get_time-2020-12-08,using_time=2020-12-09,used_time=2020-12-10



将数据同步到dwd层： 1天一同步

dwd_fact_coupon_use  在2020-12-08   时，要插入1,get_time-2020-12-08, using_time=null,used_time=null



dwd_fact_coupon_use 在2020-12-09时  ，需要更新  1,get_time-2020-12-08, using_time=2020-12-09,used_time=null



更新：使用insert overwrite ,判断overwrite 的是整个表目录还是只覆盖对应的分区目录！

### 16.2 导入数据的过程

第一次向dwd_fact_conpon_use导入的数据：2020-08-08

| 优惠券ID | 用户ID | 领取时间   | 下单使用时间 | 支付使用时间 | dt         |
| -------- | ------ | ---------- | ------------ | ------------ | ---------- |
| 1        | 1      | 2020-08-08 | null         | null         | 2020-08-08 |
| 2        | 1      | 2020-08-08 | null         | null         | 2020-08-08 |



第二次向dwd_fact_conpon_use导入数据：2020-08-09

| 优惠券ID | 用户ID | 领取时间   | 下单使用时间 | 支付使用时间 | dt         |
| -------- | ------ | ---------- | ------------ | ------------ | ---------- |
| 1        | 1      | 2020-08-08 | 2020-08-09   | 2020-08-09   | 2020-08-08 |
| 3        | 1      | 2020-08-09 | null         | null         | 2020-08-09 |



使用 insert overwrite table xxx partition(dt='"2020-08-08"'),插入以下数据：

| 优惠券ID | 用户ID | 领取时间   | 下单使用时间 | 支付使用时间 | dt         |
| -------- | ------ | ---------- | ------------ | ------------ | ---------- |
| 1        | 1      | 2020-08-08 | 2020-08-09   | 2020-08-09   | 2020-08-08 |
| 2        | 1      | 2020-08-08 | null         | null         | 2020-08-08 |

使用 insert overwrite table xxx partition(dt='"2020-08-09"'),插入以下数据：

| 优惠券ID | 用户ID | 领取时间   | 下单使用时间 | 支付使用时间 | dt         |
| -------- | ------ | ---------- | ------------ | ------------ | ---------- |
| 3        | 1      | 2020-08-09 | null         | null         | 2020-08-09 |



总结： 累积的分区的表，操作步骤：

​				①查询出，2020-08-09导入的数据中，哪些分区的数据，发生了变化(2020-08-08)

​								从ods_coupon_use中获取 get_time < 今天的数据的日期即可！

​				②将发生变化的分区中的老的数据查询出来

| 优惠券ID | 用户ID | 领取时间   | 下单使用时间 | 支付使用时间 | dt         |
| -------- | ------ | ---------- | ------------ | ------------ | ---------- |
| 1        | 1      | 2020-08-08 | null         | null         | 2020-08-08 |
| 2        | 1      | 2020-08-08 | null         | null         | 2020-08-08 |

​				③和今天新导入的数据进行  混合

| 优惠券ID | 用户ID | 领取时间   | 下单使用时间 | 支付使用时间 | dt         |
| -------- | ------ | ---------- | ------------ | ------------ | ---------- |
| 1        | 1      | 2020-08-08 | 2020-08-09   | 2020-08-09   | 2020-08-08 |
| 2        | 1      | 2020-08-08 | null         | null         | 2020-08-08 |



​			混合后：



| 优惠券ID | 用户ID | 领取时间   | 下单使用时间 | 支付使用时间 | dt         |                          |
| -------- | ------ | ---------- | ------------ | ------------ | ---------- | ------------------------ |
| 1        | 1      | 2020-08-08 | 2020-08-09   | 2020-08-09   | 2020-08-08 | 旧数据和新数据的交集     |
| 2        | 1      | 2020-08-08 | null         | null         | 2020-08-08 | 旧的，今天没有改变的数据 |
| 3        | 1      | 2020-08-09 | null         | null         | 2020-08-09 | 今天新导入的数据         |

​			④将混合的数据，插入到对应的分区





总结精简版： ①查出老数据中哪些分区的数据，在今天发生了变化

​						②根据变化的分区日期，查询老数据  old

​						③查询当天新的数据  new

​						④old full join new 

​								新老交替

​						⑤将合并后的结果写入分区！

### 16.3 导入

```sql
insert overwrite table dwd_fact_coupon_use partition(dt)
SELECT 
    --以新换旧
    nvl(new.id,old.id) id,
    nvl(new.coupon_id,old.coupon_id) coupon_id,
    nvl(new.user_id,old.user_id) user_id,
    nvl(new.order_id,old.order_id) order_id,
    nvl(new.coupon_status,old.coupon_status) coupon_status,
    nvl(new.get_time,old.get_time) get_time,
    nvl(new.using_time,old.using_time) using_time,
    nvl(new.used_time,old.used_time) used_time,
    date_format(nvl(new.get_time,old.get_time),'yyyy-MM-dd') dt
from
--查询需要被覆盖的分区的原有的数据 old
(SELECT 
    *
from dwd_fact_coupon_use 
-- 分区目录就是优惠券被领取的时间
where dt in
(--判断2020-12-09当天导入的数据中，哪些dwd层的分区需要被覆盖
SELECT 
     date_format(get_time,'yyyy-MM-dd') 
from ods_coupon_use 
where dt='2020-12-09'
-- 只有dwd层的历史分区的数据需要被覆盖，历史分区时间 < 当天
and date_format(get_time,'yyyy-MM-dd') < '2020-12-09'  ) ) old
FULL  JOIN 
--查询哪些是今天需要导入的新的数据
(SELECT 
     *
from ods_coupon_use 
where dt='2020-12-09') new
on old.id=new.id
```



## 17.dwd_fact_order_info

### 17.1 建表

dwd_fact_order_info 按照create_time分区！

```sql
drop table if exists dwd_fact_order_info;
create external table dwd_fact_order_info (
    --ods_order_info
    `id` string COMMENT '订单编号',
    `order_status` string COMMENT '订单状态',
    `user_id` string COMMENT '用户id',
    `out_trade_no` string COMMENT '支付流水号',
    `create_time` string COMMENT '创建时间(未支付状态)',
    --ods_order_status_log
    `payment_time` string COMMENT '支付时间(已支付状态)',
    `cancel_time` string COMMENT '取消时间(已取消状态)',
    `finish_time` string COMMENT '完成时间(已完成状态)',
    `refund_time` string COMMENT '退款时间(退款中状态)',
    `refund_finish_time` string COMMENT '退款完成时间(退款完成状态)',
    
    `province_id` string COMMENT '省份ID',
    --ods_activity_order
    `activity_id` string COMMENT '活动ID',
    `original_total_amount` decimal(16,2) COMMENT '原价金额',
    `benefit_reduce_amount` decimal(16,2) COMMENT '优惠金额',
    `feight_fee` decimal(16,2) COMMENT '运费',
    `final_total_amount` decimal(16,2) COMMENT '订单金额'
) COMMENT '订单事实表'
-- 订单创建的时间 create_time
PARTITIONED BY (`dt` string) 
stored as parquet
location '/warehouse/gmall/dwd/dwd_fact_order_info/'
tblproperties ("parquet.compression"="lzo");

```

主体：订单

粒度： 

目标表：

​		dwd_fact_order_info  ： 一笔订单是一行

源表：

​		ods_activity_order：  一笔参与活动的订单是一行

​        ods_order_info ：  一笔订单是一行

​	  ods_order_status_log： 一笔订单的一个状态是一行



 ods_order_info left join  ods_activity_order  left join    ods_order_status_log

join之前，需要将 ods_order_status_log的粒度进行聚合，将粒度变为一笔订单是一行！

### 17.2相关函数

concat：  多个字符串进行拼接！ 注意事项： 传入的参数中不能有NULL，一旦有一个为NULL，返回NULL

```
concat(str1, str2, ... strN)
```

concat_ws:  将字符串或数组(字符串)中的每个字符串使用指定的分隔符进行拼接！NULL值不影响，会自动忽略！

```
concat_ws(separator, [string | array(string)]+)
```

是一个聚合函数！



行转列：  一列多行，转一列一行！  聚合函数！

collect_list:(列名) : 将这列数据，全部加入到List中返回，允许重复！

collect_set(列名): 将这列数据，全部加入到Set中返回，不允许重复！



str_to_map: text就是一个字符串，delimiter1代表分割entry的分隔符！delimiter2代表entry中分割k-v的分隔符！如果有同名的k-v，后面的会覆盖之前的！

```
str_to_map(text, delimiter1, delimiter2)
```



### 17.3 导入数据的方式

①求哪些分区的老的数据发生了变化

②求老的分区的数据  old

③求新导入的数据  new

④old full join new

⑤将Join后的数据，新老交替，插入对应的分区！

 



### 17.4 导入

```sql
insert overwrite table dwd_fact_order_info partition(dt)
SELECT 
    --以新换旧
    nvl(new.id,old.id) id,
    nvl(new.order_status,old.order_status) order_status,
    nvl(new.user_id,old.user_id) user_id,
    nvl(new.out_trade_no,old.out_trade_no) out_trade_no,
    nvl(new.create_time,old.create_time) create_time,
    nvl(timesMap['1002'],old.payment_time) payment_time,
    nvl(timesMap['1003'],old.cancel_time) cancel_time,
    nvl(timesMap['1004'],old.finish_time) finish_time,
    nvl(timesMap['1005'],old.refund_time) refund_time,
    nvl(timesMap['1006'],old.refund_finish_time) refund_finish_time,
    nvl(new.province_id,old.province_id) province_id,
    nvl(new.activity_id,old.activity_id) activity_id,
    nvl(new.original_total_amount,old.original_total_amount) original_total_amount,
    nvl(new.benefit_reduce_amount,old.benefit_reduce_amount) benefit_reduce_amount,
    nvl(new.feight_fee,old.feight_fee) feight_fee,
    nvl(new.final_total_amount,old.final_total_amount) final_total_amount,
    date_format(nvl(new.create_time,old.create_time),'yyyy-MM-dd') dt
from
(
--从dwd层查询受影响的分区的所有的数据  old
SELECT 
    *
from dwd_fact_order_info 
where dt in
(--求dwd层受影响的分区有哪些
SELECT 
    date_format(create_time,'yyyy-MM-dd') 
from ods_order_info 
where dt='2020-12-09' and
-- 只有历史分区会受影响
date_format(create_time,'yyyy-MM-dd') <  '2020-12-09') ) old
full join
(
    select
        *
    from
    --今天修改和新增订单的 各种操作时间，及是否参加活动，及活动ID
   (select
        *
   from ods_order_info 
    where dt='2020-12-09' ) t1
    left join 
    (
            --求2020-12-09每笔订单的操作时间
        SELECT 
            --要的是  order_id, Map[orderStatus,operate_time]操作时间的集合
            order_id ,str_to_map(concat_ws(',',collect_list(concat(order_status ,':', operate_time )))) timesMap
        from ods_order_status_log 
        where dt='2020-12-09'
        GROUP by order_id 
    ) t2
    on t1.id = t2.order_id
    left join
    (
            --求2020-12-09 参与活动的订单
        SELECT 
            order_id ,activity_id 
        from ods_activity_order 
        where dt='2020-12-09'
    ) t3
    on t1.id = t3.order_id   
) new
on old.id = new.id;
```



## 18.用户维度表（拉链表）

### 18.1 简介

​              如果对于一张事实表，希望追踪这个事实在某些阶段的变化信息，可以使用累积型快照事实表！

​				如果是一张维度表，希望追踪维度表中每条记录变化的状态，可以使用拉链表！



​			对于事实表来说，除了事务型事实表，表中的数据在生命周期没有结束时，有可能经常发生变化。

​			维度表用来描述事实，维度表的特点是数据可能会增加，但是已经增加的数据，不经常变化（修改）！



作用： 用来追踪维度表中记录的变化状态的！

适用场景： 缓慢变化维度表！



拉链表： 拉：拉取数据

​				链：一条数据，在表中类似一个链条状！



形式：  分区表 or 全量表 ？

​			 分区表主要是为了分散数据！将一个表的数据，分散到多个分区目录中！在查询时，可以根据分区字段进行过滤，过滤后，直接从某个分区目录中取数据，而不是全表扫描！

​			 如何选取分区字段？ 根据查询习惯，选其他的对查询的过滤帮助不大！

​			 

​			维度表数据不多，可以使用全量表！



使用：  在查询时，使用记录的生效日期和结束日期作为条件进行 过滤！

​			例如：   select * from xxx where start_date <= 2020-08-16  and end_date >= 2020-08-16

​			取出 2020-8-18的数据全量切片：   

​								查询2020-8-16 及之前生效的，在2020-8-16日及以后失效的数据！



导入：  ①当天导入的新数据， start_date=当天日期， end_date=9999-99-99

​				②没有发生变化的老数据，原封不动

​				③发生变化的老数据，将end_date=当天日期-1



### 18.2 建表

```sql
drop table if exists dwd_dim_user_info_his;
create external table dwd_dim_user_info_his(
    `id` string COMMENT '用户id',
    `name` string COMMENT '姓名', 
    `birthday` string COMMENT '生日',
    `gender` string COMMENT '性别',
    `email` string COMMENT '邮箱',
    `user_level` string COMMENT '用户等级',
    `create_time` string COMMENT '创建时间',
    `operate_time` string COMMENT '操作时间',
    `start_date`  string COMMENT '有效开始日期',
    `end_date`  string COMMENT '有效结束日期'
) COMMENT '用户拉链表'
stored as parquet
location '/warehouse/gmall/dwd/dwd_dim_user_info_his/'
tblproperties ("parquet.compression"="lzo");

```

### 18.3 导入

```sql
INSERT overwrite table dwd_dim_user_info_his 
-- 查询表中原有的记录，将需要修改的进行修改
SELECT 
	old.id,
	old.name ,
	old.birthday ,
	old .gender,
	old.email ,
	old.user_level ,
	old.create_time ,
	old.operate_time ,
	old.start_date ,
	-- 只修改结束日期是9999-99-99且，在右表有数据的记录
	if(new.id is not null and end_date ='9999-99-99',date_sub(dt,1),old.end_date ) 
from dwd_dim_user_info_his old
left join
(SELECT 
	*
from ods_user_info 
where dt='2020-12-09') new
on old.id  = new.id
UNION  all
-- 当天新增的数据，拼接上起始日期和结束日期
SELECT 
	*,'9999-99-99'
from ods_user_info 
where dt='2020-12-09';
```



# 七、DWS层

## 1.常用函数

```
next_day(start_date, day_of_week) - Returns the first date which is later than start_date and named as indicated
```

next_day返回的是，传入的日期后的第一个 周几

day_of_week： 可以写全拼，还可以写简写

​								Monday, Mon, Mo



## 2.特点

①紧密贴合需求，需求要什么字段，就提供什么字段

②将需求进行分类，按照主题分类，划分 设备，用户，商品，活动，地区五大类

③表都是宽表（既有dwd事实表的字段，还有dwd层维度表的字段），都是分区（每天一个分区）表！



## 3.dws_uv_detail_daycount

### 3.1 建表

用户： 设备

活跃： 一台设备，只要启动了APP，就算一个活跃用户！启动一次，称为活跃1次！

粒度： 一个设备是一条

```sql
drop table if exists dws_uv_detail_daycount;
create external table dws_uv_detail_daycount
(
   -- dwd_start_log
    `mid_id` string COMMENT '设备id', 
    -- 使用聚集函数  concat_ws(collect_set())
    `brand` string COMMENT '手机品牌', 
    `model` string COMMENT '手机型号', 
    `login_count` bigint COMMENT '活跃次数',
    --dwd_page_log   named_struct构造结构体
    --   将每个设备对每个页面的访问次数统计出来，再封装为struct，再按照 mid_id分组，将同一个设备的多个struct封装为array
    `page_stats` array<struct<page_id:string,page_count:bigint>> COMMENT '页面访问统计'
) COMMENT '每日设备行为表'
partitioned by(dt string)
stored as parquet
location '/warehouse/gmall/dws/dws_uv_detail_daycount'
tblproperties ("parquet.compression"="lzo");

```

### 3.2 导入

```sql
--今天所有的设备的启动统计和访问页面统计
INSERT overwrite table dws_uv_detail_daycount partition(dt='2020-12-08')
SELECT 
	t1.mid_id,brand, model, login_count, 
	page_stats
from
(SELECT 
	mid_id,
	concat_ws('|',COLLECT_set(brand)) brand,
	concat_ws('|',COLLECT_set(model)) model,
	count(*) login_count
--粒度： 一条启动信息是一条
from dwd_start_log 
where dt='2020-12-08'
GROUP by mid_id ) t1
left join
(SELECT 
	mid_id, collect_set(page_struct) page_stats
from
(SELECT 
	--求每个设备访问每个页面的次数
	mid_id ,named_struct('page_id',page_id,'page_count', count(*)) page_struct
from dwd_page_log 
where dt='2020-12-08' and page_id is not null
GROUP  by mid_id ,page_id ) tmp
GROUP by mid_id) t2
on t1.mid_id = t2.mid_id
```



## 4.dws_user_action_daycount

### 4.1 建表

用户： 商城应用注册的用户 user_id

```sql
drop table if exists dws_user_action_daycount;
create external table dws_user_action_daycount
(   
    user_id string comment '用户 id',
    -- dwd_start_log
    login_count bigint comment '登录次数',
    -- dwd_action_log
    cart_count bigint comment '加入购物车次数',
    -- dwd_fact_order_info
    order_count bigint comment '下单次数',
    order_amount    decimal(16,2)  comment '下单金额',
    -- dwd_fact_payment_info
    payment_count   bigint      comment '支付次数',
    payment_amount  decimal(16,2) comment '支付金额',
    -- dwd_fact_order_detail
    order_detail_stats array<struct<sku_id:string,sku_num:bigint,order_count:bigint,order_amount:decimal(20,2)>> comment '下单明细统计'
) COMMENT '每日会员行为'
PARTITIONED BY (`dt` string)
stored as parquet
location '/warehouse/gmall/dws/dws_user_action_daycount/'
tblproperties ("parquet.compression"="lzo");

```

### 4.2 导入

```
with 
临时表名 as (),
临时表名 as (),
....

```

```sql
with
t1 as
(--array<struct<sku_id:string,sku_num:bigint,order_count:bigint,order_amount:decimal(20,2)>>
SELECT 
	user_id ,COLLECT_set(sku_struct) order_detail_stats
from
(SELECT 
	user_id,named_struct('sku_id',sku_id,'sku_num',sum(sku_num ),'order_count',count(*) , 'order_amount',cast(sum(final_amount_d ) as decimal(20,2))) sku_struct
	--粒度： 一个用户下的一笔订单的一个商品是一行
from dwd_fact_order_detail 
where dt='2020-12-08'
GROUP by user_id,sku_id ) tmp
GROUP by user_id),
t2 as
(SELECT 
	user_id,count(*) login_count
from dwd_start_log 
where dt='2020-12-08' and user_id is not NULL 
GROUP by user_id ),
t3 as
(SELECT 
	user_id,count(*) cart_count
from dwd_action_log 
where dt='2020-12-08' and action_id='cart_add'
GROUP by user_id ),
t4 as
(SELECT 
		user_id ,count(*) order_count,sum(final_total_amount) order_amount
from dwd_fact_order_info 
where dt='2020-12-08'
GROUP by user_id ),
t5 as
(SELECT 
		user_id ,count(*) payment_count,sum(payment_amount) payment_amount
from dwd_fact_payment_info 
where dt='2020-12-08'
GROUP by user_id )
insert overwrite table dws_user_action_daycount partition(dt='2020-12-08')
SELECT 
	t2.user_id,
	login_count, 
	--打开了应用，没有加购,数量类的如果不允许为NULL，需要再进行判断处理
	nvl(cart_count,0),
	nvl(order_count,0),
	nvl(order_amount,0.0),
	nvl(payment_count,0),
	nvl(payment_amount,0.0),
	order_detail_stats
from t2 
left join t3 on t2.user_id=t3.user_id
left join t4 on t2.user_id=t4.user_id
left join t5 on t2.user_id=t5.user_id
left join t1 on t2.user_id=t1.user_id
```



## 5.dws_sku_action_daycount

### 5.1建表

```sql
drop table if exists dws_sku_action_daycount;
create external table dws_sku_action_daycount 
(   
    sku_id string comment 'sku_id',
    --求下单 dwd_fact_order_detail
    order_count bigint comment '被下单次数',
    order_num bigint comment '被下单件数',
    order_amount decimal(16,2) comment '被下单金额',
    -- dwd_fact_payment_info
    payment_count bigint  comment '被支付次数',
    payment_num bigint comment '被支付件数',
    payment_amount decimal(16,2) comment '被支付金额',
    --dwd_fact_order_refund_info
    refund_count bigint  comment '被退款次数',
    refund_num bigint comment '被退款件数',
    refund_amount  decimal(16,2) comment '被退款金额',
    -- dwd_action_log
    cart_count bigint comment '被加入购物车次数',
    favor_count bigint comment '被收藏次数',
    -- dwd_fact_comment_info
    appraise_good_count bigint comment '好评数',
    appraise_mid_count bigint comment '中评数',
    appraise_bad_count bigint comment '差评数',
    appraise_default_count bigint comment '默认评价数'
) COMMENT '每日商品行为'
PARTITIONED BY (`dt` string)
stored as parquet
location '/warehouse/gmall/dws/dws_sku_action_daycount/'
tblproperties ("parquet.compression"="lzo");

```

主体： 商品，统计每一个商品每天在商城中被购买，下单，收藏等行为统计！



如何Join:   full join

### 5.2 导入

```sql
with
t6 as
(SELECT 
	sku_id ,
	sum(if(appraise = '1201',1,0)) appraise_good_count,
	sum(if(appraise = '1202',1,0)) appraise_mid_count,
	sum(if(appraise = '1203',1,0)) appraise_bad_count,
	sum(if(appraise = '1204',1,0)) appraise_default_count
from dwd_fact_comment_info 
where dt='2020-12-08'
GROUP by sku_id ),
t7 as
(SELECT 
	item sku_id,
	sum(if(action_id = 'cart_add',1,0)) cart_count,
	sum(if(action_id = 'favor_add',1,0)) favor_count
from dwd_action_log 
--从行为中只过滤出加够和收藏的
where dt='2020-12-08' and (action_id = 'cart_add' or action_id ='favor_add')
GROUP by item ),
t3 as
(--退款相关
SELECT 
	sku_id ,count(*) refund_count, sum(refund_num ) refund_num,sum(refund_amount ) refund_amount
from dwd_fact_order_refund_info 
where dt='2020-12-08'
GROUP by sku_id ),
t4 as
(
--支付表中只有订单，没有商品。 需要将dwd_fact_payment_info和dwd_fact_order_detail join求出所有当天支付的订单的详情(sku_id)
--跨天支付：  dwd_fact_payment_info/12-08 支付的订单，有可能是  12-08/12-07日生成的，在dwd_fact_order_detail/  12-08 | 12-07
SELECT 
	sku_id ,count(*) payment_count, sum(sku_num ) payment_num, sum(final_amount_d ) payment_amount
from
-- Join后的粒度   一个订单的一个商品是一条
(SELECT 
	order_id
from dwd_fact_payment_info 
where dt='2020-12-08') t1 
left join 
(
SELECT 
	order_id ,sku_id ,sku_num ,final_amount_d 
from dwd_fact_order_detail 
where dt='2020-12-08' or dt=date_sub('2020-12-08',1)
) t2
on t1.order_id=t2.order_id
GROUP by sku_id ),
t5 as
(SELECT 
	sku_id,
	count(*) order_count,
	sum(sku_num ) order_num,
	sum(original_amount_d ) order_amount
-- 一个用户在一笔订单购买的一个商品是一行
from dwd_fact_order_detail 
where dt='2020-12-08'
GROUP by sku_id )
insert overwrite table dws_sku_action_daycount partition(dt='2020-12-08')
SELECT 
		--哪个表的sku_id不为NULL，就取哪个！
		nvl(t3.sku_id,nvl(t4.sku_id,nvl(t5.sku_id,nvl(t6.sku_id,t7.sku_id)))) sku_id,
		nvl(order_count,0),
		nvl(order_num,0),
		nvl(order_amount,0.0),
		nvl(payment_count,0),
		nvl(payment_num,0),
		nvl(payment_amount,0.0),
		nvl(refund_count,0),
		nvl(refund_num,0),
		nvl(refund_amount,0.0),
		nvl(cart_count,0),
		nvl(favor_count,0), 
		nvl(appraise_good_count,0),
		nvl(appraise_mid_count,0),
		nvl(appraise_bad_count,0),
		nvl(appraise_default_count,0)	
FROM  t3 
full join t4 on t3.sku_id = t4.sku_id
full join t5 on t3.sku_id = t5.sku_id or t4.sku_id = t5.sku_id
full join t6 on t3.sku_id = t6.sku_id or t4.sku_id = t6.sku_id or t5.sku_id = t6.sku_id
full join t7 on t3.sku_id = t7.sku_id or t4.sku_id = t7.sku_id or t5.sku_id = t7.sku_id or t6.sku_id = t7.sku_id

```

弊端： join 消耗性能！

解决：  一次性查询出所有的字段

​						select

​								sku_id，聚合获取其他字段

​						from

​						(每个结果集都查询所有的字段   union all  结果集  )  

​							group by sku_id

```sql
insert overwrite table dws_sku_action_daycount partition(dt='2020-12-08')
SELECT 
	sku_id,
	sum(order_count), 
	sum(order_num),
	sum(order_amount),
	sum(payment_count),
	sum(payment_num),
	sum(payment_amount),
	sum(refund_count),
	sum(refund_num),
	sum(refund_amount),
	sum(cart_count),
	sum(favor_count), 
	sum(appraise_good_count),
	sum(appraise_mid_count),
	sum(appraise_bad_count),
	sum(appraise_default_count)	
from
(SELECT 
	sku_id ,
	0 order_count, 0 order_num,0 order_amount,
	0 payment_count,0 payment_num, 0 payment_amount, 
	0 refund_count,  0 refund_num, 0 refund_amount, 
	0 cart_count,0 favor_count, 
	sum(if(appraise = '1201',1,0)) appraise_good_count,
	sum(if(appraise = '1202',1,0)) appraise_mid_count,
	sum(if(appraise = '1203',1,0)) appraise_bad_count,
	sum(if(appraise = '1204',1,0)) appraise_default_count
from dwd_fact_comment_info 
where dt='2020-12-08'
GROUP by sku_id 
UNION all
SELECT 
	item sku_id,
	0 order_count, 0 order_num,0 order_amount,
	0 payment_count,0 payment_num, 0 payment_amount, 
	0 refund_count,  0 refund_num, 0 refund_amount, 
	sum(if(action_id = 'cart_add',1,0)) cart_count,
	sum(if(action_id = 'favor_add',1,0)) favor_count,
	0 appraise_good_count,0 appraise_mid_count,0 appraise_bad_count,0 appraise_default_count
from dwd_action_log 
--从行为中只过滤出加够和收藏的
where dt='2020-12-08' and (action_id = 'cart_add' or action_id ='favor_add')
GROUP by item 
union all
--退款相关
SELECT 
	sku_id ,
	0 order_count, 0 order_num,0 order_amount,
	0 payment_count,0 payment_num, 0 payment_amount, 
	count(*) refund_count, sum(refund_num ) refund_num,sum(refund_amount ) refund_amount,
	0 cart_count,0 favor_count, 
	0 appraise_good_count,0 appraise_mid_count,0 appraise_bad_count,0 appraise_default_count
from dwd_fact_order_refund_info 
where dt='2020-12-08'
GROUP by sku_id 
union all
--支付表中只有订单，没有商品。 需要将dwd_fact_payment_info和dwd_fact_order_detail join求出所有当天支付的订单的详情(sku_id)
--跨天支付：  dwd_fact_payment_info/12-08 支付的订单，有可能是  12-08/12-07日生成的，在dwd_fact_order_detail/  12-08 | 12-07
SELECT 
	sku_id ,
	0 order_count, 0 order_num,0 order_amount,
	count(*) payment_count, sum(sku_num ) payment_num, sum(final_amount_d ) payment_amount,
	0 refund_count,  0 refund_num, 0 refund_amount, 
	0 cart_count,0 favor_count, 
	0 appraise_good_count,0 appraise_mid_count,0 appraise_bad_count,0 appraise_default_count
from
-- Join后的粒度   一个订单的一个商品是一条
(SELECT 
	order_id
from dwd_fact_payment_info 
where dt='2020-12-08') t1 
left join 
(
SELECT 
	order_id ,sku_id ,sku_num ,final_amount_d 
from dwd_fact_order_detail 
where dt='2020-12-08' or dt=date_sub('2020-12-08',1)
) t2
on t1.order_id=t2.order_id
GROUP by sku_id 
union all
SELECT 
	sku_id,
	count(*) order_count,
	sum(sku_num ) order_num,
	sum(original_amount_d ) order_amount,
	0 payment_count,0 payment_num, 0 payment_amount, 
	0 refund_count,  0 refund_num, 0 refund_amount, 
	0 cart_count,0 favor_count, 
	0 appraise_good_count,0 appraise_mid_count,0 appraise_bad_count,0 appraise_default_count
-- 一个用户在一笔订单购买的一个商品是一行
from dwd_fact_order_detail 
where dt='2020-12-08'
GROUP by sku_id ) tmp
GROUP by sku_id 
```



## 6.dws_activity_info_daycount

### 6.1 建表

```sql
drop table if exists dws_activity_info_daycount;
create external table dws_activity_info_daycount(
    -- dwd_dim_activity_info
    `id` string COMMENT '编号',
    `activity_name` string  COMMENT '活动名称',
    `activity_type` string  COMMENT '活动类型',
    `start_time` string  COMMENT '开始时间',
    `end_time` string  COMMENT '结束时间',
    `create_time` string  COMMENT '创建时间',
    -- dwd_display_log
    `display_count` bigint COMMENT '曝光次数',
    -- dwd_fact_order_info
    `order_count` bigint COMMENT '下单次数',
    `order_amount` decimal(20,2) COMMENT '下单金额',
    `payment_count` bigint COMMENT '支付次数',
    `payment_amount` decimal(20,2) COMMENT '支付金额'
) COMMENT '每日活动统计'
PARTITIONED BY (`dt` string)
stored as parquet
location '/warehouse/gmall/dws/dws_activity_info_daycount/'
tblproperties ("parquet.compression"="lzo");

```

主体：每天举行活动的信息统计！

每天统计的应该是正在举行的活动！

### 6.2 导入

```sql
insert overwrite table dws_activity_info_daycount partition(dt='2020-12-08')
SELECT 
	t1.id,
	activity_name, activity_type, start_time, end_time, create_time, display_count,
	order_count, order_amount, 
	payment_count, payment_amount
from
(SELECT 
	*
from dwd_dim_activity_info 
where dt='2020-12-08' ) t1
--活动必须在有效期
--and  end_time >= '2020-12-08'
left join
(select
	item id,count(*) display_count
from dwd_display_log 
where dt='2020-12-08' and display_type ='activity'
GROUP by item ) t2
on t1.id = t2.id
left join
(

 select
 		activity_id id,
 		sum(if(dt='2020-12-08',1,0)) order_count,
 		sum(if(dt='2020-12-08',original_total_amount ,0)) order_amount,
 		sum(if(date_format(payment_time,'yyyy-MM-dd')='2020-12-08',1,0 )) payment_count,
 		sum(if(date_format(payment_time,'yyyy-MM-dd')='2020-12-08',final_total_amount ,0 )) payment_amount
 --存在跨天支付，取今天下单和昨天下单的所有的参与活动的订单
 from dwd_fact_order_info 
 where (dt='2020-12-08' or dt=date_sub('2020-12-08',1)) and activity_id  is not NULL 
 GROUP by activity_id 
) t3
on t1.id = t3.id
```

## 7.dws_area_stats_daycount

### 7.1建表

```sql
drop table if exists dws_area_stats_daycount;
create external table dws_area_stats_daycount(
    --dwd_dim_base_province
    `id` bigint COMMENT '编号',
    `province_name` string COMMENT '省份名称',
    `area_code` string COMMENT '地区编码',
    `iso_code` string COMMENT 'iso编码',
    `region_id` string COMMENT '地区ID',
    `region_name` string COMMENT '地区名称',
    --dwd_start_log
    `login_count` string COMMENT '活跃设备数',
    --dwd_fact_order_info
    `order_count` bigint COMMENT '下单次数',
    `order_amount` decimal(20,2) COMMENT '下单金额',
    `payment_count` bigint COMMENT '支付次数',
    `payment_amount` decimal(20,2) COMMENT '支付金额'
) COMMENT '每日地区统计表'
PARTITIONED BY (`dt` string)
stored as parquet
location '/warehouse/gmall/dws/dws_area_stats_daycount/'
tblproperties ("parquet.compression"="lzo");

```

### 7.2 导入

```SQL
insert overwrite table dws_area_stats_daycount partition(dt='2020-12-08')
--每日省份的全量统计信息
SELECT 
		t1.id, province_name, t1.area_code, iso_code, region_id, region_name, 
		nvl(login_count,0),
		nvl(order_count,0),
		nvl(order_amount,0.0),
		nvl(payment_count,0),
		nvl(payment_amount,0.0)
from dwd_dim_base_province t1
left join
(select
	province_id,
	sum(if(dt='2020-12-08',1,0)) order_count,
	sum(if(dt='2020-12-08',final_total_amount,0)) order_amount,
	--求的是2020-12-08号支付的订单有多少个，支付了多少钱
	sum(if(date_format(payment_time,'yyyy-MM-dd')='2020-12-08',1,0)) payment_count,
	sum(if(date_format(payment_time,'yyyy-MM-dd')='2020-12-08',final_total_amount ,0)) payment_amount
-- 是 2020-12-08 和  2020-12-07创建的所有的订单
from dwd_fact_order_info 
--只能过滤出2020-12-08创建的所有的订单
--还要求2020-12-08支付的订单，支付的订单，有可能是2020-12-07创建的
where dt='2020-12-08' or dt=date_sub('2020-12-08',1)
GROUP by province_id ) t2
on t1.id = t2.province_id
left join
(--活跃设备数
select
	area_code,count(*) login_count
from
(select
	area_code,mid_id 
from dwd_start_log 
where dt='2020-12-08'
GROUP by area_code,mid_id ) tmp
GROUP by area_code) t3
on t1.area_code=t3.area_code
```

# 八、DWT层

## 1.简介

DWT层也是数据服务层！

和DWS层的区别在于，是一张累积型宽表(从多张事实表和维度表中取数据)！

不是分区表，是一张全量表！

全量表在更新时决定了方式：  old(dwt表)  full join  new(dws今天新导入的数据)，新旧交替，覆盖原表！



DWT层是从DWS层取数据！ DWS层将同一主题数据按天聚合，DWT层是将同一主题数据累积聚合！



## 2.dwt_uv_topic

UV: user views: 用户浏览数量

PV: page views： 页面浏览数量

### 2.1 建表

```sql
drop table if exists dwt_uv_topic;
create external table dwt_uv_topic
(
    `mid_id` string comment '设备id',
    -- 12-08 累积完 1-- MI|IPHONE  
    --12-09 DWS统计的数据 是 1-- MI|HUAWEI
    -- 累积完 1-- MI|IPHONE|HUAWEI
    -- 遇到一些没有调用Hive本身提供函数思路的操作，可以考虑自定义函数UDF
    `brand` string comment '手机品牌',
    `model` string comment '手机型号',
    
    -- 老用户不变，新用户取当天
    `login_date_first` string  comment '首次活跃时间',
    -- 如果当天登录，那么就取当天，否则取之前的末次活跃时间
    `login_date_last` string  comment '末次活跃时间',
    -- 从dws取活跃次数
    `login_day_count` bigint comment '当日活跃次数',
    -- 之前活跃的天数  +  if(今天活跃，1,0)
    `login_count` bigint comment '累积活跃天数'
) COMMENT '设备主题宽表'
stored as parquet
location '/warehouse/gmall/dwt/dwt_uv_topic'
tblproperties ("parquet.compression"="lzo");

```

old full join new on old.id=new.id

今天登录的老用户： where old.id is not null and new.id is not null

新用户： where old.id is  null

今天未登录的老用户：  where new.id is  null

老用户： where old.id is not null

### 2.2 导入

```sql
INSERT overwrite table dwt_uv_topic 
SELECT 
	nvl(old.mid_id ,new.mid_id) mid_id,
	compact_str(old.brand,new.brand) brand,
	compact_str(old.model,new.model) model,
	--首次活跃时间  老用户不变，新用户取当天
	if(old.login_date_first is null,dt,old.login_date_first) login_date_first,
	--末次活跃时间 如果当天登录，那么就取当天，否则取之前的末次活跃时间
	if(new.mid_id is not null,dt,old.login_date_last) login_date_last,
	
	nvl(new.login_count,0) login_day_count,
	 -- 之前活跃的天数  +  if(今天活跃，1,0)
	nvl(old.login_count,0) + if(new.login_count > 0 ,1 , 0) login_count
from dwt_uv_topic old 
full join
(
select
	mid_id ,brand ,model ,login_count ,dt
from dws_uv_detail_daycount 
where dt='2020-12-08'
)new
on old.mid_id =new.mid_id;
```



## 3.dwt_user_topic

### 3.1 建表

```sql
drop table if exists dwt_user_topic;
create external table dwt_user_topic
(
    user_id string  comment '用户id',
    -- 通过dws_user_detail_daycount 用户当天的行为来更新
    login_date_first string  comment '首次登录时间',
    login_date_last string  comment '末次登录时间',
     order_date_first string  comment '首次下单时间',
    order_date_last string  comment '末次下单时间',
     payment_date_first string  comment '首次支付时间',
    payment_date_last string  comment '末次支付时间',
    -- dwt_user_topic +  dws_user_detail_daycount当天的值
    login_count bigint comment '累积登录天数',
    order_count bigint comment '累积下单次数',
    order_amount decimal(16,2) comment '累积下单金额',
     payment_count decimal(16,2) comment '累积支付次数',
    payment_amount decimal(16,2) comment '累积支付金额',
    
    
    -- 通过dws_user_detail_daycount 取最近30日，之后聚合
    login_last_30d_count bigint comment '最近30日登录天数',
    order_last_30d_count bigint comment '最近30日下单次数',
    order_last_30d_amount bigint comment '最近30日下单金额',
    payment_last_30d_count decimal(16,2) comment '最近30日支付次数',
    payment_last_30d_amount decimal(16,2) comment '最近30日支付金额'
)COMMENT '会员主题宽表'
stored as parquet
location '/warehouse/gmall/dwt/dwt_user_topic/'
tblproperties ("parquet.compression"="lzo");

```

### 3.2 导入

```sql
insert overwrite table dwt_user_topic 
SELECT 
	t1.user_id, login_date_first, login_date_last, login_count, 
	nvl(login_last_30d_count,0) login_last_30d_count, 
	order_date_first, order_date_last, order_count, order_amount,
	nvl(order_last_30d_count,0) order_last_30d_count, 
	nvl(order_last_30d_amount,0.0) order_last_30d_amount, payment_date_first, payment_date_last, payment_count,
	payment_amount,
	nvl(payment_last_30d_count,0) payment_last_30d_count,
	nvl(payment_last_30d_amount,0.0) payment_last_30d_amount
from
(SELECT 
	nvl(old.user_id,new.user_id) user_id,
	nvl(old.login_date_first,'2020-12-08') login_date_first,
	--if(old.user_id is null,'2020-12-08',old.login_date_first) login_date_first 
	nvl(new.dt,old.login_date_last) login_date_last,
	--if(new.dt is null,old.login_date_last ,dt) login_date_last
	if(old.user_id is null ,   
		if(new.order_count > 0 , '2020-12-08', NULL)     , 
		nvl(old.order_date_first,if(new.order_count > 0 ,  '2020-12-08', NULL))
      ) order_date_first,
      -- 今天未登录的，末次下单时间不变。 今天登录的，且order_count > 0的，将末次下单时间更新为今天
      if(new.order_count is null,
      -- 今天未登录的，末次下单时间不变
      	old.order_date_last, 
      	--今天登录的，且order_count > 0的，将末次下单时间更新为今天
      	if(new.order_count > 0 , '2020-12-08', old.order_date_last )
      ) order_date_last,
      if(old.user_id is null ,
		if(new.payment_count > 0 , '2020-12-08', NULL)     , 
		nvl(old.payment_date_first,if(new.payment_count > 0 ,  '2020-12-08', NULL))
      ) payment_date_first,
      if(new.payment_count is null,
      	old.payment_date_last, 
      	if(new.payment_count > 0 , '2020-12-08', old.payment_date_last )
      ) payment_date_last,
      -- old或new的数据运算都得进行判空处理
      nvl(old.login_count,0) + if(new.user_id is null ,0 , 1) login_count,
      nvl(old.order_count,0)  + nvl(new.order_count,0) order_count,
      nvl(old.order_amount,0.0)  + nvl(new.order_amount,0.0) order_amount,
      nvl(old.payment_count,0)  + nvl(new.payment_count,0) payment_count,
      nvl(old.payment_amount,0.0)  + nvl(new.payment_amount,0.0) payment_amount
from dwt_user_topic old 
full join 
(
select
	*
from dws_user_action_daycount 
where dt='2020-12-08'
) new
on old.user_id =new.user_id ) t1
left join
(SELECT 
    		user_id,
    		count(*) login_last_30d_count,
    		sum(order_count) order_last_30d_count,
    		sum(order_amount) order_last_30d_amount,
    		sum(payment_count) payment_last_30d_count,
    		sum(payment_amount) payment_last_30d_amount
    from dws_user_action_daycount 
    --先过滤最近30天每天聚合的数据
    where dt BETWEEN date_sub('2020-12-08',29)  and  '2020-12-08'
    GROUP by user_id  ) t2
    on t1.user_id = t2.user_id
```

## 4.dwt_sku_topic

### 4.1 建表

```sql
drop table if exists dwt_sku_topic;
create external table dwt_sku_topic
(
    sku_id string comment 'sku_id',
    -- 理论上dws_sku_action_daycount也应该有spu_id字段，没有只能从dwd_dim_sku_info中查询
    spu_id string comment 'spu_id',
    -- dws_sku_action_daycount 取最近30天的数据，之后聚合
    order_last_30d_count bigint comment '最近30日被下单次数',
    order_last_30d_num bigint comment '最近30日被下单件数',
    order_last_30d_amount decimal(16,2)  comment '最近30日被下单金额',
    payment_last_30d_count   bigint  comment '最近30日被支付次数',
    payment_last_30d_num bigint comment '最近30日被支付件数',
    payment_last_30d_amount  decimal(16,2) comment '最近30日被支付金额',
      refund_last_30d_count bigint comment '最近三十日退款次数',
    refund_last_30d_num bigint comment '最近三十日退款件数',
    refund_last_30d_amount decimal(16,2) comment '最近三十日退款金额',
    cart_last_30d_count bigint comment '最近30日被加入购物车次数',
    favor_last_30d_count bigint comment '最近30日被收藏次数',
     appraise_last_30d_good_count bigint comment '最近30日好评数',
    appraise_last_30d_mid_count bigint comment '最近30日中评数',
    appraise_last_30d_bad_count bigint comment '最近30日差评数',
    appraise_last_30d_default_count bigint comment '最近30日默认评价数',
    
    -- dwt_sku_topic +  dws_sku_action_daycount当天聚合的数据
    order_count bigint comment '累积被下单次数',
    order_num bigint comment '累积被下单件数',
    order_amount decimal(16,2) comment '累积被下单金额', 
    payment_count   bigint  comment '累积被支付次数',
    payment_num bigint comment '累积被支付件数',
    payment_amount  decimal(16,2) comment '累积被支付金额',
    refund_count bigint comment '累积退款次数',
    refund_num bigint comment '累积退款件数',
    refund_amount decimal(16,2) comment '累积退款金额',
    cart_count bigint comment '累积被加入购物车次数',
    favor_count bigint comment '累积被收藏次数',
    appraise_good_count bigint comment '累积好评数',
    appraise_mid_count bigint comment '累积中评数',
    appraise_bad_count bigint comment '累积差评数',
    appraise_default_count bigint comment '累积默认评价数'
 )COMMENT '商品主题宽表'
stored as parquet
location '/warehouse/gmall/dwt/dwt_sku_topic/'
tblproperties ("parquet.compression"="lzo");

```

### 4.2 导入

```sql
insert overwrite table dwt_sku_topic 
SELECT 
    t1.sku_id, spu_id, 
    nvl(order_last_30d_count,0), 
	nvl(order_last_30d_num,0), 
	nvl(order_last_30d_amount,0.0), order_count, order_num, 
	order_amount, 
	nvl(payment_last_30d_count,0),
	nvl(payment_last_30d_num,0),
	nvl(payment_last_30d_amount,0.0),
	payment_count, payment_num, payment_amount, 
	nvl(refund_last_30d_count,0),
	nvl(refund_last_30d_num,0),
	nvl(refund_last_30d_amount,0.0), refund_count, refund_num, refund_amount, 
	nvl(cart_last_30d_count,0), 
	cart_count, 
	nvl(favor_last_30d_count,0), favor_count,
	nvl(appraise_last_30d_good_count,0), 
	nvl(appraise_last_30d_mid_count,0),
	nvl(appraise_last_30d_bad_count,0),
	nvl(appraise_last_30d_default_count,0),
	appraise_good_count, appraise_mid_count, appraise_bad_count, appraise_default_count
    from
    (SELECT 
    	nvl(old.sku_id ,new.sku_id) sku_id ,
    	nvl(old.order_count,0) + nvl(new.order_count,0) order_count,
    	nvl(old.order_num,0) + nvl(new.order_num,0) order_num,
    	nvl(old.order_amount,0.0) + nvl(new.order_amount,0.0) order_amount,
    	nvl(old.payment_count,0) + nvl(new.payment_count,0) payment_count,
    	nvl(old.payment_num,0) + nvl(new.payment_num,0) payment_num,
    	nvl(old.payment_amount,0.0) + nvl(new.payment_amount,0.0) payment_amount,
    	nvl(old.refund_count,0) + nvl(new.refund_count,0) refund_count,
    	nvl(old.refund_num,0) + nvl(new.refund_num,0) refund_num,
    	nvl(old.refund_amount,0.0) + nvl(new.refund_amount,0.0) refund_amount,
    	nvl(old.cart_count,0) + nvl(new.cart_count,0) cart_count,
    	nvl(old.favor_count,0) + nvl(new.favor_count,0) favor_count,
    	nvl(old.appraise_good_count,0) + nvl(new.appraise_good_count,0) appraise_good_count,
    	nvl(old.appraise_mid_count,0) + nvl(new.appraise_mid_count,0) appraise_mid_count,
    	nvl(old.appraise_bad_count,0) + nvl(new.appraise_bad_count,0) appraise_bad_count,
    	nvl(old.appraise_default_count,0) + nvl(new.appraise_default_count,0) appraise_default_count
    from dwt_sku_topic  old 
    full join 
    (
    select
    	*
    from dws_sku_action_daycount 
    where dt='2020-12-08'
    ) new
    on old.sku_id = new.sku_id) t1
    left join   
(SELECT 
	sku_id ,
	sum(order_count) order_last_30d_count,
	sum(order_num) order_last_30d_num,
	sum(order_amount) order_last_30d_amount,
	sum(payment_count) payment_last_30d_count,
	sum(payment_num) payment_last_30d_num,
	sum(payment_amount) payment_last_30d_amount,
	sum(refund_count) refund_last_30d_count,
	sum(refund_num) refund_last_30d_num,
	sum(refund_amount) refund_last_30d_amount,
	sum(cart_count) cart_last_30d_count,
	sum(favor_count) favor_last_30d_count,
	sum(appraise_good_count) appraise_last_30d_good_count,
	sum(appraise_mid_count) appraise_last_30d_mid_count,
	sum(appraise_bad_count) appraise_last_30d_bad_count,
	sum(appraise_default_count) appraise_last_30d_default_count	
from dws_sku_action_daycount 
where dt BETWEEN  date_sub('2020-12-08',29) and '2020-12-08'
GROUP by sku_id ) t2
on t1.sku_id = t2.sku_id
left join 
(
SELECT 
	id ,spu_id 
from dwd_dim_sku_info 
where dt='2020-12-08'
) t3
on t1.sku_id = t3.id;
```

## 5.dwt_activity_topic

### 5.1 建表

```sql
drop table if exists dwt_activity_topic;
create external table dwt_activity_topic(
    -- dws_activity_info_daycount
    `id` string COMMENT '编号',
    `activity_name` string  COMMENT '活动名称',
    `activity_type` string  COMMENT '活动类型',
    `start_time` string  COMMENT '开始时间',
    `end_time` string  COMMENT '结束时间',
    `create_time` string  COMMENT '创建时间',  
    -- dws_activity_info_daycount
    `display_day_count` bigint COMMENT '当日曝光次数',
    `order_day_count` bigint COMMENT '当日下单次数',
    `order_day_amount` decimal(20,2) COMMENT '当日下单金额',
    `payment_day_count` bigint COMMENT '当日支付次数',
    `payment_day_amount` decimal(20,2) COMMENT '当日支付金额',
    -- dwt +  dws_activity_info_daycount
    `display_count` bigint COMMENT '累积曝光次数',
    `order_count` bigint COMMENT '累积下单次数',
    `order_amount` decimal(20,2) COMMENT '累积下单金额',
    `payment_count` bigint COMMENT '累积支付次数',
    `payment_amount` decimal(20,2) COMMENT '累积支付金额'
) COMMENT '活动主题宽表'
stored as parquet
location '/warehouse/gmall/dwt/dwt_activity_topic/'
tblproperties ("parquet.compression"="lzo");

```



### 5.2 导入

```sql
insert overwrite table dwt_activity_topic 
SELECT 
	nvl(old.id,new.id) id,
	nvl(old.activity_name,new.activity_name) activity_name,
	nvl(old.activity_type,new.activity_type) activity_type,
	nvl(old.start_time,new.start_time) start_time,
	nvl(old.end_time,new.end_time) end_time,
	nvl(old.create_time,new.create_time) create_time,
	nvl(new.display_count,0) display_day_count,
	nvl(new.order_count,0) order_day_count,
	nvl(new.order_amount,0.0) order_day_amount,
	nvl(new.payment_count,0) payment_day_count,
	nvl(new.payment_amount,0.0) payment_day_amount,
	nvl(old.display_count,0 ) + nvl(new.display_count,0)  display_count,
	nvl(old.order_count,0 ) + nvl(new.order_count,0)  order_count,
	nvl(old.order_amount,0.0 ) + nvl(new.order_amount,0.0)  order_amount,
	nvl(old.payment_count,0 ) + nvl(new.payment_count,0)  payment_count,
	nvl(old.payment_amount,0.0 ) + nvl(new.payment_amount,0.0)  payment_amount
from dwt_activity_topic old 
full join
(
select
	*
from dws_activity_info_daycount 
where dt='2020-12-08'
) new
on old.id = new.id;
```

## 6.dwt_area_topic

### 6.1 建表

```sql
drop table if exists dwt_area_topic;
create external table dwt_area_topic(
    --dws_area_stats_daycount
    `id` bigint COMMENT '编号',
    `province_name` string COMMENT '省份名称',
    `area_code` string COMMENT '地区编码',
    `iso_code` string COMMENT 'iso编码',
    `region_id` string COMMENT '地区ID',
    `region_name` string COMMENT '地区名称',
    --dws_area_stats_daycount
    `login_day_count` string COMMENT '当天活跃设备数',
    `payment_day_count` bigint COMMENT '当天支付次数',
    `payment_day_amount` decimal(16,2) COMMENT '当天支付金额',
    `order_day_count` bigint COMMENT '当天下单次数',
    `order_day_amount` decimal(16,2) COMMENT '当天下单金额',
    
    --建模少累积
    
    --dws_area_stats_daycount 取最近30天的数据，之后聚合
    `login_last_30d_count` string COMMENT '最近30天活跃设备数',
    `order_last_30d_count` bigint COMMENT '最近30天下单次数',
    `order_last_30d_amount` decimal(16,2) COMMENT '最近30天下单金额',
    `payment_last_30d_count` bigint COMMENT '最近30天支付次数',
    `payment_last_30d_amount` decimal(16,2) COMMENT '最近30天支付金额'
) COMMENT '地区主题宽表'
stored as parquet
location '/warehouse/gmall/dwt/dwt_area_topic/'
tblproperties ("parquet.compression"="lzo");

```

### 6.2 导入

```sql
insert overwrite table dwt_area_topic 
SELECT 
		t1.id, province_name, area_code, iso_code, region_id, region_name, 
	login_day_count, login_last_30d_count, order_day_count, order_day_amount,
	order_last_30d_count, order_last_30d_amount, payment_day_count, 
	payment_day_amount, payment_last_30d_count, payment_last_30d_amount
from	
(SELECT 
    	id,province_name,area_code,iso_code,region_id,region_name,
    	login_count login_day_count,
    	order_count order_day_count,
    	order_amount order_day_amount,
    	payment_count payment_day_count,
    	payment_amount payment_day_amount	
    --统计的全量的地区信息
    from dws_area_stats_daycount 
    where dt='2020-12-08' ) t1
    join
    ( 
    -- 最近30天活跃设备数(从dwd_start_log取最近30天所有的mid_id，去重，统计数量)     最近30天累积活跃设备数 
    SELECT 
    --最近30天累积活跃设备数 
    		id,
    		sum(login_count) login_last_30d_count,
    		sum(order_count) order_last_30d_count,
    		sum(order_amount) order_last_30d_amount,
    		sum(payment_count) payment_last_30d_count,
    		sum(payment_amount) payment_last_30d_amount
    from dws_area_stats_daycount 
    where dt BETWEEN  date_sub('2020-12-08',29) and '2020-12-08'
    GROUP by id ) t2
    on t1.id = t2.id;
```

# 九、ADS层

## 1.简介

ADS层直接和需求相关！通常是一个（类）需求一张表！

ADS层可以使用分区表，但是没必要，数据量少！通常是全量表！

ADS层每一个统计的结果必须有一个对应的日期属性！

ADS层的数据，从DWS和DWT层导入而来，如果查询某N天的数据，从DWS层取！如果取截至到目前的累积数据，从DWT层取！



## 2.设备主题

日活：当日活跃的**设备数**

周活：当周活跃的**设备数**

月活：当月活跃的**设备数**



活跃：在指定的日期范围内，至少启动过一次APP!



### 2.1活跃设备数

#### 2.1.1 建表

```sql
drop table if exists ads_uv_count;
create external table ads_uv_count(
    `dt` string COMMENT '统计日期',
    `day_count` bigint COMMENT '当日用户数量',
    `wk_count`  bigint COMMENT '当周用户数量',
    `mn_count`  bigint COMMENT '当月用户数量',
    `is_weekend` string COMMENT 'Y,N是否是周末,用于得到本周最终结果',
    `is_monthend` string COMMENT 'Y,N是否是月末,用于得到本月最终结果' 
) COMMENT '活跃设备数'
row format delimited fields terminated by '\t'
location '/warehouse/gmall/ads/ads_uv_count/';

```

#### 2.1.2 导入

```sql
-- ads的数据从 dwt 和 dws中查询得到
-- dws : 只查当天的数据 活 查 某个连续N天的数据
-- dwt:  累积了所有的数据，查询全量数据
insert into table ads_uv_count 
SELECT 
	'2020-12-08',
    -- 从dwt层取所有用户，判断 login_date_last = 统计日期
    sum(if(login_date_last ='2020-12-08',1,0)) day_count,
    -- 从dwt层取所有用户，判断 login_date_last >= 统计日期所在的周一
    sum(if(login_date_last  >=  date_sub(next_day('2020-12-08','Mo'),7) ,1 ,0 )) wk_count,
     -- 从dwt层取所有用户，判断 login_date_last 所在月  = 统计日期所在月
    sum(if(last_day(login_date_last) = last_day('2020-12-08') , 1, 0 )) mn_count,
    -- 取统计日期所在周的周日，如果和统计日期是同一天，统计日期就是周日，否则不是
    if(date_sub(next_day('2020-12-08','Mo'),1) = '2020-12-08' ,'Y','N')  is_weekend,
    -- 取统计日期所在月的最后一天，如果和统计日期是同一天，统计日期就是月末
    if(last_day('2020-12-08') = '2020-12-08' ,'Y','N' ) is_monthend
from dwt_uv_topic ;
```



### 2.2 每日新增设备

#### 2.2.1 建表

```sql
create external table ads_new_mid_count
(
    `create_date`     string comment '创建时间' ,
    `new_mid_count`   BIGINT comment '新增设备数量' 
)  COMMENT '每日新增设备数量'
row format delimited fields terminated by '\t'
location '/warehouse/gmall/ads/ads_new_mid_count/';

```

#### 2.2.2 导入

```sql
insert into table ads_new_mid_count 
--dws无法判断当天的设备是否是新设备
SELECT 
	'2020-12-08',
	count(*)
from dwt_uv_topic 
--过滤出今天活跃的新设备
where login_date_first = '2020-12-08';
```

### 2.3 留存率

日期：  新增当天的日期  ，  留存的天数 ， 留存N天后的日活日期

​				新增当天的日期  + 留存的天数 = 留存N天后的日活日期



统计：  2020-8-8日新增的这批用户，留存1天留存率

​					2020-8-8日当天新增的人数： x

​					x中，在 2020-8-8 + 1 日期，活跃的设备数 ：y

 2020-8-8日新 用户留存1天的人数： y

 2020-8-8日新 用户留存1天的留存率： y / x





#### 2.3.1 建表

```sql
drop table if exists ads_user_retention_day_rate;
create external table ads_user_retention_day_rate 
(
     `stat_date`          string comment '统计数据的日期',
     `create_date`       string  comment '设备新增日期',
     `retention_day`     int comment '截止当前日期留存天数',
     `retention_count`    bigint comment  '留存数量',
     `new_mid_count`     bigint comment '设备新增数量',
     `retention_ratio`   decimal(16,2) comment '留存率'
)  COMMENT '留存率'
row format delimited fields terminated by '\t'
location '/warehouse/gmall/ads/ads_user_retention_day_rate/';

```

#### 2.3.2 导入

```sql
insert into table ads_user_retention_day_rate
     --已经将2020-12-08日的数据导入到数仓了
     	--有2020-12-08日的日活数据
     	-- 可以求2020-12-07日新增这批人的1日留存，可以求2020-12-06新增这批人的2日留存，求2020-12-05日新增这批人的3日留存
     select
     	-- 先求2020-12-07日新增这批人的1日留存
     	'2020-12-08' stat_date,
     	date_sub('2020-12-08',1) create_date ,
     	1 retention_day,
     	-- 2020-12-07日新增的这批人在2020-12-08日活跃的有多少人
     	retention_count,
     	new_mid_count,
     	cast (retention_count / new_mid_count * 100 as decimal(16,2)) retention_ratio
     from
    ( select
    	'2020-12-08' dt,
     -- 2020-12-07日新增的这批人在2020-12-08日活跃的有多少人
     count(*) retention_count
     from dwt_uv_topic 
     where login_date_last = '2020-12-08'
     and login_date_first =date_sub('2020-12-08',1) ) t1 join     
 (SELECT 
 	'2020-12-08' dt,
 	new_mid_count
 ---- 2020-12-07日新增的这批人
 from  ads_new_mid_count 
 where create_date = date_sub('2020-12-08',1) ) t2
 on t1.dt = t2.dt
  union all
 select
     	-- 先求2020-12-06日新增这批人的2日留存
     	'2020-12-08' stat_date,
     	date_sub('2020-12-08',2) create_date ,
     	2 retention_day,
     	-- 2020-12-06日新增的这批人在2020-12-08日活跃的有多少人
     	retention_count,
     	new_mid_count,
     	cast (retention_count / new_mid_count * 100 as decimal(16,2)) retention_ratio
     from
    ( select
    	'2020-12-08' dt,
     -- 2020-12-06日新增的这批人在2020-12-08日活跃的有多少人
     count(*) retention_count
     from dwt_uv_topic 
     where login_date_last = '2020-12-08'
     and login_date_first =date_sub('2020-12-08',2) ) t1 join     
 (SELECT 
 	'2020-12-08' dt,
 	new_mid_count
 ---- 2020-12-06日新增的这批人
 from  ads_new_mid_count 
 where create_date = date_sub('2020-12-08',2) ) t2
 on t1.dt = t2.dt
 union all
 select
     	-- 先求2020-12-05日新增这批人的3日留存
     	'2020-12-08' stat_date,
     	date_sub('2020-12-08',3) create_date ,
     	3 retention_day,
     	-- 2020-12-05日新增的这批人在2020-12-08日活跃的有多少人
     	retention_count,
     	new_mid_count,
     	cast (retention_count / new_mid_count * 100 as decimal(16,2)) retention_ratio
     from
    ( select
    	'2020-12-08' dt,
     -- 2020-12-05日新增的这批人在2020-12-08日活跃的有多少人
     count(*) retention_count
     from dwt_uv_topic 
     where login_date_last = '2020-12-08'
     and login_date_first =date_sub('2020-12-08',3) ) t1 join     
 (SELECT 
 	'2020-12-08' dt,
 	new_mid_count
 ---- 2020-12-05日新增的这批人
 from  ads_new_mid_count 
 where create_date = date_sub('2020-12-08',3) ) t2
 on t1.dt = t2.dt;
```

### 2.4 沉默用户数

沉默用户数:  仅仅在安装当天启动过应用，并且安装的当天距离现在已经是7天之前



仅仅在安装当天启动过应用:  从dwt层，login_date_last=login_date_first=安装当天

​														login_date_last=login_date_first <= date_sub(当前日期，7)

#### 2.4.1 建表

```sql
drop table if exists ads_silent_count;
create external table ads_silent_count( 
    `dt` string COMMENT '统计日期',
    `silent_count` bigint COMMENT '沉默设备数'
) COMMENT '沉默用户数'
row format delimited fields terminated by '\t'
location '/warehouse/gmall/ads/ads_silent_count';

```

#### 2.4.2 导入

```sql
insert into table ads_silent_count 
SELECT 
	'2020-12-08',
	count(*)
from dwt_uv_topic 
-- 仅仅在安装当天启动过
where login_date_first = login_date_last 
and
-- 安装当天 距离现在七天之前
login_date_first  <= date_sub('2020-12-08',7) ;
```

### 2.5 本周回流用户数

本周回流用户： 本周的活跃老用户  ，上周没有活跃



本周回流用户：  本周活跃的老用户  -（取差集）  上周活跃的老用户 

SQL取差集合：  a left join b on xxx  where b.id is null

​		

```sql

```



#### 2.5.1 建表

```sql
drop table if exists ads_back_count;
create external table ads_back_count( 
    `dt` string COMMENT '统计日期',
    `wk_dt` string COMMENT '统计日期所在周',
    `wastage_count` bigint COMMENT '回流设备数'
) COMMENT '本周回流用户数'
row format delimited fields terminated by '\t'
location '/warehouse/gmall/ads/ads_back_count';

```



#### 2.5.2 导入

```sql
--本周活跃的老用户  -（取差集）  上周活跃的老用户 
insert into table ads_back_count
SELECT 
	'2020-12-08',
	concat(date_sub(next_day('2020-12-08','MO'),7),'_',date_sub(next_day('2020-12-08','MO'),1)),
	count(*) 
from
(--本周活跃的老用户
SELECT 
	mid_id 
from dwt_uv_topic 
--求出本周活跃用户
where login_date_last >= date_sub(next_day('2020-12-08','MO'),7)
and
--过滤出本周活跃的老用户  不是这周新注册的就是老用户
login_date_first < date_sub(next_day('2020-12-08','MO'),7) ) t1
left join
(
-- 上周活跃的老用户 
SELECT 
	 mid_id 
	--dwt_uv_topic 无法判断是否上周活跃
from dws_uv_detail_daycount 
where dt BETWEEN  date_sub(next_day('2020-12-08','MO'),14) and date_sub(next_day('2020-12-08','MO'),8)
GROUP by mid_id ) t2
on t1.mid_id = t2.mid_id
where t2.mid_id is null;
```



### 2.6 流失用户

流失用户：最近7天未活跃的设备

​						最后一次登录的时间，距离现在已经7天了！

​						从dwt层取，login_date_last  <= date_sub(当前日期，7)

#### 2.6.1 建表

```sql
drop table if exists ads_wastage_count;
create external table ads_wastage_count( 
    `dt` string COMMENT '统计日期',
    `wastage_count` bigint COMMENT '流失设备数'
) COMMENT '流失用户数'
row format delimited fields terminated by '\t'
location '/warehouse/gmall/ads/ads_wastage_count';

```

#### 2.6.2 导入

```sql
insert into table ads_wastage_count
    SELECT 
    	'2020-12-08',
    	count(*) 
    from dwt_uv_topic 
    where login_date_last  <= date_sub('2020-12-08',7)
```



### 2.7最近连续三周活跃用户数

连续三周活跃:  最近连续三周，每周都需要活跃一次！

取本周，所有的周活用户:t1

取最近前1周，所有的周活用户:t2

取最近前2周，所有的周活用户:t3

t1,t2,t3都需要去重

t1 union all t2 union all t3

group by mid_id

count(*) = 3

#### 2.7.1 建表

```sql
drop table if exists ads_continuity_wk_count;
create external table ads_continuity_wk_count( 
    `dt` string COMMENT '统计日期,一般用结束周周日日期,如果每天计算一次,可用当天日期',
    `wk_dt` string COMMENT '持续时间',
    `continuity_count` bigint COMMENT '活跃次数'
) COMMENT '最近连续三周活跃用户数'
row format delimited fields terminated by '\t'
location '/warehouse/gmall/ads/ads_continuity_wk_count';

```

#### 2.7.2 导入

```sql
insert into table ads_continuity_wk_count
    SELECT 
    	'2020-12-08',
    	concat(date_sub(next_day('2020-12-08','MO'),21),'_','2020-12-08'),
    	count(*) 
    from
    (SELECT 
    	mid_id
    from
    (--求本周的周活用户
    SELECT 
    	mid_id 
    from dwt_uv_topic 
    where login_date_last >= date_sub(next_day('2020-12-08','MO'),7)
    union all
    --求上周的周活用户
    SELECT 
    	mid_id
    from dws_uv_detail_daycount 
    where dt BETWEEN  date_sub(next_day('2020-12-08','MO'),14) and date_sub(next_day('2020-12-08','MO'),8)
    GROUP by mid_id 
     union all
     --求上上周的周活用户
    SELECT 
    	mid_id
    from dws_uv_detail_daycount 
    where dt BETWEEN  date_sub(next_day('2020-12-08','MO'),21) and date_sub(next_day('2020-12-08','MO'),15)
    GROUP by mid_id ) tmp
    GROUP by mid_id 
    having count(*)=3) tmp2;

```

### 2.8 最近七天内连续三天活跃用户数

连续三天



连续的数据都有以下特征：

A列，以a为初始值，每次递增x,连续递增;

B列，以b为初始值，每次递增y,连续递增;



此时A，B两列差值连续两行的差值，总是相等的！

| A    | B    | A-B        | A-B列相邻两行的差值 |
| ---- | ---- | ---------- | ------------------- |
| a    | b    | a-b        | x-y                 |
| a+x  | b+y  | a-b+(x-y)  | x-y                 |
| a+2x | b+2y | a-b+2(x-y) | ...                 |

如果A,B两列的增量一样， X=Y，此时A-B列相邻两行的差值总为0！



依照此规律，求连续3天，可以这样求

| ID   | 日期A      | 参照列B row_number | A-B        |
| ---- | ---------- | ------------------ | ---------- |
| 1    | 2020-08-10 | 1                  | 2020-08-09 |
| 1    | 2020-08-11 | 2                  | 2020-08-09 |
| 1    | 2020-08-12 | 3                  | 2020-08-09 |

只需要找一个和A列 增量一致的参照列!

做差！

根据ID和差值分组，分组后统计图内数据的行数 >=3 即复合连续3天！

#### 2.8.1 建表

```sql
drop table if exists ads_continuity_uv_count;
create external table ads_continuity_uv_count( 
    `dt` string COMMENT '统计日期',
    `wk_dt` string COMMENT '最近7天日期',
    `continuity_count` bigint
) COMMENT '最近七天内连续三天活跃用户数'
row format delimited fields terminated by '\t'
location '/warehouse/gmall/ads/ads_continuity_uv_count';

```

#### 2.8.2 导入

```sql
insert into table ads_continuity_uv_count
select 
	'2020-12-08',
	concat(date_sub('2020-12-08',7),'_','2020-12-08'),
	count(*) 
from
(select 
	mid_id
from
(select 
	 mid_id 
from
(select
	mid_id ,dt ,rn , date_sub(dt,rn) diff_value
from
(SELECT 
--将取到的数据按照用户id进行分组,将每个用户活跃的时间，按照dt进行升序排序，排序后，求参照列row_number,及日期和参照列的差值
	mid_id , dt , ROW_NUMBER() over(PARTITION by mid_id order by dt  )  rn 
from dws_uv_detail_daycount 
--求最近7天所有用户的活跃记录
where dt > date_sub('2020-12-08',7) ) t1 ) t2
GROUP by mid_id ,diff_value
-- 符合连续3天的
having count(*) >= 3 ) t3
GROUP by mid_id ) t4;
```

## 3.会员主题

### 3.1 会员信息

会员： userid进行标识



会员活跃率：  活跃会员数 /  会员总数

会员付费率：  总付费会员数 /  会员总数

会员新鲜度： 新增用户数  / 活跃用户数

#### 3.1.1建表

```sql
drop table if exists ads_user_topic;
create external table ads_user_topic(
    `dt` string COMMENT '统计日期',
    `day_users` string COMMENT '活跃会员数',
    `day_new_users` string COMMENT '新增会员数',
    `day_new_payment_users` string COMMENT '新增消费会员数',
    `payment_users` string COMMENT '总付费会员数',
    `users` string COMMENT '总会员数',
    `day_users2users` decimal(16,2) COMMENT '会员活跃率',
    `payment_users2users` decimal(16,2) COMMENT '会员付费率',
    `day_new_users2users` decimal(16,2) COMMENT '会员新鲜度'
) COMMENT '会员信息表'
row format delimited fields terminated by '\t'
location '/warehouse/gmall/ads/ads_user_topic';

```

#### 3.1.2 导入

```sql
INSERT into table ads_user_topic
    SELECT 
    	'2020-12-08',
    	sum(if(login_date_last = '2020-12-08',1,0)) day_users ,
    	sum(if(login_date_first = '2020-12-08',1,0)) day_new_users ,
    	sum(if(payment_date_first = '2020-12-08',1,0)) day_new_payment_users,
    	sum(if(payment_date_first is not null,1,0)) payment_users,
    	count(*) users,
    	 cast(sum(if(login_date_last = '2020-12-08',1,0)) / count(*) * 100 as decimal(16,2) ) day_users2users,
    	 cast(sum(if(payment_date_first is not null,1,0)) / count(*) * 100 as decimal(16,2) ) payment_users2users,
    	 cast(sum(if(login_date_first = '2020-12-08',1,0)) / sum(if(login_date_last = '2020-12-08',1,0)) * 100 as decimal(16,2) ) 	
    from dwt_user_topic ;
```



### 3.2 ads_user_topic

漏斗分析： 用来分析转换率！

#### 3.2.1建表

```sql
drop table if exists ads_user_action_convert_day;
create external  table ads_user_action_convert_day(
    `dt` string COMMENT '统计日期',
    `home_count`  bigint COMMENT '浏览首页人数',
    `good_detail_count` bigint COMMENT '浏览商品详情页人数',
    `home2good_detail_convert_ratio` decimal(16,2) COMMENT '首页到商品详情转化率',
    `cart_count` bigint COMMENT '加入购物车的人数',
    `good_detail2cart_convert_ratio` decimal(16,2) COMMENT '商品详情页到加入购物车转化率',
    `order_count` bigint     COMMENT '下单人数',
    `cart2order_convert_ratio`  decimal(16,2) COMMENT '加入购物车到下单转化率',
    `payment_amount` bigint     COMMENT '支付人数',
    `order2payment_convert_ratio` decimal(16,2) COMMENT '下单到支付的转化率'
) COMMENT '漏斗分析'
row format delimited  fields terminated by '\t'
location '/warehouse/gmall/ads/ads_user_action_convert_day/';

```

#### 3.2.2 导入

求商城宏观的转换率

```
insert into table ads_user_action_convert_day
SELECT 
    	'2020-12-08' dt,
    	home_count,
    	good_detail_count,
    	nvl(cast(good_detail_count / home_count * 100 as decimal(16,2)),0.0) home2good_detail_convert_ratio,
    	cart_count,
    	nvl(cast(cart_count / good_detail_count * 100 as decimal(16,2)),0.0)  good_detail2cart_convert_ratio,
    	order_count,
    	nvl(cast(order_count / cart_count * 100 as decimal(16,2)),0.0) cart2order_convert_ratio,
    	payment_amount,
    	nvl(cast(payment_amount / order_count * 100 as decimal(16,2)),0.0) order2payment_convert_ratio 
from
(select
	sum(if(cart_count  > 0 ,1,0)) cart_count,
	sum(if(order_count  > 0 ,1,0)) order_count,
	sum(if(payment_count  > 0 ,1,0)) payment_amount
-- 粒度： 一个user是一条
from dws_user_action_daycount 
where dt='2020-12-08' ) t1
join
(
select
	count(*) home_count
from
(SELECT 
	user_id
from dwd_page_log 
--home_count
where dt='2020-12-08' and page_id ='home' 
GROUP by user_id ) tmp ) t2
join
(
select
	count(*) good_detail_count
from
(SELECT 
	user_id
from dwd_page_log 
--home_count
where dt='2020-12-08' and page_id ='good_detail' 
GROUP by user_id ) tmp2 ) t3;
```



```sql
insert into table ads_user_action_convert_day
SELECT 
    	'2020-12-08' dt,
    	home_count,
    	good_detail_count,
    	nvl(cast(good_detail_count / home_count * 100 as decimal(16,2)),0.0) home2good_detail_convert_ratio,
    	cart_count,
    	nvl(cast(cart_count / good_detail_count * 100 as decimal(16,2)),0.0)  good_detail2cart_convert_ratio,
    	order_count,
    	nvl(cast(order_count / cart_count * 100 as decimal(16,2)),0.0) cart2order_convert_ratio,
    	payment_amount,
    	nvl(cast(payment_amount / order_count * 100 as decimal(16,2)),0.0) order2payment_convert_ratio 
from
(SELECT

	count(*) payment_amount
from dws_user_action_daycount 
where dt='2020-12-08' and payment_count > 0 and order_count > 0) t3
   join
--加入到购物车，最终下单了
(SELECT
	
	count(*) order_count
from dws_user_action_daycount 
where dt='2020-12-08' and cart_count > 0 and order_count > 0 ) t4
    JOIN 
(SELECT
	count(*) home_count
from
(SELECT 
	user_id
from dwd_page_log 
--home_count
where dt='2020-12-08' and page_id ='home'
GROUP by user_id  ) tmp ) t5
join
(
--求 从首页跳转到 商品详情页人数
SELECT
	count(*) good_detail_count
from
(SELECT 
	user_id
from dwd_page_log 
where dt='2020-12-08' and page_id ='good_detail' and last_page_id ='home'
GROUP by user_id  ) tmp ) t6
join
(
select 
	count(*)  cart_count
from
(SELECT 
	--跳转到购物车的人
	 user_id
from dwd_page_log 
where dt='2020-12-08' and page_id ='cart' and last_page_id ='good_detail'
GROUP by user_id  ) t1
join 
(
--今天加购的人求出
select
	user_id 
from dws_user_action_daycount 
where dt='2020-12-08' and cart_count > 0
) t2
on t1.user_id = t2.user_id ) t7;
```

## 4.商品主题

### 4.1商品信息

#### 4.1.1 建表

```sql
drop table if exists ads_product_info;
create external table ads_product_info(
    `dt` string COMMENT '统计日期',
    `sku_num` string COMMENT 'sku个数',
    `spu_num` string COMMENT 'spu个数'
) COMMENT '商品个数信息'
row format delimited fields terminated by '\t'
location '/warehouse/gmall/ads/ads_product_info';

```

#### 4.1.2 导入

```sql
INSERT into table ads_product_info
SELECT 
	'2020-12-08',
	sku_num,spu_num
from
(SELECT 
	count(*) sku_num
from dwt_sku_topic ) t1
join
(
select
	count(*) spu_num
from
(SELECT 
	spu_id 
from dwt_sku_topic 
group by spu_id  ) t ) t2;
```

### 4.2 商品销量排名

#### 4.2.1 建表

```sql
drop table if exists ads_product_sale_topN;
create external table ads_product_sale_topN(
    `dt` string COMMENT '统计日期',
    `sku_id` string COMMENT '商品ID',
    `payment_amount` bigint COMMENT '销量'
) COMMENT '商品销量排名'
row format delimited fields terminated by '\t'
location '/warehouse/gmall/ads/ads_product_sale_topN';

```

#### 4.2.1 导入

```sql
--N=10 求每日   销量(当日？ 累积的总销量？) 排名前十的商品  
-- 求每日销量排名前十  或   求每日, 销量排名前十
insert into table ads_product_sale_topN
select
	dt,
	sku_id ,payment_num 
from dws_sku_action_daycount 
where dt='2020-12-08' and
--将有销量的商品过滤
payment_num > 0
order by payment_num desc
limit 10;
```



### 4.3 商品收藏排名

#### 4.3.1 建表

```sql
drop table if exists ads_product_favor_topN;
create external table ads_product_favor_topN(
    `dt` string COMMENT '统计日期',
    `sku_id` string COMMENT '商品ID',
    `favor_count` bigint COMMENT '收藏量'
) COMMENT '商品收藏排名'
row format delimited fields terminated by '\t'
location '/warehouse/gmall/ads/ads_product_favor_topN';

```

#### 4.3.2 导入

```sql
-- 求每日收藏量排名前十 
insert into table ads_product_favor_topN
select
	dt,
	sku_id ,favor_count 
from dws_sku_action_daycount 
where dt='2020-12-08' and
--将有销量的商品过滤
favor_count > 0
order by favor_count desc
limit 10;
```



### 4.3 商品加入购物车排名

#### 4.3.1 建表

```sql
drop table if exists ads_product_cart_topN;
create external table ads_product_cart_topN(
    `dt` string COMMENT '统计日期',
    `sku_id` string COMMENT '商品ID',
    `cart_count` bigint COMMENT '加入购物车次数'
) COMMENT '商品加入购物车排名'
row format delimited fields terminated by '\t'
location '/warehouse/gmall/ads/ads_product_cart_topN';

```

#### 4.3.2 导入

```sql
-- 求每日收藏量排名前十 
insert into table ads_product_cart_topN
select
	dt,
	sku_id ,cart_count 
from dws_sku_action_daycount 
where dt='2020-12-08' and
--将有销量的商品过滤
cart_count > 0
order by cart_count desc
limit 10;
```

### 4.4 商品退款率排名（最近30天）

#### 4.4.1 建表

```sql
drop table if exists ads_product_refund_topN;
create external table ads_product_refund_topN(
    `dt` string COMMENT '统计日期',
    `sku_id` string COMMENT '商品ID',
    `refund_ratio` decimal(16,2) COMMENT '退款率'
) COMMENT '商品退款率排名'
row format delimited fields terminated by '\t'
location '/warehouse/gmall/ads/ads_product_refund_topN';

```

#### 4.4.2 导入

```sql
insert into table ads_product_refund_topN
select
	'2020-12-08' dt,
	sku_id ,
	cast(refund_last_30d_count / payment_last_30d_count * 100 as decimal(16,2)) refund_ratio
from dwt_sku_topic 
where payment_last_30d_count > 0 and refund_last_30d_count > 0
order by refund_ratio desc
LIMIT 10;
```



### 4.5 商品差评率排名

#### 4.5.1 建表

```sql
drop table if exists ads_appraise_bad_topN;
create external table ads_appraise_bad_topN(
    `dt` string COMMENT '统计日期',
    `sku_id` string COMMENT '商品ID',
    `appraise_bad_ratio` decimal(16,2) COMMENT '差评率'
) COMMENT '商品差评率'
row format delimited fields terminated by '\t'
location '/warehouse/gmall/ads/ads_appraise_bad_topN';

```



#### 4.5.2 导入

```sql
insert into table ads_appraise_bad_topN
select
	'2020-12-08' dt,
	sku_id ,
	cast(appraise_bad_count / (appraise_bad_count + appraise_good_count + appraise_mid_count + appraise_default_count) * 100 as decimal(16,2)) appraise_bad_ratio
from dwt_sku_topic 
where appraise_bad_count > 0
order by appraise_bad_ratio desc
LIMIT 10;
```



## 5.营销主题

### 5.1 下单数目统计

#### 5.1.1 建表

```sql
drop table if exists ads_order_daycount;
create external table ads_order_daycount(
    dt string comment '统计日期',
    order_count bigint comment '单日下单笔数',
    order_amount bigint comment '单日下单金额',
    order_users bigint comment '单日下单用户数'
) comment '下单数目统计'
row format delimited fields terminated by '\t'
location '/warehouse/gmall/ads/ads_order_daycount';
```



#### 5.1.2 导入

```sql
insert into table ads_order_daycount
select
	'2020-12-08',
	sum(order_count) ,
	sum( order_amount),
	sum(if(order_count > 0 ,1, 0))
from dws_user_action_daycount 
where dt='2020-12-08';
```



### 5.2 支付信息统计

#### 5.2.1 建表

```sql
drop table if exists ads_payment_daycount;
create external table ads_payment_daycount(
    dt string comment '统计日期',
    order_count bigint comment '单日支付笔数',
    order_amount bigint comment '单日支付金额',
     payment_sku_count bigint comment '单日支付商品数',
    payment_user_count bigint comment '单日支付人数',
    payment_avg_time decimal(16,2) comment '下单到支付的平均时长，取分钟数'
) comment '支付信息统计'
row format delimited fields terminated by '\t'
location '/warehouse/gmall/ads/ads_payment_daycount';
```



#### 5.2.2 导入

```sql
insert into table ads_payment_daycount
select 
	dt,order_count,order_amount,payment_user_count,payment_sku_count,payment_avg_time
from
(select
	'2020-12-08' dt,
	sum(payment_count) order_count,
	sum( payment_amount) order_amount,
	sum(if(payment_count > 0 ,1, 0)) payment_user_count
from dws_user_action_daycount 
where dt='2020-12-08') t1
join
(select
	count(*) payment_sku_count
from dws_sku_action_daycount 
where dt='2020-12-08' and payment_count > 0 ) t2
join
(SELECT 
   --平均时长 =  下单到支付的总时长 / 支付的单数
   --cast(sum( unix_timestamp(payment_time) - unix_timestamp(create_time ) ) / 60 / count(*)  as decimal(16,2)),
  cast( avg(unix_timestamp(payment_time) - unix_timestamp(create_time )) / 60  as decimal(16,2) ) payment_avg_time
from dwd_fact_order_info 
--存在跨天支付
where (dt='2020-12-08' or dt=date_sub('2020-12-08',1))
--取2020-12-08支付的订单
and '2020-12-08'=date_format(payment_time ,'yyyy-MM-dd') ) t3;
```



### 5.3 品牌复购率

统计的是一个品牌在当前月的复够率！

复购率: 商品被重复购买的频率！

单次复购率：  两次及以上购买人数 /  购买过的总人数

多次复购率： 三次及以上购买人数 /  购买过的总人数

#### 5.3.1 建表

```sql
drop table ads_sale_tm_category1_stat_mn;
--粒度？  一个品牌是一行(需要聚合) 还是 一个品牌的一个一级品类是一行？
create external table ads_sale_tm_category1_stat_mn
(  
    -- tm_id 
    tm_id string comment '品牌id',
    category1_id string comment '1级品类id ',
    category1_name string comment '1级品类名称 ',
    -- 用户支付商品的信息  dwd_fact_order_detail  join dwd_fact_payment_info 取购买的订单的商品和用户信息
    buycount   bigint comment  '购买人数',
    buy_twice_last bigint  comment '两次以上购买人数',
    buy_twice_last_ratio decimal(16,2)  comment  '单次复购率',
    buy_3times_last   bigint comment   '三次以上购买人数',
    buy_3times_last_ratio decimal(16,2)  comment  '多次复购率',
    stat_mn string comment '统计月份',
    stat_date string comment '统计日期' 
) COMMENT '品牌复购率统计'
row format delimited fields terminated by '\t'
location '/warehouse/gmall/ads/ads_sale_tm_category1_stat_mn/';

```

#### 5.3.2 导入

```sql
insert into table ads_sale_tm_category1_stat_mn
	select 
		tm_id,category1_id,category1_name,
		sum(if(buytimes > 0 ,1 ,0)) buycount,
		sum(if(buytimes > 1 ,1 ,0)) buy_twice_last,
		cast(sum(if(buytimes > 1 ,1 ,0)) / sum(if(buytimes > 0 ,1 ,0)) * 100 as decimal(16,2) ) buy_twice_last_ratio,
		sum(if(buytimes > 2 ,1 ,0))  buy_3times_last,
		cast(sum(if(buytimes > 2 ,1 ,0)) / sum(if(buytimes > 0 ,1 ,0)) * 100 as decimal(16,2) ) buy_3times_last_ratio,
		date_format('2020-12-08','yyyy-MM'),
		'2020-12-08'
	from
    (select 
    --求每个用户，当月各购买每个品牌多少次
    	user_id,tm_id,category1_id,category1_name,count(*) buytimes
    	-- 统计月品牌复购率，先将当前月，所有用户购买所有商品的记录求出，根据商品id关联出其品牌id
    from
    (select
    	user_id ,sku_id
    from
    --统计当前月所有用户下单的商品信息
    (select
    	user_id ,sku_id ,order_id
    from dwd_fact_order_detail 
    where date_format(dt,'yyyy-MM') = date_format('2020-12-08','yyyy-MM') 
    --存在跨天支付，取上个月最后一天
    	or dt=add_months(last_day('2020-12-08'),-1)
    ) t1 join
    (--求哪些订单被支付了，也就是商品被购买了
    SELECT 
    	order_id 
    from dwd_fact_payment_info 
    where date_format(dt,'yyyy-MM') = date_format('2020-12-08','yyyy-MM') ) t2
    on t1.order_id = t2.order_id ) t5
    join
    (
    SELECT 
    	id,t3.tm_id,category1_id,category1_name
    from
    (SELECT 
    	id,tm_id 
    from dwd_dim_sku_info 
    where dt='2020-12-08') t3
    left join
    (  
    SELECT 
    	tm_id ,concat_ws('|',collect_set(category1_id)) category1_id,
    	concat_ws('|',collect_set(category1_name))  category1_name
    from dwd_dim_sku_info 
    where dt='2020-12-08'
    group by tm_id  ) t4
    on t3.tm_id = t4.tm_id ) t6
    on t5.sku_id = t6.id
    GROUP by user_id,tm_id,category1_id,category1_name ) tmp
    group by tm_id,category1_id,category1_name;
```

## 6.地区主题

### 6.1 地区主题信息

#### 6.1.1 建表

```sql
drop table if exists ads_area_topic;
create external table ads_area_topic(
    `dt` string COMMENT '统计日期',
    `id` bigint COMMENT '编号',
    `province_name` string COMMENT '省份名称',
    `area_code` string COMMENT '地区编码',
    `iso_code` string COMMENT 'iso编码',
    `region_id` string COMMENT '地区ID',
    `region_name` string COMMENT '地区名称',
    `login_day_count` bigint COMMENT '当天活跃设备数',
    `order_day_count` bigint COMMENT '当天下单次数',
    `order_day_amount` decimal(16,2) COMMENT '当天下单金额',
    `payment_day_count` bigint COMMENT '当天支付次数',
    `payment_day_amount` decimal(16,2) COMMENT '当天支付金额'
) COMMENT '地区主题信息'
row format delimited fields terminated by '\t'
location '/warehouse/gmall/ads/ads_area_topic/';

```

#### 6.1.2 导入

```sql
INSERT into table ads_area_topic  
SELECT 
	'2020-12-08',
	id, province_name, area_code, iso_code, region_id, region_name, 
	login_day_count, order_day_count, order_day_amount, payment_day_count, payment_day_amount
from dwt_area_topic ;
```

