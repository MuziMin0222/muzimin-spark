# MuziMinSpark  
  用于spark大数据开发框架，让开发人员更加注重业务逻辑，无需关注数据的读取，写入，只需在意dataframe/dateset之间的转换

# 如何在idea中运行示例代码
在本地运行时，需要将所有的文件的路径写成绝对/相对路径
1. VM options中添加
```$xslt
-Dspark.master=local[*]
-Dspark.serializer=org.apache.spark.serializer.KryoSerializer
```
2. Program argument中添加配置文件的位置或者Json配置数据
```$xslt
-c conf/config.yaml
或者
-j {"aaa":"bbb"}
```

3. 运行Job前需要指定两个文件，config.yaml以及step.yaml，因为在本地运行，所以指定文件路径的时候弄成相对路径或者绝对路径，可以让程序访问到即可

### 如何配置config.yaml（可以是其他名称，只需要是yaml文件即可）

1. 开头先指定好step.yaml的路径，相对路径或者绝对路径，step.yaml可以有多个，steps参数是必选项，如果没有，程序将退出

```
steps:
  - conf/step.yaml
  - conf/step_2.yaml
```

2. 配置输入的数据源（目前只实现了file）
   1. 配置输入源是csv文件，ratings是将csv转为DataFrame后注册的表名，该名称可任取；
   2. file表示该数据源类型是文件；
   3. path可以是一个文件的路径，也可以是一些文件所在的文件夹，该参数选型是必选的；
   4. format表示该文件的格式，可不填写，默认是csv格式；
   5. options表示读取该文件的配置项，比如csv文件是否将表头作为dataframe的schema，当读取csv文件时，默认的配置项是
      1. 逗号分隔符
      2. 字段存在双引号进行转义
      3. 用双引号作为包裹符号处理
      4. 对csv文件进行全包裹
      5. 将csv第一行数据作为DataFrame的表头

```yaml
inputs:
  ratings:
    file:
      path: examples/file_inputs
      format: csv
      options:
        delimiter: "\u0001"
        nullValue: "空值"
```

3. showPreviewLines，非必选值，默认是0；表示将处理后的DataFrame，show出来多少行
4. showQuery：是否将sql语句进行输出控制台；非必选值
5. cacheOnPreview：非必选值，表示是否将DataFrame进行缓存（该缓存级别是内存级）
6. config.yaml的完整样例

```yaml
steps:
  - conf/step.yaml

inputs:
  ratings:
    file:
      path: examples/file_inputs
      format: csv
      options:
        delimiter: "\u0001"
        nullValue: "空值"

showPreviewLines: 10

showQuery: true

cacheOnPreview: true
```

### 如果配置step.yaml文件

1. 以steps开始的数组
   1. dataFrameName：表示处理后注册得到的临时表，亦或者是UDF的名称
   2. sql：表示sql语句；
   3. file：表示sql所在的sql文件；
   4. classpath：表示继承com.muzimin.job.RichProcessJob的子类，该子类中可以使用代码进行数据操作；
   5. params：表示一些自定义变量，由key-value组成

```yaml
steps:
- dataFrameName: moviesWithRatings
  sql:
    SELECT userid,
           movieid,
           rating,
           timestamp,
           'demo' as title,
           '111' as genres
    FROM ratings
- dataFrameName: fantasyMoviesWithRatings
  sql:
    SELECT movieId,
           cast(rating AS float) AS rating,
           timestamp,
           title,
           genres
    FROM moviesWithRatings
- dataFrameName: topFantasyMovies
  file: examples/sqlFile/topFantasyMovies.sql

- dataFrameName: table3
  classpath: com.muzimin.job.mycode.Demo1
  params:
    param1: 20210101
    param2: 20220101
```



# 如何在服务器上通过spark-submit提交任务

在服务器运行时，只需要将文件名定义好就行
