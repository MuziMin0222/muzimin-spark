# MuziMinSpark  
  用于spark大数据开发框架，让开发人员更加注重业务逻辑，无需关注数据的读取，写入，只需在意dataframe/dateset之间的转换

# 代码执行步骤说明
  1. 通过ConfigurationParser对象将-c传进来的Yaml文件解析成字符串或者-j传进行的JSON字符串映射为Configuration对象。  

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

# 如何在服务器上通过spark-submit提交任务
在服务器运行时，只需要将文件名定义好就行

# 关于config.yaml(可以是其他名字，只需要是yaml文件)
## 配置input
### 当读取的是文件/目录时
默认读取的文件类型是csv格式数据
```yaml
inputs:
  ratings:
    file:
      # path一定要配置，可以是一个目录，可以是一个文件
      path: examples/file_inputs
      # format可以不配置，默认读取的是csv格式，format的选项有csv，parquet，json，jsonl，excel
      format: csv
      # 读取的文件，是否配置schema，可以配置，也可以不配置
      schemaPath: schema/schema.json
      # 文件的选项配置，可以不配置，比如csv的包裹腹，分隔符，头文件是否作为列名等，目前该选项只有csv有默认配置
      options:
         quoteAll: false
         header: \t
```

