# MuziMinSpark  
  用于spark大数据开发框架，让开发人员更加注重业务逻辑，无需关注数据的读取，写入，只需在意dataframe/dateset之间的转换

# 代码执行步骤说明
  1. 通过ConfigurationParser对象将-c传进来的Yaml文件解析成字符串或者-j传进行的JSON字符串映射为Configuration对象。  

  2. 如果配置了influxdb，就将influxdb的配置放入spark监听器中

  3. 配置了spark catalog中的数据库，进行创建数据库和使用数据库操作

     
未完成。。。待续
