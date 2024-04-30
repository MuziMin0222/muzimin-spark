# MuziMinSpark  
受https://github.com/YotpoLtd/metorikku  该框架启发以及改造而成，添加新功能并缩减实时功能  

用于spark大数据开发框架，让开发人员更加注重业务逻辑，无需关注数据的读取，写入，只需在意dataframe/dateset之间的转换

# 版本
## 版本v1.0
1. 该版本只支持yarn cluster和本地调试使用
2. 需要配置两个yaml文件

## 版本v2.0
1. 该版本只支持yarn cluster和本地调试使用
2. 只需要使用一个yaml配置文件即可
3. 内置了时间参数，可以通过变量方式给到配置文件，使用${}进行引用
4. 添加了全局配置文件，配置文件中的值，可以添加到yaml文件中，使用${}进行引用
