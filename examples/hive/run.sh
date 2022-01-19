#!/bin/bash
base_dir=$(dirname $(pwd))

jobName=$1

jar_str=""
for jar in `ls ${base_dir}/lib/*.jar`
do
  jar_str=${jar_str},$jar
done

echo "拼接的jar包路径：${jar_str#*,} "

conf_str=""
for file in `ls ${base_dir}/conf/${jobName}/*`
do
  conf_str=${conf_str},${file}
done

echo "拼接文件的路径：${conf_str}"

concat_log(){
  echo ${base_dir}/logs/$1.log
}

spark2-submit \
--master yarn \
--deploy-mode cluster \
--jars ${jar_str} \
--files ${conf_str} \
--name ${jobName} \
--class com.muzimin.Application ${base_dir}/jar/MuziMinSpark-1.0-SNAPSHOT.jar \
-c config.yaml \
> `concat_log ${jobName}` 2>&1

if [[ $? -eq 0 ]]; then
    echo "任务成功！"
else
    echo "任务失败。。。。"
fi