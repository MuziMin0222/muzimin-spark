#!/bin/bash
kinit -kt /home/cdp_etl/cdp_etl.keytab
base_dir="/home/cdp_etl/batch/lhm/muziminspark"

jar_str=""
for jar in `ls ${base_dir}/lib/*.jar`
do
  jar_str=${jar_str},$jar
done

echo "拼接的jar包路径：${jar_str#*,} "

conf_str=""
for file in `ls ${base_dir}/conf/*`
do
  conf_str=${conf_str},${file}
done

echo "拼接文件的路径：${conf_str}"

concat_log(){
  echo ${base_dir}/logs/$1.log
}

spark2-submit \
--queue root.users.cdp_etl \
--master yarn \
--deploy-mode client \
--jars ${jar_str} \
--files ${conf_str} \
--class com.muzimin.Application ${base_dir}/jar/MuziMinSpark-1.0-SNAPSHOT.jar \
-c config.yaml \
> `concat_log $1` 2>&1

if [[ $? -eq 0 ]]; then
    echo "任务成功！"
else
    echo "任务失败。。。。"
fi