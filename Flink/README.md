
# Flink


基于官方提供的命令来构建Flink项目 

> https://ci.apache.org/projects/flink/flink-docs-release-1.13/docs/dev/datastream/project-configuration/

```
mvn archetype:generate                \
  -DarchetypeGroupId=org.apache.flink   \
  -DarchetypeArtifactId=flink-quickstart-java \
  -DarchetypeVersion=1.13.0
```


设置默认值
```
mvn archetype:generate	 \
 -DarchetypeGroupId=org.apache.flink  \
 -DarchetypeArtifactId=flink-quickstart-java  \
 -DarchetypeVersion=1.13.0 \
 -DgroupId=org.dxtd.quickstart \
 -DartifactId=FlinkDemoStart  \
 -Dversion="0.1" \
 -DinteractiveMode=false 
```
