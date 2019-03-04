# teradata-connector

**对TDCH的扩展功能如下：**
 1. 增加指定Hive分区导出功能，参数：-sourcepartitionpath。 其中支持分区字段匹配模式，如：**/*2019*
 2. 完善和修复TDCH对Hive Parquet表decimal类型字段的解析支持。
 3. 增加自动获取Hive表字段信息功能(分区字段除外)。
 4. 后续增加Teradata数据表字段的自动获取功能。【期待】
 5. 后续增加对HDFS文件按大小进行分拣合并。【期待】
 6. 后续增加FTP服务功能，直接将HDFS文件放到FTP服务器上，也或者从FTP服务器直接放到HDFS文件系统上【期待】

##  使用样例
### Hive orcfile to Teradata 
```
export HIVE_LIB_JARS=$HIVE_HOME/lib/hive-cli-1.1.0-cdh5.13.0.jar,\
$HIVE_HOME/lib/hive-exec-1.1.0-cdh5.13.0.jar,\
$HIVE_HOME/lib/hive-metastore-1.1.0-cdh5.13.0.jar,\
$HIVE_HOME/lib/libfb303-0.9.3.jar,\
$HIVE_HOME/lib/libthrift-0.9.3.jar,\
$HIVE_HOME/lib/jdo-api-3.0.1.jar,\
$HIVE_HOME/lib/antlr-runtime-3.4.jar,\
$HIVE_HOME/lib/parquet-column-1.8.2.jar,\
$HIVE_HOME/lib/parquet-common-1.8.2.jar,\
$HIVE_HOME/lib/parquet-encoding-1.8.2.jar,\
$HIVE_HOME/lib/parquet-format-2.3.1.jar,\
$HIVE_HOME/lib/parquet-hadoop-1.8.2.jar,\
$HIVE_HOME/lib/parquet-jackson-1.8.2.jar

export HADOOP_CLASSPATH=$HIVE_HOME/conf:\
$HIVE_HOME/lib/commons-dbcp-1.4.jar:\
$HIVE_HOME/lib/commons-pool-1.5.4.jar:\
$HIVE_HOME/lib/datanucleus-core-3.2.10.jar:\
$HIVE_HOME/lib/datanucleus-rdbms-3.2.9.jar:\
$HIVE_HOME/lib/hive-cli-1.1.0-cdh5.13.0.jar:\
$HIVE_HOME/lib/hive-exec-1.1.0-cdh5.13.0.jar:\
$HIVE_HOME/lib/hive-metastore-1.1.0-cdh5.13.0.jar:\
$HIVE_HOME/lib/libfb303-0.9.3.jar:\
$HIVE_HOME/lib/libthrift-0.9.3.jar:\
$HIVE_HOME/lib/antlr-runtime-3.4.jar:\
$HIVE_HOME/lib/hive-common-1.1.0-cdh5.13.0.jar

hadoop jar ./teradata-connector-2.0.jar com.teradata.connector.common.tool.ConnectorExportTool \
-D mapreduce.input.fileinputformat.split.minsize.per.node=1 \
-D mapreduce.input.fileinputformat.split.minsize.per.rack=1 \
-libjars $HIVE_LIB_JARS \
-classname com.teradata.jdbc.TeraDriver \
-url jdbc:teradata://192.168.20.197/DATABASE=pmid3,CHARSET=ASCII,CLIENT_CHARSET=GBK \
-username dbc \
-password dbc \
-jobtype hive \
-fileformat orcfile \
-separator "\\u0001" \
-method batch.insert \
-sourcepaths /user/hive/warehouse/pmid3.db/tb_mid_bll_vc_user_day/deal_date_p=20190217 \
-sourcefieldnames "DEAL_DATE,SYS_TYPE_CODE,USER_ID,USER_NAME" \
-targettable pmid3.TB_MID_BLL_VC_USER_DAY \
-targetfieldnames "DEAL_DATE,SYS_TYPE_CODE,USER_ID,USER_NAME"
```

### Hive parquet file to Teradata
```
export HIVE_LIB_JARS=$HIVE_HOME/lib/hive-cli-1.1.0-cdh5.13.0.jar,\
$HIVE_HOME/lib/hive-exec-1.1.0-cdh5.13.0.jar,\
$HIVE_HOME/lib/hive-metastore-1.1.0-cdh5.13.0.jar,\
$HIVE_HOME/lib/libfb303-0.9.3.jar,\
$HIVE_HOME/lib/libthrift-0.9.3.jar,\
$HIVE_HOME/lib/jdo-api-3.0.1.jar,\
$HIVE_HOME/lib/antlr-runtime-3.4.jar,\
$HIVE_HOME/lib/parquet-column-1.8.2.jar,\
$HIVE_HOME/lib/parquet-common-1.8.2.jar,\
$HIVE_HOME/lib/parquet-encoding-1.8.2.jar,\
$HIVE_HOME/lib/parquet-format-2.3.1.jar,\
$HIVE_HOME/lib/parquet-hadoop-1.8.2.jar,\
$HIVE_HOME/lib/parquet-jackson-1.8.2.jar

export HADOOP_CLASSPATH=$HIVE_HOME/conf:\
$HIVE_HOME/lib/commons-dbcp-1.4.jar:\
$HIVE_HOME/lib/commons-pool-1.5.4.jar:\
$HIVE_HOME/lib/datanucleus-core-3.2.10.jar:\
$HIVE_HOME/lib/datanucleus-rdbms-3.2.9.jar:\
$HIVE_HOME/lib/hive-cli-1.1.0-cdh5.13.0.jar:\
$HIVE_HOME/lib/hive-exec-1.1.0-cdh5.13.0.jar:\
$HIVE_HOME/lib/hive-metastore-1.1.0-cdh5.13.0.jar:\
$HIVE_HOME/lib/libfb303-0.9.3.jar:\
$HIVE_HOME/lib/libthrift-0.9.3.jar:\
$HIVE_HOME/lib/antlr-runtime-3.4.jar:\
$HIVE_HOME/lib/hive-common-1.1.0-cdh5.13.0.jar

hadoop jar ./teradata-connector-2.0.jar com.teradata.connector.common.tool.ConnectorExportTool \
-D mapreduce.input.fileinputformat.split.minsize.per.node=1 \
-D mapreduce.input.fileinputformat.split.minsize.per.rack=1 \
-libjars $HIVE_LIB_JARS \
-classname com.teradata.jdbc.TeraDriver \
-url jdbc:teradata://192.168.20.197/DATABASE=pmid3,CHARSET=ASCII,CLIENT_CHARSET=GBK \
-username dbc \
-password dbc \
-jobtype hive \
-fileformat parquet \
-method batch.insert \
-sourcetable pmid3.tb_mid_bll_vc_user_day_parquet_anke \
-sourcepartitionpath **/*20190215 \
-sourcefieldnames "deal_date,sys_type_code,USER_ID,USER_NAME,should_par" \
-targettable pmid3.tb_mid_bll_vc_user_day_parquet_anke \
-targetfieldnames "DEAL_DATE,SYS_TYPE_CODE,USER_ID,USER_NAME,should_par"
```

### 不指定hive 和 Teradata 字段（分区字段除外）
```
hadoop jar ./teradata-connector-2.0.jar com.teradata.connector.common.tool.ConnectorExportTool \
-D mapreduce.input.fileinputformat.split.minsize.per.node=1 \
-D mapreduce.input.fileinputformat.split.minsize.per.rack=1 \
-libjars $HIVE_LIB_JARS \
-classname com.teradata.jdbc.TeraDriver \
-url jdbc:teradata://192.168.20.197/DATABASE=pmid3,CHARSET=ASCII,CLIENT_CHARSET=GBK \
-username dbc \
-password dbc \
-jobtype hive \
-fileformat parquet \
-method batch.insert \
-sourcetable pmid3.tb_mid_bll_vc_user_day_parquet_anke \
-sourcepartitionpath **/*20190215 \
-sourcefieldnames "*" \
-targettable pmid3.tb_mid_bll_vc_user_day_parquet_anke \
```


### 获取Hive表字段元数据信息
```
hadoop jar ./teradata-connector-2.0.jar com.teradata.connector.common.tool.ConnectorGetHiveColumns \
-libjars $HIVE_LIB_JARS \
-sourcedatabase pmid3 \
-sourcetablename tb_mid_bll_vc_user_day_parquet
```

### 获取Teradata元数据信息
```
hadoop jar ./teradata-connector-2.0.jar com.teradata.connector.common.tool.ConnectorGetTeradataColumns \
-libjars $HIVE_LIB_JARS \
-url jdbc:teradata://192.168.20.197/DATABASE=pmid3,CHARSET=ASCII,CLIENT_CHARSET=GBK \
-username dbc \
-password dbc \
-tablename pmid3.tb_mid_bll_vc_user_day_parquet_anke
```

### fastload方式导入Teradata数据库
```
hadoop jar ./teradata-connector-2.0.jar com.teradata.connector.common.tool.ConnectorExportTool \
-D mapreduce.input.fileinputformat.split.minsize.per.node=1 \
-D mapreduce.input.fileinputformat.split.minsize.per.rack=1 \
-libjars $HIVE_LIB_JARS \
-classname com.teradata.jdbc.TeraDriver \
-url jdbc:teradata://192.168.20.197/DATABASE=pmid3,CHARSET=ASCII,CLIENT_CHARSET=GBK \
-username dbc \
-password dbc \
-jobtype hive \
-fileformat orcfile \
-method internal.fastload
-fastloadsockethost 192.168.0.201
-fastloadsocketport 8678
-sourcetable pmid3.tb_mid_bll_vc_user_day_parquet_anke \
-sourcepartitionpath **/*20190215 \
-sourcefieldnames "deal_date,sys_type_code,USER_ID,USER_NAME,should_par" \
-targettable pmid3.tb_mid_bll_vc_user_day_parquet_anke \
-targetfieldnames "DEAL_DATE,SYS_TYPE_CODE,USER_ID,USER_NAME,should_par"
```