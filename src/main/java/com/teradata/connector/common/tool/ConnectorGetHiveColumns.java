package com.teradata.connector.common.tool;

import com.teradata.connector.common.exception.ConnectorException;
import com.teradata.connector.common.utils.ConnectorConfiguration;
import com.teradata.connector.common.utils.ConnectorSchemaUtils;
import com.teradata.connector.common.utils.ConnectorStringUtils;
import com.teradata.connector.hcat.utils.HCatPlugInConfiguration;
import com.teradata.connector.hive.processor.HiveInputProcessor;
import com.teradata.connector.hive.utils.HivePlugInConfiguration;
import com.teradata.connector.hive.utils.HiveUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.metastore.HiveMetaStoreClient;
import org.apache.hadoop.hive.metastore.api.FieldSchema;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * @Project:
 * @Description:
 * @Version 1.0.0
 * @Throws SystemException:
 * @Author: <li>2019/2/20/020 Administrator Create 1.0
 * @Copyright ©2018-2019 al.github
 * @Modified By:
 */
public class ConnectorGetHiveColumns extends Configured implements Tool {

    private static Log logger = LogFactory.getLog(ConnectorGetHiveColumns.class);
    private String jobType;
    private String sourceDataBase;
    private String sourceHiveTableName;

    public ConnectorGetHiveColumns(){
        this.jobType = "hdfs";
        this.sourceDataBase = "default";
        this.sourceHiveTableName = "dual";
    }

    @Override
    public int run(String[] args) throws Exception {
        Job job = new Job(getConf());
        Map configurationMap = new HashMap();
        int i = 0;
        int length = args.length;

        if (length < 1) {
            throw new ConnectorException(13020);
        }
        while (i < length) {
            String argStr = args[i];
            if ((argStr == null) || (argStr.isEmpty())) {
                return 0;
            }
            if (argStr.charAt(0) != '-') {
                throw new ConnectorException(13001);
            }
            if ((argStr.equalsIgnoreCase("-h")) || (argStr.equalsIgnoreCase("-help"))) {
                return -1;
            }
            if (++i < length) {
                String value = args[i];
                if (value == null) {
                    throw new ConnectorException(13001);
                }

                configurationMap.put(argStr, value);
                ++i;
            } else {
                throw new ConnectorException(13001);
            }
        }
        Configuration configuration = job.getConfiguration();
        configuration.addResource("teradata-export-properties.xml");
        if (configurationMap.containsKey("-sourcedatabase")) {
            sourceDataBase = (String) configurationMap.remove("-sourcedatabase");
        }else{
            logger.error("user provided sourcedatabase for Hive table");
            throw new Exception("user provided sourcedatabase for Hive table");
        }

        if (configurationMap.containsKey("-sourcetablename")) {
            sourceHiveTableName = (String) configurationMap.remove("-sourcetablename");
        }else{
            logger.error("user provided sourcetablename for Hive table");
            throw new Exception("user provided sourcetablename for Hive table");
        }

        if (!configurationMap.isEmpty()) {
            String unrecognizedParams = "";
            for (Object key : configurationMap.keySet()) {
                unrecognizedParams = unrecognizedParams + key + " ";
            }
            throw new ConnectorException(15008, new Object[]{unrecognizedParams});
        }

        HiveConf hiveConf = new HiveConf(configuration, HiveInputProcessor.class);
        HiveUtils.loadHiveConf(hiveConf, ConnectorConfiguration.direction.input);
        HiveMetaStoreClient client = new HiveMetaStoreClient(hiveConf);
        List<FieldSchema> fields1 = client.getFields(sourceDataBase, sourceHiveTableName);
        logger.info("fields1===========" + fields1.size());
        StringBuilder sb = new StringBuilder();
        for (FieldSchema fieldSchema:fields1){
            sb.append(fieldSchema.getName()+",");
        }
        int pos = sb.length() - 1;
        sb.deleteCharAt(pos);
        logger.info(String.format("The Columns of Hive Table %s is:%s",sourceDataBase+"."+sourceHiveTableName,sb));
        return 0;
    }

    public static void main(String[] args) {
        int res = 1;
        try {
            long start = System.currentTimeMillis();
            logger.info("ConnectorGetHiveColumns starts at " + start);

            res = ToolRunner.run(new ConnectorGetHiveColumns(), args);

            long end = System.currentTimeMillis();
            logger.info("ConnectorGetHiveColumns ends at " + end);
            logger.info("ConnectorGetHiveColumns time is " + (end - start) / 1000L + "s");
        } catch (Throwable e) {
            logger.info(ConnectorStringUtils.getExceptionStack(e));

            if (e instanceof ConnectorException) {
                res = ((ConnectorException) e).getCode();
            } else {
                res = 10000;
            }
        } finally {
            logger.info("job completed with exit code " + res);

            if (res > 255) {
                res /= 1000;
            }
            System.exit(res);
        }
    }
}
