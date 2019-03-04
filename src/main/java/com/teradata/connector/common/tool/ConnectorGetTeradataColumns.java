package com.teradata.connector.common.tool;

import com.teradata.connector.common.exception.ConnectorException;
import com.teradata.connector.common.utils.ConnectorStringUtils;
import com.teradata.connector.teradata.db.TeradataConnection;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import java.util.HashMap;
import java.util.Map;

/**
 * @Project:
 * @Description:
 * @Version 1.0.0
 * @Throws SystemException:
 * @Author: <li>2019/2/21/021 Administrator Create 1.0
 * @Copyright ©2018-2019 al.github
 * @Modified By:
 */
public class ConnectorGetTeradataColumns extends Configured implements Tool {
    private static Log logger = LogFactory.getLog(ConnectorGetTeradataColumns.class);
    private String className = "com.teradata.jdbc.TeraDriver";
    private String url;
    private String userName;
    private String password;
    private boolean useXview=false;
    private String tableName;

    public ConnectorGetTeradataColumns(){
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
        if (configurationMap.containsKey("-url")) {
            url = (String) configurationMap.remove("-url");
        }else{
            logger.error("user provided url for Teradata");
            throw new Exception("user provided url for Teradata");
        }

        if (configurationMap.containsKey("-username")) {
            userName = (String) configurationMap.remove("-username");
        }else{
            logger.error("user provided username for Teradata");
            throw new Exception("user provided username for Teradata");
        }

        if (configurationMap.containsKey("-password")) {
            password = (String) configurationMap.remove("-password");
        }else{
            logger.error("user provided password for Teradata");
            throw new Exception("user provided password for Teradata");
        }

        if (configurationMap.containsKey("-tablename")) {
            tableName = (String) configurationMap.remove("-tablename");
        }else{
            logger.error("user provided tablename for Teradata");
            throw new Exception("user provided tablename for Teradata");
        }

        if (!configurationMap.isEmpty()) {
            String unrecognizedParams = "";
            for (Object key : configurationMap.keySet()) {
                unrecognizedParams = unrecognizedParams + key + " ";
            }
            throw new ConnectorException(15008, new Object[]{unrecognizedParams});
        }

        TeradataConnection connection = new TeradataConnection(className, url, userName, password, useXview);
        String[] listColumns = connection.getListColumns(tableName);
        StringBuilder sb=new StringBuilder();
        for (String column:listColumns){
            sb.append(column+",");
        }
        int pos = sb.length() - 1;
        sb.deleteCharAt(pos);
        logger.info(String.format("The Columns of Teradata Table %s is:%s",tableName,sb));
        connection.close();
        return 0;
    }

    public static void main(String[] args) {
        int res = 1;
        try {
            long start = System.currentTimeMillis();
            logger.info("ConnectorGetTeradataColumns starts at " + start);

            res = ToolRunner.run(new ConnectorGetTeradataColumns(), args);

            long end = System.currentTimeMillis();
            logger.info("ConnectorGetTeradataColumns ends at " + end);
            logger.info("ConnectorGetTeradataColumns time is " + (end - start) / 1000L + "s");
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
