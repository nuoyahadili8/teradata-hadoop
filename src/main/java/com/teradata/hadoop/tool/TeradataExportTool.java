package com.teradata.hadoop.tool;

import com.teradata.connector.common.exception.ConnectorException;
import com.teradata.connector.common.tool.ConnectorExportTool;
import com.teradata.connector.common.utils.ConnectorStringUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;


public class TeradataExportTool {
    private static Log logger;

    public static void main(final String[] args) {
        int res = 1;
        try {
            final long start = System.currentTimeMillis();
            TeradataExportTool.logger.info((Object) ("ConnectorExportTool starts at " + start));
            res = ToolRunner.run((Tool) new ConnectorExportTool(), args);
            final long end = System.currentTimeMillis();
            TeradataExportTool.logger.info((Object) ("ConnectorExportTool ends at " + end));
            TeradataExportTool.logger.info((Object) ("ConnectorExportTool time is " + (end - start) / 1000L + "s"));
        } catch (Throwable e) {
            TeradataExportTool.logger.info((Object) ConnectorStringUtils.getExceptionStack(e));
            if (e instanceof ConnectorException) {
                res = ((ConnectorException) e).getCode();
            } else {
                res = 10000;
            }
        } finally {
            TeradataExportTool.logger.info((Object) ("job completed with exit code " + res));
            if (res > 255) {
                res /= 1000;
            }
            System.exit(res);
        }
    }

    static {
        TeradataExportTool.logger = LogFactory.getLog((Class) ConnectorExportTool.class);
    }
}
