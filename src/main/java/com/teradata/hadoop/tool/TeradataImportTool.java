package com.teradata.hadoop.tool;

import com.teradata.connector.common.exception.ConnectorException;
import com.teradata.connector.common.tool.ConnectorImportTool;
import com.teradata.connector.common.utils.ConnectorStringUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;


public class TeradataImportTool {
    private static Log logger;

    public static void main(final String[] args) {
        int res = 1;
        try {
            final long start = System.currentTimeMillis();
            TeradataImportTool.logger.info((Object) ("ConnectorImportTool starts at " + start));
            res = ToolRunner.run((Tool) new ConnectorImportTool(), args);
            final long end = System.currentTimeMillis();
            TeradataImportTool.logger.info((Object) ("ConnectorImportTool ends at " + end));
            TeradataImportTool.logger.info((Object) ("ConnectorImportTool time is " + (end - start) / 1000L + "s"));
        } catch (Throwable e) {
            TeradataImportTool.logger.info((Object) ConnectorStringUtils.getExceptionStack(e));
            if (e instanceof ConnectorException) {
                res = ((ConnectorException) e).getCode();
            } else {
                res = 10000;
            }
        } finally {
            TeradataImportTool.logger.info((Object) ("job completed with exit code " + res));
            if (res > 255) {
                res /= 1000;
            }
            System.exit(res);
        }
    }

    static {
        TeradataImportTool.logger = LogFactory.getLog((Class) ConnectorImportTool.class);
    }
}
