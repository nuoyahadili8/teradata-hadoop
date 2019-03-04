package com.teradata.connector.common.tool;

import com.teradata.connector.common.exception.ConnectorException;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.junit.Assert;
import org.junit.Test;


/**
 * @author Administrator
 */
public class ConnectorPluginToolTest {
    @Test
    public void testRun() throws Exception {
        final String[] args = {"-sourceplugin", "source", "-targetplugin", "target", "-sourcerecordschema", "\"(int, float)\"", "-targetrecordschema", "\"(int, double)\"", "-nummappers", "2"};
        try {
            ToolRunner.run((Tool) new ConnectorPluginTool(), args);
        } catch (ConnectorException ce) {
            Assert.assertTrue(ce.getMessage().matches(".*plugin .* not found.*"));
        }
        args[1] = "hdfs-textfile";
        args[3] = "hdfs-avrofile";
        try {
            ToolRunner.run((Tool) new ConnectorPluginTool(), args);
        } catch (ConnectorException ce) {
            Assert.assertTrue(ce.getMessage().contains("job is invalid"));
        }
    }
}
