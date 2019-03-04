package com.teradata.connector.common.tool;

import com.teradata.connector.common.exception.ConnectorException;
import com.teradata.connector.test.group.SlowTests;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.junit.Assert;
import org.junit.Test;
import org.junit.experimental.categories.Category;


/**
 * @author Administrator
 */
public class ConnectorExportToolTest {
    final String dbs;
    final String databasename;
    final String user;
    final String password;

    public ConnectorExportToolTest() {
        this.dbs = System.getProperty("dbs");
        this.databasename = System.getProperty("databasename");
        this.user = System.getProperty("user");
        this.password = System.getProperty("password");
    }

    @Test
    public void testRun() throws Exception {
        try {
            ToolRunner.run((Tool) new ConnectorExportTool(), new String[0]);
        } catch (ConnectorException ce) {
            Assert.assertTrue(ce.getMessage().contains("Export tool parameters is not provided"));
        }
        try {
            final String[] args = {"-jobtype", "spark"};
            ToolRunner.run((Tool) new ConnectorExportTool(), args);
        } catch (ConnectorException ce) {
            Assert.assertTrue(ce.getMessage().contains("Export job type is invalid"));
        }
        try {
            final String[] args = {"-fileformat", "newsequence"};
            ToolRunner.run((Tool) new ConnectorExportTool(), args);
        } catch (ConnectorException ce) {
            Assert.assertTrue(ce.getMessage().contains("Export file format is invalid"));
        }
        try {
            ToolRunner.run((Tool) new ConnectorExportTool(), new String[]{"-method", "split.by.nothing"});
        } catch (ConnectorException ce) {
            Assert.assertTrue(ce.getMessage().contains("Export method is invalid, should be one of 'batch.insert','fastload','internal.fastload'"));
        }
        try {
            final String[] args = {"-nummappers", "i"};
            ToolRunner.run((Tool) new ConnectorExportTool(), args);
        } catch (ConnectorException ce) {
            Assert.assertTrue(ce.getMessage().contains("NumberFormatException: For input string: \"i\""));
        }
        try {
            final String[] args = {"-numreducers", "i"};
            ToolRunner.run((Tool) new ConnectorExportTool(), args);
        } catch (ConnectorException ce) {
            Assert.assertTrue(ce.getMessage().contains("NumberFormatException: For input string: \"i\""));
        }
        try {
            final String[] args = {"-targetfieldcount", "i", "-fastloadsocketport", "i", "-errorlimit", "i", "-batchsize", "i", "-fastloadsockettimeout", "i"};
            ToolRunner.run((Tool) new ConnectorExportTool(), args);
        } catch (ConnectorException ce) {
            Assert.assertTrue(ce.getMessage().contains("NumberFormatException: For input string: \"i\""));
        }
    }

    @Category({SlowTests.class})
    @Test
    public void testRunJob() throws Exception {
        final String[] args = {"-url", "jdbc:teradata://" + this.dbs + "/database=" + this.databasename, "-username", this.user, "-password", this.password, "-classname", "com.teradata.jdbc.TeraDriver", "-fileformat", "textfile", "-jobtype", "hdfs", "-targettable", this.databasename + ".export_hdfs_fun1", "-nummappers", "2", "-sourcepaths", "-method", "batch.insert"};
        try {
            ToolRunner.run((Tool) new ConnectorExportTool(), args);
        } catch (ConnectorException ce) {
            Assert.assertTrue(ce.getMessage().contains("Export tool parameters is invalid"));
        }
        final String[] argsUnknown = {"-url", "jdbc:teradata://" + this.dbs + "/database=" + this.databasename, "-username", this.user, "-password", this.password, "-classname", "com.teradata.jdbc.TeraDriver", "-fileformat", "textfile", "-jobtype", "hdfs", "-nummappers", "2", "-sourcepaths", "/", "-method", "batch.insert"};
        try {
            ToolRunner.run((Tool) new ConnectorExportTool(), argsUnknown);
        } catch (ConnectorException ce2) {
            Assert.assertTrue(ce2.getMessage().contains("Export target table name is not provided"));
        }
        final String[] newArgs = {"-url", "jdbc:teradata://" + this.dbs + "/database=" + this.databasename, "-username", this.user, "-password", this.password, "-classname", "com.teradata.jdbc.TeraDriver", "-fileformat", "textfile", "-jobtype", "hdfs", "-targettable", this.databasename + ".export_hdfs_fun1", "-nummappers", "2", "-sourcepaths", "/user/root/export_hdfs_fun1", "-method", "batch.insert", "-lineseparator", "\n", "-targettimeformat", "HH//mm:ss", "-targettimestampformat", "dd/MM/yyyy HH//mm:ss.SSSSSS"};
        try {
            final int res = ToolRunner.run((Tool) new ConnectorExportTool(), newArgs);
            Assert.assertEquals(res, 0);
        } catch (Exception e) {
            if (e.getMessage().contains("Input path does not exist: /user/root/export_hdfs_fun1")) {
                return;
            }
            throw e;
        }
    }
}
