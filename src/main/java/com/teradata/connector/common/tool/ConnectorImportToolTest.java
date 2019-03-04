package com.teradata.connector.common.tool;

import com.teradata.connector.common.exception.ConnectorException;
import com.teradata.connector.test.group.SlowTests;

import java.util.Date;

import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.junit.Assert;
import org.junit.Test;
import org.junit.experimental.categories.Category;

public class ConnectorImportToolTest {
    String dbs;
    String databasename;
    String user;
    String password;

    public ConnectorImportToolTest() {
        this.dbs = System.getProperty("dbs");
        this.databasename = System.getProperty("databasename");
        this.user = System.getProperty("user");
        this.password = System.getProperty("password");
    }

    @Test
    public void testRun() throws Exception {
        try {
            ToolRunner.run((Tool) new ConnectorImportTool(), new String[0]);
        } catch (ConnectorException ce) {
            Assert.assertTrue(ce.getMessage().contains("Import tool parameters is not provided"));
        }
        try {
            final String[] args = {"-jobtype", "spark"};
            ToolRunner.run((Tool) new ConnectorImportTool(), args);
        } catch (ConnectorException ce) {
            Assert.assertTrue(ce.getMessage().contains("Import job type is invalid"));
        }
        try {
            final String[] args = {"-fileformat", "newsequence"};
            ToolRunner.run((Tool) new ConnectorImportTool(), args);
        } catch (ConnectorException ce) {
            Assert.assertTrue(ce.getMessage().contains("Import file format is invalid"));
        }
        try {
            final String[] args = {"-method", "split.by.nothing"};
            ToolRunner.run((Tool) new ConnectorImportTool(), args);
        } catch (ConnectorException ce) {
            Assert.assertTrue(ce.getMessage().contains("Import method was invalid, should be one of 'split.by.hash','split.by.value','split.by.partition','split.by.amp'"));
        }
        try {
            final String[] args = {"-nummappers", "-1"};
            ToolRunner.run((Tool) new ConnectorImportTool(), args);
        } catch (ConnectorException ce) {
            Assert.assertTrue(ce.getMessage().contains("invalid provided value for parameter -nummappers,  greater than zero  value is required for this parameter"));
        }
        try {
            final String[] args = {"-numreducers", "i"};
            ToolRunner.run((Tool) new ConnectorImportTool(), args);
        } catch (ConnectorException ce) {
            Assert.assertTrue(ce.getMessage().contains("NumberFormatException: For input string: \"i\""));
        }
    }

    @Category({SlowTests.class})
    @Test
    public void testRunJob() throws Exception {
        final String[] args = {"-url", "jdbc:teradata://" + this.dbs + "/database=" + this.databasename, "-username", this.user, "-password", this.password, "-classname", "com.teradata.jdbc.TeraDriver", "-fileformat", "textfile", "-jobtype", "hive", "-sourcetable", this.databasename + ".TD_ALL_DATATYPE_small", "-nummappers", "2", "-targetdatabase", "hivetest", "-method", "split.by.partition", "-usexviews", "false"};
        try {
            ToolRunner.run((Tool) new ConnectorImportTool(), args);
        } catch (ConnectorException ce) {
            Assert.assertTrue(ce.getMessage().contains("Neither target path nor target Hive table name is provided"));
        }
        final String[] argsUnknown = {"-url", "jdbc:teradata://" + this.dbs + "/database=" + this.databasename, "-username", this.user, "-password", this.password, "-classname", "com.teradata.jdbc.TeraDriver", "-fileformat", "textfile", "-jobtype", "hive", "-sourcetable", this.databasename + ".TD_ALL_DATATYPE_small", "-nummappers", "2", "-targetdatabase", "hivetest", "-method", "split.by.partition", "-usexviews", "false", "-hiveversion", "0.13"};
        try {
            ToolRunner.run((Tool) new ConnectorImportTool(), argsUnknown);
        } catch (ConnectorException ce2) {
            Assert.assertTrue(ce2.getMessage().contains("unrecognized input parameters"));
        }
        final String[] newArgs = {"-url", "jdbc:teradata://" + this.dbs + "/database=" + this.databasename, "-username", this.user, "-password", this.password, "-classname", "com.teradata.jdbc.TeraDriver", "-fileformat", "textfile", "-jobtype", "hdfs", "-sourcetable", this.databasename + ".TD_ALL_DATATYPE_small", "-nummappers", "2", "-targetpaths", "/tmp/" + new Date(), "-method", "split.by.partition", "-usexviews", "false", "-lineseparator", "\n", "-debugoption", "1", "-sourcedateformat", "dd-mm-yy", "-sourcetimeformat", "HH//mm:ss", "-sourcetimestampformat", "dd/MM/yyyy HH//mm:ss.SSSSSS"};
        final int res = ToolRunner.run((Tool) new ConnectorImportTool(), newArgs);
        Assert.assertEquals(res, 0);
    }
}
