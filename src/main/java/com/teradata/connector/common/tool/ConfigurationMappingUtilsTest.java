package com.teradata.connector.common.tool;

import org.apache.hadoop.conf.*;
import com.teradata.hadoop.db.*;
import com.teradata.connector.common.exception.*;
import com.teradata.connector.teradata.utils.*;
import org.apache.hadoop.mapreduce.*;
import org.junit.Assert;
import org.junit.Test;

public class ConfigurationMappingUtilsTest {
    String user;
    String password;

    public ConfigurationMappingUtilsTest() {
        this.user = "tduser";
        this.password = "tdpass";
    }

    @Test
    public void testImportConfigurationMapping() throws Exception {
        final Configuration conf = new Configuration();
        TeradataConfiguration.setInputJobType(conf, "hdfs");
        TeradataConfiguration.setInputFileFormat(conf, "rcfile");
        TeradataConfiguration.setInputTargetPaths(conf, "/user/TDimport");
        TeradataConfiguration.setInputSourceTable(conf, "TDtable");
        TeradataConfiguration.setInputSourceDatabase(conf, "TDdatabase");
        TeradataConfiguration.setJDBCDriverClass(conf, "com.teradata.jdbc.TeraDriver");
        TeradataConfiguration.setJDBCURL(conf, "jdbc:teradata://dbs/database=databasename");
        TeradataConfiguration.setJDBCUsername(conf, this.user);
        TeradataConfiguration.setJDBCPassword(conf, this.password);
        try {
            ConfigurationMappingUtils.importConfigurationMapping(conf);
        } catch (ConnectorException ce) {
            Assert.assertTrue(ce.getMessage().contains("plugin \"hdfs-rcfile\" not found"));
        }
        TeradataConfiguration.setInputJobType(conf, "hive");
        ConfigurationMappingUtils.importConfigurationMapping(conf);
        Assert.assertEquals(conf.getInt("tdch.num.mappers", 0), 2);
        Assert.assertEquals(conf.get("teradata.db.input.source.table"), "TDtable");
        Assert.assertNull((Object) conf.get("tdch.output.hive.table"));
        Assert.assertEquals(conf.get("tdch.output.hive.paths"), "/user/TDimport");
        Assert.assertEquals(conf.get("mapred.output.dir"), "/user/TDimport");
    }

    @Test
    public void exportConfigurationMappingTest() throws Exception {
        final Configuration conf = new Configuration();
        TeradataConfiguration.setOutputJobType(conf, "hdfs");
        TeradataConfiguration.setOutputFileFormat(conf, "textfile");
        TeradataConfiguration.setOutputSourcePaths(conf, "/user/TDexport");
        TeradataConfiguration.setOutputTargetTable(conf, "TDtable");
        TeradataConfiguration.setOutputTargetDatabase(conf, "TDdatabase");
        TeradataConfiguration.setJDBCDriverClass(conf, "com.teradata.jdbc.TeraDriver");
        TeradataConfiguration.setJDBCURL(conf, "jdbc:teradata://dbs/database=databasename");
        TeradataConfiguration.setJDBCUsername(conf, this.user);
        TeradataConfiguration.setJDBCPassword(conf, this.password);
        TeradataConfiguration.setOutputMethod(conf, "batch.insert");
        TeradataConfiguration.setOutputSeparator(conf, ",");
        ConfigurationMappingUtils.exportConfigurationMapping(conf);
        Assert.assertEquals(conf.get("teradata.db.output.target.table"), "TDtable");
        Assert.assertEquals(conf.getInt("tdch.num.mappers", 0), 2);
        Assert.assertNull((Object) conf.get("teradata.db.output.source.database"));
        Assert.assertEquals(conf.get("teradata.db.output.source.paths"), "/user/TDexport");
        Assert.assertEquals(conf.get("teradata.db.output.fields.separator"), ",");
    }

    @Test
    public void hideCredentialsTest() throws Exception {
        Configuration conf = new Configuration();
        TeradataConfiguration.setJDBCUsername(conf, this.user);
        TeradataConfiguration.setJDBCPassword(conf, this.password);
        TeradataPlugInConfiguration.setInputJdbcUserName(conf, this.user);
        TeradataPlugInConfiguration.setInputJdbcPassword(conf, this.password);
        TeradataPlugInConfiguration.setOutputJdbcUserName(conf, this.user);
        TeradataPlugInConfiguration.setOutputJdbcPassword(conf, this.password);
        final JobContext context = (JobContext) new Job(conf);
        conf = context.getConfiguration();
        Assert.assertEquals(conf.get("mapreduce.jdbc.username"), this.user);
        Assert.assertEquals(conf.get("mapreduce.jdbc.password"), this.password);
        ConfigurationMappingUtils.hideCredentials(context);
        Assert.assertTrue(conf.get("mapreduce.jdbc.username").isEmpty());
        Assert.assertTrue(conf.get("mapreduce.jdbc.password").isEmpty());
        Assert.assertTrue(conf.get("tdch.input.teradata.jdbc.user.name").isEmpty());
        Assert.assertTrue(conf.get("tdch.input.teradata.jdbc.password").isEmpty());
        Assert.assertTrue(conf.get("tdch.output.teradata.jdbc.user.name").isEmpty());
        Assert.assertTrue(conf.get("tdch.output.teradata.jdbc.password").isEmpty());
    }
}
