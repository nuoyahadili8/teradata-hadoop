package com.teradata.connector.teradata;

import com.teradata.connector.common.utils.StandardCharsets;
import com.teradata.connector.teradata.db.TeradataConnection;
import com.teradata.connector.teradata.processor.TeradataSplitByPartitionProcessor;
import com.teradata.connector.teradata.utils.TeradataPlugInConfiguration;
import com.teradata.connector.teradata.utils.TeradataSchemaUtils;

import java.sql.SQLException;
import java.util.List;

import junit.framework.Assert;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.JobContext;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;


public class TeradataSplitByPartitionInputFormatTest {
    private static String dbJdbcUrl;
    private static String dbIpAddress;
    private static String dbDatabase;
    private static String dbUsername;
    private static String dbPassword;
    private static String dbJdbcDriverClass;
    private static TeradataConnection dbConnection;
    protected static final int VALUE_PARTITION_ID_TYPE = 4;
    protected static final String COLUMN_PARTITION_ID = "TDIN_PARTID";
    protected static final String COLUMN_PARTITION = "PARTITION";
    protected static final int PARTITION_RANGE_MIN = 10;

    @BeforeClass
    public static void initializeTests() throws Exception {
        TeradataSplitByPartitionInputFormatTest.dbIpAddress = System.getProperty("test.database.ipaddress", "10.25.35.197");
        TeradataSplitByPartitionInputFormatTest.dbDatabase = System.getProperty("test.database.databasename", "tdch_testing2");
        TeradataSplitByPartitionInputFormatTest.dbUsername = System.getProperty("test.database.username", "tdch_testing2");
        TeradataSplitByPartitionInputFormatTest.dbPassword = System.getProperty("test.database.userpassword", "tdch_testing2");
        TeradataSplitByPartitionInputFormatTest.dbJdbcDriverClass = System.getProperty("test.database.jdbcdriver.class", "com.teradata.jdbc.TeraDriver");
        TeradataSplitByPartitionInputFormatTest.dbJdbcUrl = "jdbc:teradata://" + TeradataSplitByPartitionInputFormatTest.dbIpAddress + "/database=" + TeradataSplitByPartitionInputFormatTest.dbDatabase;
        (TeradataSplitByPartitionInputFormatTest.dbConnection = new TeradataConnection(TeradataSplitByPartitionInputFormatTest.dbJdbcDriverClass, TeradataSplitByPartitionInputFormatTest.dbJdbcUrl, TeradataSplitByPartitionInputFormatTest.dbUsername, TeradataSplitByPartitionInputFormatTest.dbPassword, false)).connect();
        createDatabaseTables();
    }

    @AfterClass
    public static void cleanupTests() throws Exception {
        dropDatabaseTables();
        TeradataSplitByPartitionInputFormatTest.dbConnection.close();
    }

    @Test
    public void testGetSplitsStagingEnabled1() throws Exception {
        final boolean stagingEnabled = true;
        final TeradataSplitByPartitionProcessor processor = new TeradataSplitByPartitionProcessor();
        final JobContext context = (JobContext) new Job();
        final Configuration configuration = context.getConfiguration();
        configuration.set("tdch.num.mappers", "4");
        configuration.set("tdch.input.teradata.num.partitions", "10");
        configuration.set("tdch.input.teradata.stage.table.enabled", stagingEnabled ? "true" : "false");
        TeradataPlugInConfiguration.setInputJdbcDriverClass(configuration, TeradataSplitByPartitionInputFormatTest.dbJdbcDriverClass);
        TeradataPlugInConfiguration.setInputJdbcUrl(configuration, TeradataSplitByPartitionInputFormatTest.dbJdbcUrl);
        TeradataPlugInConfiguration.setInputTable(configuration, TeradataSplitByPartitionInputFormatTest.dbDatabase + ".splitbypartition_table_nopartitions");
        TeradataPlugInConfiguration.setInputStageTableEnabled(configuration, stagingEnabled);
        TeradataPlugInConfiguration.setInputTeradataUserName(context, TeradataSplitByPartitionInputFormatTest.dbUsername.getBytes(StandardCharsets.UTF_8));
        TeradataPlugInConfiguration.setInputTeradataPassword(context, TeradataSplitByPartitionInputFormatTest.dbPassword.getBytes(StandardCharsets.UTF_8));
        TeradataSchemaUtils.setupTeradataSourceTableSchema(configuration, TeradataSplitByPartitionInputFormatTest.dbConnection);
        processor.inputPreProcessor(context);
        TeradataSchemaUtils.configureSourceConnectorRecordSchema(configuration);
        final TeradataSplitByPartitionInputFormat sbpInputFormat = new TeradataSplitByPartitionInputFormat();
        final List<InputSplit> inputSplits = sbpInputFormat.getSplits(context);
        Assert.assertTrue(inputSplits.size() == 4);
        int count = 0;
        for (final InputSplit split : inputSplits) {
            final TeradataInputFormat.TeradataInputSplit tsplit = (TeradataInputFormat.TeradataInputSplit) split;
            switch (count) {
                case 0: {
                    Assert.assertTrue(tsplit.getSplitSql().contains(".PARTITION BETWEEN 0 AND 2"));
                    break;
                }
                case 1: {
                    Assert.assertTrue(tsplit.getSplitSql().contains(".PARTITION BETWEEN 3 AND 5"));
                    break;
                }
                case 2: {
                    Assert.assertTrue(tsplit.getSplitSql().contains(".PARTITION BETWEEN 6 AND 7"));
                    break;
                }
                case 3: {
                    Assert.assertTrue(tsplit.getSplitSql().contains(".PARTITION BETWEEN 8 AND 10"));
                    break;
                }
            }
            ++count;
        }
        processor.inputPostProcessor(context);
    }

    @Test
    public void testGetSplitsStagingEnabled2() throws Exception {
        final boolean stagingEnabled = true;
        final TeradataSplitByPartitionProcessor processor = new TeradataSplitByPartitionProcessor();
        final JobContext context = (JobContext) new Job();
        final Configuration configuration = context.getConfiguration();
        configuration.set("tdch.num.mappers", "10");
        configuration.set("tdch.input.teradata.num.partitions", "10");
        configuration.set("tdch.input.teradata.stage.table.enabled", stagingEnabled ? "true" : "false");
        TeradataPlugInConfiguration.setInputJdbcDriverClass(configuration, TeradataSplitByPartitionInputFormatTest.dbJdbcDriverClass);
        TeradataPlugInConfiguration.setInputJdbcUrl(configuration, TeradataSplitByPartitionInputFormatTest.dbJdbcUrl);
        TeradataPlugInConfiguration.setInputTable(configuration, TeradataSplitByPartitionInputFormatTest.dbDatabase + ".splitbypartition_table_nopartitions");
        TeradataPlugInConfiguration.setInputStageTableEnabled(configuration, stagingEnabled);
        TeradataPlugInConfiguration.setInputTeradataUserName(context, TeradataSplitByPartitionInputFormatTest.dbUsername.getBytes(StandardCharsets.UTF_8));
        TeradataPlugInConfiguration.setInputTeradataPassword(context, TeradataSplitByPartitionInputFormatTest.dbPassword.getBytes(StandardCharsets.UTF_8));
        TeradataSchemaUtils.setupTeradataSourceTableSchema(configuration, TeradataSplitByPartitionInputFormatTest.dbConnection);
        processor.inputPreProcessor(context);
        TeradataSchemaUtils.configureSourceConnectorRecordSchema(configuration);
        final TeradataSplitByPartitionInputFormat sbpInputFormat = new TeradataSplitByPartitionInputFormat();
        final List<InputSplit> inputSplits = sbpInputFormat.getSplits(context);
        Assert.assertTrue(inputSplits.size() == 10);
        int count = 0;
        for (final InputSplit split : inputSplits) {
            final TeradataInputFormat.TeradataInputSplit tsplit = (TeradataInputFormat.TeradataInputSplit) split;
            switch (count) {
                case 0: {
                    Assert.assertTrue(tsplit.getSplitSql().contains(".PARTITION = 1"));
                    break;
                }
                case 1: {
                    Assert.assertTrue(tsplit.getSplitSql().contains(".PARTITION = 2"));
                    break;
                }
                case 2: {
                    Assert.assertTrue(tsplit.getSplitSql().contains(".PARTITION = 3"));
                    break;
                }
                case 3: {
                    Assert.assertTrue(tsplit.getSplitSql().contains(".PARTITION = 4"));
                    break;
                }
                case 4: {
                    Assert.assertTrue(tsplit.getSplitSql().contains(".PARTITION = 5"));
                    break;
                }
                case 5: {
                    Assert.assertTrue(tsplit.getSplitSql().contains(".PARTITION = 6"));
                    break;
                }
                case 6: {
                    Assert.assertTrue(tsplit.getSplitSql().contains(".PARTITION = 7"));
                    break;
                }
                case 7: {
                    Assert.assertTrue(tsplit.getSplitSql().contains(".PARTITION = 8"));
                    break;
                }
                case 8: {
                    Assert.assertTrue(tsplit.getSplitSql().contains(".PARTITION = 9"));
                    break;
                }
                case 9: {
                    Assert.assertTrue(tsplit.getSplitSql().contains(".PARTITION = 10"));
                    break;
                }
            }
            ++count;
        }
        processor.inputPostProcessor(context);
    }

    @Test
    public void testGetSplitsStagingEnabled3() throws Exception {
        final boolean stagingEnabled = true;
        final TeradataSplitByPartitionProcessor processor = new TeradataSplitByPartitionProcessor();
        final JobContext context = (JobContext) new Job();
        final Configuration configuration = context.getConfiguration();
        configuration.set("tdch.num.mappers", "12");
        configuration.set("tdch.input.teradata.num.partitions", "10");
        configuration.set("tdch.input.teradata.stage.table.enabled", stagingEnabled ? "true" : "false");
        TeradataPlugInConfiguration.setInputJdbcDriverClass(configuration, TeradataSplitByPartitionInputFormatTest.dbJdbcDriverClass);
        TeradataPlugInConfiguration.setInputJdbcUrl(configuration, TeradataSplitByPartitionInputFormatTest.dbJdbcUrl);
        TeradataPlugInConfiguration.setInputTable(configuration, TeradataSplitByPartitionInputFormatTest.dbDatabase + ".splitbypartition_table_nopartitions");
        TeradataPlugInConfiguration.setInputStageTableEnabled(configuration, stagingEnabled);
        TeradataPlugInConfiguration.setInputTeradataUserName(context, TeradataSplitByPartitionInputFormatTest.dbUsername.getBytes(StandardCharsets.UTF_8));
        TeradataPlugInConfiguration.setInputTeradataPassword(context, TeradataSplitByPartitionInputFormatTest.dbPassword.getBytes(StandardCharsets.UTF_8));
        TeradataSchemaUtils.setupTeradataSourceTableSchema(configuration, TeradataSplitByPartitionInputFormatTest.dbConnection);
        processor.inputPreProcessor(context);
        TeradataSchemaUtils.configureSourceConnectorRecordSchema(configuration);
        final TeradataSplitByPartitionInputFormat sbpInputFormat = new TeradataSplitByPartitionInputFormat();
        final List<InputSplit> inputSplits = sbpInputFormat.getSplits(context);
        Assert.assertTrue(inputSplits.size() == 10);
        int count = 0;
        for (final InputSplit split : inputSplits) {
            final TeradataInputFormat.TeradataInputSplit tsplit = (TeradataInputFormat.TeradataInputSplit) split;
            switch (count) {
                case 0: {
                    Assert.assertTrue(tsplit.getSplitSql().contains(".PARTITION = 1"));
                    break;
                }
                case 1: {
                    Assert.assertTrue(tsplit.getSplitSql().contains(".PARTITION = 2"));
                    break;
                }
                case 2: {
                    Assert.assertTrue(tsplit.getSplitSql().contains(".PARTITION = 3"));
                    break;
                }
                case 3: {
                    Assert.assertTrue(tsplit.getSplitSql().contains(".PARTITION = 4"));
                    break;
                }
                case 4: {
                    Assert.assertTrue(tsplit.getSplitSql().contains(".PARTITION = 5"));
                    break;
                }
                case 5: {
                    Assert.assertTrue(tsplit.getSplitSql().contains(".PARTITION = 6"));
                    break;
                }
                case 6: {
                    Assert.assertTrue(tsplit.getSplitSql().contains(".PARTITION = 7"));
                    break;
                }
                case 7: {
                    Assert.assertTrue(tsplit.getSplitSql().contains(".PARTITION = 8"));
                    break;
                }
                case 8: {
                    Assert.assertTrue(tsplit.getSplitSql().contains(".PARTITION = 9"));
                    break;
                }
                case 9: {
                    Assert.assertTrue(tsplit.getSplitSql().contains(".PARTITION = 10"));
                    break;
                }
            }
            ++count;
        }
        processor.inputPostProcessor(context);
    }

    @Test
    public void testGetSplitsStagingEnabled4() throws Exception {
        final boolean stagingEnabled = true;
        final TeradataSplitByPartitionProcessor processor = new TeradataSplitByPartitionProcessor();
        final JobContext context = (JobContext) new Job();
        final Configuration configuration = context.getConfiguration();
        configuration.set("tdch.num.mappers", "1");
        configuration.set("tdch.input.teradata.stage.table.enabled", stagingEnabled ? "true" : "false");
        TeradataPlugInConfiguration.setInputJdbcDriverClass(configuration, TeradataSplitByPartitionInputFormatTest.dbJdbcDriverClass);
        TeradataPlugInConfiguration.setInputJdbcUrl(configuration, TeradataSplitByPartitionInputFormatTest.dbJdbcUrl);
        TeradataPlugInConfiguration.setInputTable(configuration, TeradataSplitByPartitionInputFormatTest.dbDatabase + ".splitbypartition_table_nopartitions");
        TeradataPlugInConfiguration.setInputStageTableEnabled(configuration, stagingEnabled);
        TeradataPlugInConfiguration.setInputTeradataUserName(context, TeradataSplitByPartitionInputFormatTest.dbUsername.getBytes(StandardCharsets.UTF_8));
        TeradataPlugInConfiguration.setInputTeradataPassword(context, TeradataSplitByPartitionInputFormatTest.dbPassword.getBytes(StandardCharsets.UTF_8));
        TeradataSchemaUtils.setupTeradataSourceTableSchema(configuration, TeradataSplitByPartitionInputFormatTest.dbConnection);
        processor.inputPreProcessor(context);
        TeradataSchemaUtils.configureSourceConnectorRecordSchema(configuration);
        final TeradataSplitByPartitionInputFormat sbpInputFormat = new TeradataSplitByPartitionInputFormat();
        final List<InputSplit> inputSplits = sbpInputFormat.getSplits(context);
        Assert.assertTrue(inputSplits.size() == 1);
        final TeradataInputFormat.TeradataInputSplit tsplit = (TeradataInputFormat.TeradataInputSplit) inputSplits.get(0);
        Assert.assertTrue(tsplit.getSplitSql().contains("SELECT \"col1\", \"col2\", \"col3\" FROM \"" + TeradataSplitByPartitionInputFormatTest.dbDatabase + "\".\"splitbypartition_table_nopartitions\""));
        processor.inputPostProcessor(context);
    }

    @Test
    public void testGetSplitsStagingEnabled5() throws Exception {
        final boolean stagingEnabled = true;
        final TeradataSplitByPartitionProcessor processor = new TeradataSplitByPartitionProcessor();
        final JobContext context = (JobContext) new Job();
        final Configuration configuration = context.getConfiguration();
        configuration.set("tdch.num.mappers", "4");
        configuration.set("tdch.input.teradata.num.partitions", "10");
        configuration.set("tdch.input.teradata.stage.table.enabled", stagingEnabled ? "true" : "false");
        TeradataPlugInConfiguration.setInputJdbcDriverClass(configuration, TeradataSplitByPartitionInputFormatTest.dbJdbcDriverClass);
        TeradataPlugInConfiguration.setInputJdbcUrl(configuration, TeradataSplitByPartitionInputFormatTest.dbJdbcUrl);
        TeradataPlugInConfiguration.setInputTable(configuration, TeradataSplitByPartitionInputFormatTest.dbDatabase + ".splitbypartition_table_10partitions");
        TeradataPlugInConfiguration.setInputStageTableEnabled(configuration, stagingEnabled);
        TeradataPlugInConfiguration.setInputTeradataUserName(context, TeradataSplitByPartitionInputFormatTest.dbUsername.getBytes(StandardCharsets.UTF_8));
        TeradataPlugInConfiguration.setInputTeradataPassword(context, TeradataSplitByPartitionInputFormatTest.dbPassword.getBytes(StandardCharsets.UTF_8));
        TeradataSchemaUtils.setupTeradataSourceTableSchema(configuration, TeradataSplitByPartitionInputFormatTest.dbConnection);
        processor.inputPreProcessor(context);
        TeradataSchemaUtils.configureSourceConnectorRecordSchema(configuration);
        final TeradataSplitByPartitionInputFormat sbpInputFormat = new TeradataSplitByPartitionInputFormat();
        final List<InputSplit> inputSplits = sbpInputFormat.getSplits(context);
        Assert.assertTrue(inputSplits.size() == 4);
        int count = 0;
        for (final InputSplit split : inputSplits) {
            final TeradataInputFormat.TeradataInputSplit tsplit = (TeradataInputFormat.TeradataInputSplit) split;
            switch (count) {
                case 0: {
                    Assert.assertTrue(tsplit.getSplitSql().contains(".PARTITION BETWEEN 0 AND 2"));
                    break;
                }
                case 1: {
                    Assert.assertTrue(tsplit.getSplitSql().contains(".PARTITION BETWEEN 3 AND 5"));
                    break;
                }
                case 2: {
                    Assert.assertTrue(tsplit.getSplitSql().contains(".PARTITION BETWEEN 6 AND 7"));
                    break;
                }
                case 3: {
                    Assert.assertTrue(tsplit.getSplitSql().contains(".PARTITION BETWEEN 8 AND 10"));
                    break;
                }
            }
            ++count;
        }
        processor.inputPostProcessor(context);
    }

    @Test
    public void testGetSplitsStagingEnabled6() throws Exception {
        final boolean stagingEnabled = true;
        final TeradataSplitByPartitionProcessor processor = new TeradataSplitByPartitionProcessor();
        final JobContext context = (JobContext) new Job();
        final Configuration configuration = context.getConfiguration();
        configuration.set("tdch.num.mappers", "2");
        configuration.set("tdch.input.teradata.num.partitions", "10");
        configuration.set("tdch.input.teradata.stage.table.enabled", stagingEnabled ? "true" : "false");
        TeradataPlugInConfiguration.setInputJdbcDriverClass(configuration, TeradataSplitByPartitionInputFormatTest.dbJdbcDriverClass);
        TeradataPlugInConfiguration.setInputJdbcUrl(configuration, TeradataSplitByPartitionInputFormatTest.dbJdbcUrl);
        TeradataPlugInConfiguration.setInputTable(configuration, TeradataSplitByPartitionInputFormatTest.dbDatabase + ".splitbypartition_table_10partitions");
        TeradataPlugInConfiguration.setInputStageTableEnabled(configuration, stagingEnabled);
        TeradataPlugInConfiguration.setInputTeradataUserName(context, TeradataSplitByPartitionInputFormatTest.dbUsername.getBytes(StandardCharsets.UTF_8));
        TeradataPlugInConfiguration.setInputTeradataPassword(context, TeradataSplitByPartitionInputFormatTest.dbPassword.getBytes(StandardCharsets.UTF_8));
        TeradataSchemaUtils.setupTeradataSourceTableSchema(configuration, TeradataSplitByPartitionInputFormatTest.dbConnection);
        processor.inputPreProcessor(context);
        TeradataSchemaUtils.configureSourceConnectorRecordSchema(configuration);
        final TeradataSplitByPartitionInputFormat sbpInputFormat = new TeradataSplitByPartitionInputFormat();
        final List<InputSplit> inputSplits = sbpInputFormat.getSplits(context);
        Assert.assertTrue(inputSplits.size() == 2);
        int count = 0;
        for (final InputSplit split : inputSplits) {
            final TeradataInputFormat.TeradataInputSplit tsplit = (TeradataInputFormat.TeradataInputSplit) split;
            switch (count) {
                case 0: {
                    Assert.assertTrue(tsplit.getSplitSql().contains(".PARTITION BETWEEN 0 AND 4"));
                    break;
                }
                case 1: {
                    Assert.assertTrue(tsplit.getSplitSql().contains(".PARTITION BETWEEN 5 AND 10"));
                    break;
                }
            }
            ++count;
        }
        processor.inputPostProcessor(context);
    }

    @Test
    public void testGetSplitsStagingEnabled7() throws Exception {
        final boolean stagingEnabled = true;
        final TeradataSplitByPartitionProcessor processor = new TeradataSplitByPartitionProcessor();
        final JobContext context = (JobContext) new Job();
        final Configuration configuration = context.getConfiguration();
        configuration.set("tdch.num.mappers", "3");
        configuration.set("tdch.input.teradata.num.partitions", "32");
        configuration.set("tdch.input.teradata.stage.table.enabled", stagingEnabled ? "true" : "false");
        TeradataPlugInConfiguration.setInputJdbcDriverClass(configuration, TeradataSplitByPartitionInputFormatTest.dbJdbcDriverClass);
        TeradataPlugInConfiguration.setInputJdbcUrl(configuration, TeradataSplitByPartitionInputFormatTest.dbJdbcUrl);
        TeradataPlugInConfiguration.setInputTable(configuration, TeradataSplitByPartitionInputFormatTest.dbDatabase + ".splitbypartition_table_nopartitions");
        TeradataPlugInConfiguration.setInputStageTableEnabled(configuration, stagingEnabled);
        TeradataPlugInConfiguration.setInputTeradataUserName(context, TeradataSplitByPartitionInputFormatTest.dbUsername.getBytes(StandardCharsets.UTF_8));
        TeradataPlugInConfiguration.setInputTeradataPassword(context, TeradataSplitByPartitionInputFormatTest.dbPassword.getBytes(StandardCharsets.UTF_8));
        TeradataSchemaUtils.setupTeradataSourceTableSchema(configuration, TeradataSplitByPartitionInputFormatTest.dbConnection);
        processor.inputPreProcessor(context);
        TeradataSchemaUtils.configureSourceConnectorRecordSchema(configuration);
        final TeradataSplitByPartitionInputFormat sbpInputFormat = new TeradataSplitByPartitionInputFormat();
        final List<InputSplit> inputSplits = sbpInputFormat.getSplits(context);
        Assert.assertTrue(inputSplits.size() == 3);
        int count = 0;
        for (final InputSplit split : inputSplits) {
            final TeradataInputFormat.TeradataInputSplit tsplit = (TeradataInputFormat.TeradataInputSplit) split;
            switch (count) {
                case 0: {
                    Assert.assertTrue(tsplit.getSplitSql().contains(".PARTITION BETWEEN 1 AND 11"));
                    break;
                }
                case 1: {
                    Assert.assertTrue(tsplit.getSplitSql().contains(".PARTITION BETWEEN 12 AND 21"));
                    break;
                }
                case 2: {
                    Assert.assertTrue(tsplit.getSplitSql().contains(".PARTITION BETWEEN 22 AND 32"));
                    break;
                }
            }
            ++count;
        }
        processor.inputPostProcessor(context);
    }

    @Test
    public void testGetSplitsStagingDisabled1() throws Exception {
        final boolean stagingEnabled = false;
        final TeradataSplitByPartitionProcessor processor = new TeradataSplitByPartitionProcessor();
        final JobContext context = (JobContext) new Job();
        final Configuration configuration = context.getConfiguration();
        configuration.set("tdch.num.mappers", "4");
        configuration.set("tdch.input.teradata.num.partitions", "10");
        configuration.set("tdch.input.teradata.stage.table.enabled", stagingEnabled ? "true" : "false");
        TeradataPlugInConfiguration.setInputJdbcDriverClass(configuration, TeradataSplitByPartitionInputFormatTest.dbJdbcDriverClass);
        TeradataPlugInConfiguration.setInputJdbcUrl(configuration, TeradataSplitByPartitionInputFormatTest.dbJdbcUrl);
        TeradataPlugInConfiguration.setInputTable(configuration, TeradataSplitByPartitionInputFormatTest.dbDatabase + ".splitbypartition_table_10partitions");
        TeradataPlugInConfiguration.setInputStageTableEnabled(configuration, stagingEnabled);
        TeradataPlugInConfiguration.setInputTeradataUserName(context, TeradataSplitByPartitionInputFormatTest.dbUsername.getBytes(StandardCharsets.UTF_8));
        TeradataPlugInConfiguration.setInputTeradataPassword(context, TeradataSplitByPartitionInputFormatTest.dbPassword.getBytes(StandardCharsets.UTF_8));
        TeradataSchemaUtils.setupTeradataSourceTableSchema(configuration, TeradataSplitByPartitionInputFormatTest.dbConnection);
        processor.inputPreProcessor(context);
        TeradataSchemaUtils.configureSourceConnectorRecordSchema(configuration);
        final TeradataSplitByPartitionInputFormat sbpInputFormat = new TeradataSplitByPartitionInputFormat();
        final List<InputSplit> inputSplits = sbpInputFormat.getSplits(context);
        Assert.assertTrue(inputSplits.size() == 4);
        int count = 0;
        for (final InputSplit split : inputSplits) {
            final TeradataInputFormat.TeradataInputSplit tsplit = (TeradataInputFormat.TeradataInputSplit) split;
            switch (count) {
                case 0: {
                    Assert.assertTrue(tsplit.getSplitSql().contains(".PARTITION BETWEEN 24 AND 55"));
                    break;
                }
                case 1: {
                    Assert.assertTrue(tsplit.getSplitSql().contains(".PARTITION BETWEEN 83 AND 127"));
                    break;
                }
                case 2: {
                    Assert.assertTrue(tsplit.getSplitSql().contains(".PARTITION BETWEEN 133 AND 162"));
                    break;
                }
                case 3: {
                    Assert.assertTrue(tsplit.getSplitSql().contains(".PARTITION BETWEEN 170 AND 179"));
                    break;
                }
            }
            ++count;
        }
        processor.inputPostProcessor(context);
    }

    @Test
    public void testGetSplitsStagingDisabled2() throws Exception {
        final boolean stagingEnabled = false;
        final TeradataSplitByPartitionProcessor processor = new TeradataSplitByPartitionProcessor();
        final JobContext context = (JobContext) new Job();
        final Configuration configuration = context.getConfiguration();
        configuration.set("tdch.num.mappers", "2");
        configuration.set("tdch.input.teradata.num.partitions", "10");
        configuration.set("tdch.input.teradata.stage.table.enabled", stagingEnabled ? "true" : "false");
        TeradataPlugInConfiguration.setInputJdbcDriverClass(configuration, TeradataSplitByPartitionInputFormatTest.dbJdbcDriverClass);
        TeradataPlugInConfiguration.setInputJdbcUrl(configuration, TeradataSplitByPartitionInputFormatTest.dbJdbcUrl);
        TeradataPlugInConfiguration.setInputTable(configuration, TeradataSplitByPartitionInputFormatTest.dbDatabase + ".splitbypartition_table_10partitions");
        TeradataPlugInConfiguration.setInputStageTableEnabled(configuration, stagingEnabled);
        TeradataPlugInConfiguration.setInputTeradataUserName(context, TeradataSplitByPartitionInputFormatTest.dbUsername.getBytes(StandardCharsets.UTF_8));
        TeradataPlugInConfiguration.setInputTeradataPassword(context, TeradataSplitByPartitionInputFormatTest.dbPassword.getBytes(StandardCharsets.UTF_8));
        TeradataSchemaUtils.setupTeradataSourceTableSchema(configuration, TeradataSplitByPartitionInputFormatTest.dbConnection);
        processor.inputPreProcessor(context);
        TeradataSchemaUtils.configureSourceConnectorRecordSchema(configuration);
        final TeradataSplitByPartitionInputFormat sbpInputFormat = new TeradataSplitByPartitionInputFormat();
        final List<InputSplit> inputSplits = sbpInputFormat.getSplits(context);
        Assert.assertTrue(inputSplits.size() == 2);
        int count = 0;
        for (final InputSplit split : inputSplits) {
            final TeradataInputFormat.TeradataInputSplit tsplit = (TeradataInputFormat.TeradataInputSplit) split;
            switch (count) {
                case 0: {
                    Assert.assertTrue(tsplit.getSplitSql().contains(".PARTITION BETWEEN 24 AND 110"));
                    break;
                }
                case 1: {
                    Assert.assertTrue(tsplit.getSplitSql().contains(".PARTITION BETWEEN 127 AND 179"));
                    break;
                }
            }
            ++count;
        }
        processor.inputPostProcessor(context);
    }

    @Test
    public void testGetSplitsStagingDisabled3() throws Exception {
        final boolean stagingEnabled = false;
        final TeradataSplitByPartitionProcessor processor = new TeradataSplitByPartitionProcessor();
        final JobContext context = (JobContext) new Job();
        final Configuration configuration = context.getConfiguration();
        configuration.set("tdch.num.mappers", "10");
        configuration.set("tdch.input.teradata.num.partitions", "10");
        configuration.set("tdch.input.teradata.stage.table.enabled", stagingEnabled ? "true" : "false");
        TeradataPlugInConfiguration.setInputJdbcDriverClass(configuration, TeradataSplitByPartitionInputFormatTest.dbJdbcDriverClass);
        TeradataPlugInConfiguration.setInputJdbcUrl(configuration, TeradataSplitByPartitionInputFormatTest.dbJdbcUrl);
        TeradataPlugInConfiguration.setInputTable(configuration, TeradataSplitByPartitionInputFormatTest.dbDatabase + ".splitbypartition_table_10partitions");
        TeradataPlugInConfiguration.setInputStageTableEnabled(configuration, stagingEnabled);
        TeradataPlugInConfiguration.setInputTeradataUserName(context, TeradataSplitByPartitionInputFormatTest.dbUsername.getBytes(StandardCharsets.UTF_8));
        TeradataPlugInConfiguration.setInputTeradataPassword(context, TeradataSplitByPartitionInputFormatTest.dbPassword.getBytes(StandardCharsets.UTF_8));
        TeradataSchemaUtils.setupTeradataSourceTableSchema(configuration, TeradataSplitByPartitionInputFormatTest.dbConnection);
        processor.inputPreProcessor(context);
        TeradataSchemaUtils.configureSourceConnectorRecordSchema(configuration);
        final TeradataSplitByPartitionInputFormat sbpInputFormat = new TeradataSplitByPartitionInputFormat();
        final List<InputSplit> inputSplits = sbpInputFormat.getSplits(context);
        Assert.assertTrue(inputSplits.size() == 10);
        int count = 0;
        for (final InputSplit split : inputSplits) {
            final TeradataInputFormat.TeradataInputSplit tsplit = (TeradataInputFormat.TeradataInputSplit) split;
            switch (count) {
                case 0: {
                    Assert.assertTrue(tsplit.getSplitSql().contains(".PARTITION = 24"));
                    break;
                }
                case 1: {
                    Assert.assertTrue(tsplit.getSplitSql().contains(".PARTITION = 31"));
                    break;
                }
                case 2: {
                    Assert.assertTrue(tsplit.getSplitSql().contains(".PARTITION = 55"));
                    break;
                }
                case 3: {
                    Assert.assertTrue(tsplit.getSplitSql().contains(".PARTITION = 83"));
                    break;
                }
                case 4: {
                    Assert.assertTrue(tsplit.getSplitSql().contains(".PARTITION = 110"));
                    break;
                }
                case 5: {
                    Assert.assertTrue(tsplit.getSplitSql().contains(".PARTITION = 127"));
                    break;
                }
                case 6: {
                    Assert.assertTrue(tsplit.getSplitSql().contains(".PARTITION = 133"));
                    break;
                }
                case 7: {
                    Assert.assertTrue(tsplit.getSplitSql().contains(".PARTITION = 162"));
                    break;
                }
                case 8: {
                    Assert.assertTrue(tsplit.getSplitSql().contains(".PARTITION = 170"));
                    break;
                }
                case 9: {
                    Assert.assertTrue(tsplit.getSplitSql().contains(".PARTITION = 179"));
                    break;
                }
            }
            ++count;
        }
        processor.inputPostProcessor(context);
    }

    @Test
    public void testGetSplitsStagingDisabled4() throws Exception {
        final boolean stagingEnabled = false;
        final TeradataSplitByPartitionProcessor processor = new TeradataSplitByPartitionProcessor();
        final JobContext context = (JobContext) new Job();
        final Configuration configuration = context.getConfiguration();
        configuration.set("tdch.num.mappers", "12");
        configuration.set("tdch.input.teradata.num.partitions", "10");
        configuration.set("tdch.input.teradata.stage.table.enabled", stagingEnabled ? "true" : "false");
        TeradataPlugInConfiguration.setInputJdbcDriverClass(configuration, TeradataSplitByPartitionInputFormatTest.dbJdbcDriverClass);
        TeradataPlugInConfiguration.setInputJdbcUrl(configuration, TeradataSplitByPartitionInputFormatTest.dbJdbcUrl);
        TeradataPlugInConfiguration.setInputTable(configuration, TeradataSplitByPartitionInputFormatTest.dbDatabase + ".splitbypartition_table_10partitions");
        TeradataPlugInConfiguration.setInputStageTableEnabled(configuration, stagingEnabled);
        TeradataPlugInConfiguration.setInputTeradataUserName(context, TeradataSplitByPartitionInputFormatTest.dbUsername.getBytes(StandardCharsets.UTF_8));
        TeradataPlugInConfiguration.setInputTeradataPassword(context, TeradataSplitByPartitionInputFormatTest.dbPassword.getBytes(StandardCharsets.UTF_8));
        TeradataSchemaUtils.setupTeradataSourceTableSchema(configuration, TeradataSplitByPartitionInputFormatTest.dbConnection);
        processor.inputPreProcessor(context);
        TeradataSchemaUtils.configureSourceConnectorRecordSchema(configuration);
        final TeradataSplitByPartitionInputFormat sbpInputFormat = new TeradataSplitByPartitionInputFormat();
        final List<InputSplit> inputSplits = sbpInputFormat.getSplits(context);
        Assert.assertTrue(inputSplits.size() == 10);
        int count = 0;
        for (final InputSplit split : inputSplits) {
            final TeradataInputFormat.TeradataInputSplit tsplit = (TeradataInputFormat.TeradataInputSplit) split;
            switch (count) {
                case 0: {
                    Assert.assertTrue(tsplit.getSplitSql().contains(".PARTITION = 24"));
                    break;
                }
                case 1: {
                    Assert.assertTrue(tsplit.getSplitSql().contains(".PARTITION = 31"));
                    break;
                }
                case 2: {
                    Assert.assertTrue(tsplit.getSplitSql().contains(".PARTITION = 55"));
                    break;
                }
                case 3: {
                    Assert.assertTrue(tsplit.getSplitSql().contains(".PARTITION = 83"));
                    break;
                }
                case 4: {
                    Assert.assertTrue(tsplit.getSplitSql().contains(".PARTITION = 110"));
                    break;
                }
                case 5: {
                    Assert.assertTrue(tsplit.getSplitSql().contains(".PARTITION = 127"));
                    break;
                }
                case 6: {
                    Assert.assertTrue(tsplit.getSplitSql().contains(".PARTITION = 133"));
                    break;
                }
                case 7: {
                    Assert.assertTrue(tsplit.getSplitSql().contains(".PARTITION = 162"));
                    break;
                }
                case 8: {
                    Assert.assertTrue(tsplit.getSplitSql().contains(".PARTITION = 170"));
                    break;
                }
                case 9: {
                    Assert.assertTrue(tsplit.getSplitSql().contains(".PARTITION = 179"));
                    break;
                }
            }
            ++count;
        }
        processor.inputPostProcessor(context);
    }

    @Test
    public void testGetSplitsStagingDisabled5() throws Exception {
        final boolean stagingEnabled = false;
        final TeradataSplitByPartitionProcessor processor = new TeradataSplitByPartitionProcessor();
        final JobContext context = (JobContext) new Job();
        final Configuration configuration = context.getConfiguration();
        configuration.set("tdch.num.mappers", "1");
        configuration.set("tdch.input.teradata.num.partitions", "10");
        configuration.set("tdch.input.teradata.stage.table.enabled", stagingEnabled ? "true" : "false");
        TeradataPlugInConfiguration.setInputJdbcDriverClass(configuration, TeradataSplitByPartitionInputFormatTest.dbJdbcDriverClass);
        TeradataPlugInConfiguration.setInputJdbcUrl(configuration, TeradataSplitByPartitionInputFormatTest.dbJdbcUrl);
        TeradataPlugInConfiguration.setInputTable(configuration, TeradataSplitByPartitionInputFormatTest.dbDatabase + ".splitbypartition_table_10partitions");
        TeradataPlugInConfiguration.setInputStageTableEnabled(configuration, stagingEnabled);
        TeradataPlugInConfiguration.setInputTeradataUserName(context, TeradataSplitByPartitionInputFormatTest.dbUsername.getBytes(StandardCharsets.UTF_8));
        TeradataPlugInConfiguration.setInputTeradataPassword(context, TeradataSplitByPartitionInputFormatTest.dbPassword.getBytes(StandardCharsets.UTF_8));
        TeradataSchemaUtils.setupTeradataSourceTableSchema(configuration, TeradataSplitByPartitionInputFormatTest.dbConnection);
        processor.inputPreProcessor(context);
        TeradataSchemaUtils.configureSourceConnectorRecordSchema(configuration);
        final TeradataSplitByPartitionInputFormat sbpInputFormat = new TeradataSplitByPartitionInputFormat();
        final List<InputSplit> inputSplits = sbpInputFormat.getSplits(context);
        Assert.assertTrue(inputSplits.size() == 1);
        final TeradataInputFormat.TeradataInputSplit tsplit = (TeradataInputFormat.TeradataInputSplit) inputSplits.get(0);
        Assert.assertTrue(tsplit.getSplitSql().contains("SELECT \"col1\", \"col2\", \"col3\" FROM \"" + TeradataSplitByPartitionInputFormatTest.dbDatabase + "\".\"splitbypartition_table_10partitions\""));
        processor.inputPostProcessor(context);
    }

    @Test
    public void testGetSplitsStagingDisabled6() throws Exception {
        final boolean stagingEnabled = false;
        final TeradataSplitByPartitionProcessor processor = new TeradataSplitByPartitionProcessor();
        final JobContext context = (JobContext) new Job();
        final Configuration configuration = context.getConfiguration();
        configuration.set("tdch.num.mappers", "3");
        configuration.set("tdch.input.teradata.num.partitions", "32");
        configuration.set("tdch.input.teradata.stage.table.enabled", stagingEnabled ? "true" : "false");
        TeradataPlugInConfiguration.setInputJdbcDriverClass(configuration, TeradataSplitByPartitionInputFormatTest.dbJdbcDriverClass);
        TeradataPlugInConfiguration.setInputJdbcUrl(configuration, TeradataSplitByPartitionInputFormatTest.dbJdbcUrl);
        TeradataPlugInConfiguration.setInputTable(configuration, TeradataSplitByPartitionInputFormatTest.dbDatabase + ".splitbypartition_table_32partitions");
        TeradataPlugInConfiguration.setInputStageTableEnabled(configuration, stagingEnabled);
        TeradataPlugInConfiguration.setInputTeradataUserName(context, TeradataSplitByPartitionInputFormatTest.dbUsername.getBytes(StandardCharsets.UTF_8));
        TeradataPlugInConfiguration.setInputTeradataPassword(context, TeradataSplitByPartitionInputFormatTest.dbPassword.getBytes(StandardCharsets.UTF_8));
        TeradataSchemaUtils.setupTeradataSourceTableSchema(configuration, TeradataSplitByPartitionInputFormatTest.dbConnection);
        processor.inputPreProcessor(context);
        TeradataSchemaUtils.configureSourceConnectorRecordSchema(configuration);
        final TeradataSplitByPartitionInputFormat sbpInputFormat = new TeradataSplitByPartitionInputFormat();
        final List<InputSplit> inputSplits = sbpInputFormat.getSplits(context);
        Assert.assertTrue(inputSplits.size() == 3);
        int count = 0;
        for (final InputSplit split : inputSplits) {
            final TeradataInputFormat.TeradataInputSplit tsplit = (TeradataInputFormat.TeradataInputSplit) split;
            switch (count) {
                case 0: {
                    Assert.assertTrue(tsplit.getSplitSql().contains(".PARTITION BETWEEN 12 AND 129"));
                    break;
                }
                case 1: {
                    Assert.assertTrue(tsplit.getSplitSql().contains(".PARTITION BETWEEN 130 AND 247"));
                    break;
                }
                case 2: {
                    Assert.assertTrue(tsplit.getSplitSql().contains(".PARTITION BETWEEN 248 AND 365"));
                    break;
                }
            }
            ++count;
        }
        processor.inputPostProcessor(context);
    }

    @Test
    public void testGetSplitsStagingDisabled7() throws Exception {
        final boolean stagingEnabled = false;
        final TeradataSplitByPartitionProcessor processor = new TeradataSplitByPartitionProcessor();
        final JobContext context = (JobContext) new Job();
        final Configuration configuration = context.getConfiguration();
        configuration.set("tdch.num.mappers", "1");
        configuration.set("tdch.input.teradata.stage.table.enabled", stagingEnabled ? "true" : "false");
        TeradataPlugInConfiguration.setInputJdbcDriverClass(configuration, TeradataSplitByPartitionInputFormatTest.dbJdbcDriverClass);
        TeradataPlugInConfiguration.setInputJdbcUrl(configuration, TeradataSplitByPartitionInputFormatTest.dbJdbcUrl);
        TeradataPlugInConfiguration.setInputTable(configuration, TeradataSplitByPartitionInputFormatTest.dbDatabase + ".splitbypartition_table_nopartitions");
        TeradataPlugInConfiguration.setInputStageTableEnabled(configuration, stagingEnabled);
        TeradataPlugInConfiguration.setInputTeradataUserName(context, TeradataSplitByPartitionInputFormatTest.dbUsername.getBytes(StandardCharsets.UTF_8));
        TeradataPlugInConfiguration.setInputTeradataPassword(context, TeradataSplitByPartitionInputFormatTest.dbPassword.getBytes(StandardCharsets.UTF_8));
        TeradataSchemaUtils.setupTeradataSourceTableSchema(configuration, TeradataSplitByPartitionInputFormatTest.dbConnection);
        processor.inputPreProcessor(context);
        TeradataSchemaUtils.configureSourceConnectorRecordSchema(configuration);
        final TeradataSplitByPartitionInputFormat sbpInputFormat = new TeradataSplitByPartitionInputFormat();
        final List<InputSplit> inputSplits = sbpInputFormat.getSplits(context);
        Assert.assertTrue(inputSplits.size() == 1);
        final TeradataInputFormat.TeradataInputSplit tsplit = (TeradataInputFormat.TeradataInputSplit) inputSplits.get(0);
        Assert.assertTrue(tsplit.getSplitSql().contains("SELECT \"col1\", \"col2\", \"col3\" FROM \"" + TeradataSplitByPartitionInputFormatTest.dbDatabase + "\".\"splitbypartition_table_nopartitions\""));
        processor.inputPostProcessor(context);
    }

    private static void createDatabaseTables() throws Exception {
        dropDatabaseTables();
        TeradataSplitByPartitionInputFormatTest.dbConnection.executeDDL("CREATE TABLE " + TeradataSplitByPartitionInputFormatTest.dbDatabase + ".splitbypartition_table_nodata (col1 INT, col2 INT, col3 VARCHAR(20)) PARTITION BY (col1), PRIMARY INDEX (col1);");
        TeradataSplitByPartitionInputFormatTest.dbConnection.executeDDL("CREATE TABLE " + TeradataSplitByPartitionInputFormatTest.dbDatabase + ".splitbypartition_table_nopartitions (col1 INT, col2 INT, col3 VARCHAR(20));");
        TeradataSplitByPartitionInputFormatTest.dbConnection.executeDDL("INSERT INTO " + TeradataSplitByPartitionInputFormatTest.dbDatabase + ".splitbypartition_table_nopartitions VALUES (24,95,'hello world');");
        TeradataSplitByPartitionInputFormatTest.dbConnection.executeDDL("INSERT INTO " + TeradataSplitByPartitionInputFormatTest.dbDatabase + ".splitbypartition_table_nopartitions VALUES (83,40,'hello world');");
        TeradataSplitByPartitionInputFormatTest.dbConnection.executeDDL("INSERT INTO " + TeradataSplitByPartitionInputFormatTest.dbDatabase + ".splitbypartition_table_nopartitions VALUES (55,24,'hello world');");
        TeradataSplitByPartitionInputFormatTest.dbConnection.executeDDL("INSERT INTO " + TeradataSplitByPartitionInputFormatTest.dbDatabase + ".splitbypartition_table_nopartitions VALUES (31,116,'hello world');");
        TeradataSplitByPartitionInputFormatTest.dbConnection.executeDDL("INSERT INTO " + TeradataSplitByPartitionInputFormatTest.dbDatabase + ".splitbypartition_table_nopartitions VALUES (133,54,'hello world');");
        TeradataSplitByPartitionInputFormatTest.dbConnection.executeDDL("INSERT INTO " + TeradataSplitByPartitionInputFormatTest.dbDatabase + ".splitbypartition_table_nopartitions VALUES (179,147,'hello world');");
        TeradataSplitByPartitionInputFormatTest.dbConnection.executeDDL("INSERT INTO " + TeradataSplitByPartitionInputFormatTest.dbDatabase + ".splitbypartition_table_nopartitions VALUES (110,41,'hello world');");
        TeradataSplitByPartitionInputFormatTest.dbConnection.executeDDL("INSERT INTO " + TeradataSplitByPartitionInputFormatTest.dbDatabase + ".splitbypartition_table_nopartitions VALUES (170,167,'hello world');");
        TeradataSplitByPartitionInputFormatTest.dbConnection.executeDDL("INSERT INTO " + TeradataSplitByPartitionInputFormatTest.dbDatabase + ".splitbypartition_table_nopartitions VALUES (127,107,'hello world');");
        TeradataSplitByPartitionInputFormatTest.dbConnection.executeDDL("INSERT INTO " + TeradataSplitByPartitionInputFormatTest.dbDatabase + ".splitbypartition_table_nopartitions VALUES (162,181,'hello world');");
        TeradataSplitByPartitionInputFormatTest.dbConnection.executeDDL("CREATE TABLE " + TeradataSplitByPartitionInputFormatTest.dbDatabase + ".splitbypartition_table_10partitions (col1 INT, col2 INT, col3 VARCHAR(20)) PARTITION BY (col1), PRIMARY INDEX (col1);");
        TeradataSplitByPartitionInputFormatTest.dbConnection.executeDDL("INSERT INTO " + TeradataSplitByPartitionInputFormatTest.dbDatabase + ".splitbypartition_table_10partitions VALUES (24,95,'hello world');");
        TeradataSplitByPartitionInputFormatTest.dbConnection.executeDDL("INSERT INTO " + TeradataSplitByPartitionInputFormatTest.dbDatabase + ".splitbypartition_table_10partitions VALUES (83,40,'hello world');");
        TeradataSplitByPartitionInputFormatTest.dbConnection.executeDDL("INSERT INTO " + TeradataSplitByPartitionInputFormatTest.dbDatabase + ".splitbypartition_table_10partitions VALUES (55,24,'hello world');");
        TeradataSplitByPartitionInputFormatTest.dbConnection.executeDDL("INSERT INTO " + TeradataSplitByPartitionInputFormatTest.dbDatabase + ".splitbypartition_table_10partitions VALUES (31,116,'hello world');");
        TeradataSplitByPartitionInputFormatTest.dbConnection.executeDDL("INSERT INTO " + TeradataSplitByPartitionInputFormatTest.dbDatabase + ".splitbypartition_table_10partitions VALUES (133,54,'hello world');");
        TeradataSplitByPartitionInputFormatTest.dbConnection.executeDDL("INSERT INTO " + TeradataSplitByPartitionInputFormatTest.dbDatabase + ".splitbypartition_table_10partitions VALUES (179,147,'hello world');");
        TeradataSplitByPartitionInputFormatTest.dbConnection.executeDDL("INSERT INTO " + TeradataSplitByPartitionInputFormatTest.dbDatabase + ".splitbypartition_table_10partitions VALUES (110,41,'hello world');");
        TeradataSplitByPartitionInputFormatTest.dbConnection.executeDDL("INSERT INTO " + TeradataSplitByPartitionInputFormatTest.dbDatabase + ".splitbypartition_table_10partitions VALUES (170,167,'hello world');");
        TeradataSplitByPartitionInputFormatTest.dbConnection.executeDDL("INSERT INTO " + TeradataSplitByPartitionInputFormatTest.dbDatabase + ".splitbypartition_table_10partitions VALUES (127,107,'hello world');");
        TeradataSplitByPartitionInputFormatTest.dbConnection.executeDDL("INSERT INTO " + TeradataSplitByPartitionInputFormatTest.dbDatabase + ".splitbypartition_table_10partitions VALUES (162,181,'hello world');");
        TeradataSplitByPartitionInputFormatTest.dbConnection.executeDDL("CREATE TABLE " + TeradataSplitByPartitionInputFormatTest.dbDatabase + ".splitbypartition_table_32partitions (col1 INT, col2 INT, col3 VARCHAR(20)) PARTITION BY (col1), PRIMARY INDEX (col1);");
        TeradataSplitByPartitionInputFormatTest.dbConnection.executeDDL("INSERT INTO " + TeradataSplitByPartitionInputFormatTest.dbDatabase + ".splitbypartition_table_32partitions VALUES (24,95,'hello world');");
        TeradataSplitByPartitionInputFormatTest.dbConnection.executeDDL("INSERT INTO " + TeradataSplitByPartitionInputFormatTest.dbDatabase + ".splitbypartition_table_32partitions VALUES (83,40,'hello world');");
        TeradataSplitByPartitionInputFormatTest.dbConnection.executeDDL("INSERT INTO " + TeradataSplitByPartitionInputFormatTest.dbDatabase + ".splitbypartition_table_32partitions VALUES (55,24,'hello world');");
        TeradataSplitByPartitionInputFormatTest.dbConnection.executeDDL("INSERT INTO " + TeradataSplitByPartitionInputFormatTest.dbDatabase + ".splitbypartition_table_32partitions VALUES (31,116,'hello world');");
        TeradataSplitByPartitionInputFormatTest.dbConnection.executeDDL("INSERT INTO " + TeradataSplitByPartitionInputFormatTest.dbDatabase + ".splitbypartition_table_32partitions VALUES (133,54,'hello world');");
        TeradataSplitByPartitionInputFormatTest.dbConnection.executeDDL("INSERT INTO " + TeradataSplitByPartitionInputFormatTest.dbDatabase + ".splitbypartition_table_32partitions VALUES (179,147,'hello world');");
        TeradataSplitByPartitionInputFormatTest.dbConnection.executeDDL("INSERT INTO " + TeradataSplitByPartitionInputFormatTest.dbDatabase + ".splitbypartition_table_32partitions VALUES (110,41,'hello world');");
        TeradataSplitByPartitionInputFormatTest.dbConnection.executeDDL("INSERT INTO " + TeradataSplitByPartitionInputFormatTest.dbDatabase + ".splitbypartition_table_32partitions VALUES (170,167,'hello world');");
        TeradataSplitByPartitionInputFormatTest.dbConnection.executeDDL("INSERT INTO " + TeradataSplitByPartitionInputFormatTest.dbDatabase + ".splitbypartition_table_32partitions VALUES (127,107,'hello world');");
        TeradataSplitByPartitionInputFormatTest.dbConnection.executeDDL("INSERT INTO " + TeradataSplitByPartitionInputFormatTest.dbDatabase + ".splitbypartition_table_32partitions VALUES (319,88,'hello world');");
        TeradataSplitByPartitionInputFormatTest.dbConnection.executeDDL("INSERT INTO " + TeradataSplitByPartitionInputFormatTest.dbDatabase + ".splitbypartition_table_32partitions VALUES (162,181,'hello world');");
        TeradataSplitByPartitionInputFormatTest.dbConnection.executeDDL("INSERT INTO " + TeradataSplitByPartitionInputFormatTest.dbDatabase + ".splitbypartition_table_32partitions VALUES (33,200,'hello world');");
        TeradataSplitByPartitionInputFormatTest.dbConnection.executeDDL("INSERT INTO " + TeradataSplitByPartitionInputFormatTest.dbDatabase + ".splitbypartition_table_32partitions VALUES (72,101,'hello world');");
        TeradataSplitByPartitionInputFormatTest.dbConnection.executeDDL("INSERT INTO " + TeradataSplitByPartitionInputFormatTest.dbDatabase + ".splitbypartition_table_32partitions VALUES (89,26,'hello world');");
        TeradataSplitByPartitionInputFormatTest.dbConnection.executeDDL("INSERT INTO " + TeradataSplitByPartitionInputFormatTest.dbDatabase + ".splitbypartition_table_32partitions VALUES (101,11,'hello world');");
        TeradataSplitByPartitionInputFormatTest.dbConnection.executeDDL("INSERT INTO " + TeradataSplitByPartitionInputFormatTest.dbDatabase + ".splitbypartition_table_32partitions VALUES (102,76,'hello world');");
        TeradataSplitByPartitionInputFormatTest.dbConnection.executeDDL("INSERT INTO " + TeradataSplitByPartitionInputFormatTest.dbDatabase + ".splitbypartition_table_32partitions VALUES (122,34,'hello world');");
        TeradataSplitByPartitionInputFormatTest.dbConnection.executeDDL("INSERT INTO " + TeradataSplitByPartitionInputFormatTest.dbDatabase + ".splitbypartition_table_32partitions VALUES (131,298,'hello world');");
        TeradataSplitByPartitionInputFormatTest.dbConnection.executeDDL("INSERT INTO " + TeradataSplitByPartitionInputFormatTest.dbDatabase + ".splitbypartition_table_32partitions VALUES (144,97,'hello world');");
        TeradataSplitByPartitionInputFormatTest.dbConnection.executeDDL("INSERT INTO " + TeradataSplitByPartitionInputFormatTest.dbDatabase + ".splitbypartition_table_32partitions VALUES (181,43,'hello world');");
        TeradataSplitByPartitionInputFormatTest.dbConnection.executeDDL("INSERT INTO " + TeradataSplitByPartitionInputFormatTest.dbDatabase + ".splitbypartition_table_32partitions VALUES (201,40,'hello world');");
        TeradataSplitByPartitionInputFormatTest.dbConnection.executeDDL("INSERT INTO " + TeradataSplitByPartitionInputFormatTest.dbDatabase + ".splitbypartition_table_32partitions VALUES (176,35,'hello world');");
        TeradataSplitByPartitionInputFormatTest.dbConnection.executeDDL("INSERT INTO " + TeradataSplitByPartitionInputFormatTest.dbDatabase + ".splitbypartition_table_32partitions VALUES (207,90,'hello world');");
        TeradataSplitByPartitionInputFormatTest.dbConnection.executeDDL("INSERT INTO " + TeradataSplitByPartitionInputFormatTest.dbDatabase + ".splitbypartition_table_32partitions VALUES (213,81,'hello world');");
        TeradataSplitByPartitionInputFormatTest.dbConnection.executeDDL("INSERT INTO " + TeradataSplitByPartitionInputFormatTest.dbDatabase + ".splitbypartition_table_32partitions VALUES (287,63,'hello world');");
        TeradataSplitByPartitionInputFormatTest.dbConnection.executeDDL("INSERT INTO " + TeradataSplitByPartitionInputFormatTest.dbDatabase + ".splitbypartition_table_32partitions VALUES (265,204,'hello world');");
        TeradataSplitByPartitionInputFormatTest.dbConnection.executeDDL("INSERT INTO " + TeradataSplitByPartitionInputFormatTest.dbDatabase + ".splitbypartition_table_32partitions VALUES (365,243,'hello world');");
        TeradataSplitByPartitionInputFormatTest.dbConnection.executeDDL("INSERT INTO " + TeradataSplitByPartitionInputFormatTest.dbDatabase + ".splitbypartition_table_32partitions VALUES (344,222,'hello world');");
        TeradataSplitByPartitionInputFormatTest.dbConnection.executeDDL("INSERT INTO " + TeradataSplitByPartitionInputFormatTest.dbDatabase + ".splitbypartition_table_32partitions VALUES (307,178,'hello world');");
        TeradataSplitByPartitionInputFormatTest.dbConnection.executeDDL("INSERT INTO " + TeradataSplitByPartitionInputFormatTest.dbDatabase + ".splitbypartition_table_32partitions VALUES (234,333,'hello world');");
        TeradataSplitByPartitionInputFormatTest.dbConnection.executeDDL("INSERT INTO " + TeradataSplitByPartitionInputFormatTest.dbDatabase + ".splitbypartition_table_32partitions VALUES (12,71,'hello world');");
        TeradataSplitByPartitionInputFormatTest.dbConnection.executeDDL("INSERT INTO " + TeradataSplitByPartitionInputFormatTest.dbDatabase + ".splitbypartition_table_32partitions VALUES (45,302,'hello world');");
    }

    private static void dropDatabaseTables() throws Exception {
        try {
            TeradataSplitByPartitionInputFormatTest.dbConnection.dropTable(TeradataSplitByPartitionInputFormatTest.dbDatabase + ".splitbypartition_table_nodata");
        } catch (SQLException ex) {
        }
        try {
            TeradataSplitByPartitionInputFormatTest.dbConnection.dropTable(TeradataSplitByPartitionInputFormatTest.dbDatabase + ".splitbypartition_table_nopartitions");
        } catch (SQLException ex2) {
        }
        try {
            TeradataSplitByPartitionInputFormatTest.dbConnection.dropTable(TeradataSplitByPartitionInputFormatTest.dbDatabase + ".splitbypartition_table_10partitions");
        } catch (SQLException ex3) {
        }
        try {
            TeradataSplitByPartitionInputFormatTest.dbConnection.dropTable(TeradataSplitByPartitionInputFormatTest.dbDatabase + ".splitbypartition_table_32partitions");
        } catch (SQLException ex4) {
        }
    }
}
