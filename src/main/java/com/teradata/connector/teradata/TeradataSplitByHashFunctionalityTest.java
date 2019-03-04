package com.teradata.connector.teradata;

import com.teradata.connector.common.exception.ConnectorException;
import com.teradata.connector.common.utils.StandardCharsets;
import com.teradata.connector.teradata.db.TeradataConnection;
import com.teradata.connector.teradata.processor.TeradataInputProcessor;
import com.teradata.connector.teradata.processor.TeradataSplitByHashProcessor;
import com.teradata.connector.teradata.utils.TeradataPlugInConfiguration;
import com.teradata.connector.teradata.utils.TeradataSchemaUtils;
import java.io.IOException;
import java.sql.SQLException;
import junit.framework.Assert;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.JobContext;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;


public class TeradataSplitByHashFunctionalityTest {
    private static String dbJdbcUrl;
    private static String dbIpAddress;
    private static String dbDatabase;
    private static String dbUsername;
    private static String dbPassword;
    private static String dbJdbcDriverClass;
    private static TeradataConnection dbConnection;
    private static Log logger;
    protected static final int VALUE_PARTITION_ID_TYPE = 4;
    protected static final String COLUMN_PARTITION_ID = "TDIN_PARTID";
    protected static final String COLUMN_PARTITION = "PARTITION";
    protected static final int PARTITION_RANGE_MIN = 10;

    @BeforeClass
    public static void initializeTests() throws Exception {
        TeradataSplitByHashFunctionalityTest.dbIpAddress = System.getProperty("test.database.ipaddress", "10.25.35.197");
        TeradataSplitByHashFunctionalityTest.dbDatabase = System.getProperty("test.database.databasename", "tdch_testing2");
        TeradataSplitByHashFunctionalityTest.dbUsername = System.getProperty("test.database.username", "tdch_testing2");
        TeradataSplitByHashFunctionalityTest.dbPassword = System.getProperty("test.database.userpassword", "tdch_testing2");
        TeradataSplitByHashFunctionalityTest.dbJdbcDriverClass = System.getProperty("test.database.jdbcdriver.class", "com.teradata.jdbc.TeraDriver");
        TeradataSplitByHashFunctionalityTest.dbJdbcUrl = "jdbc:teradata://" + TeradataSplitByHashFunctionalityTest.dbIpAddress + "/database=" + TeradataSplitByHashFunctionalityTest.dbDatabase;
        (TeradataSplitByHashFunctionalityTest.dbConnection = new TeradataConnection(TeradataSplitByHashFunctionalityTest.dbJdbcDriverClass, TeradataSplitByHashFunctionalityTest.dbJdbcUrl, TeradataSplitByHashFunctionalityTest.dbUsername, TeradataSplitByHashFunctionalityTest.dbPassword, false)).connect();
        createDatabaseTables();
    }

    @AfterClass
    public static void cleanupTests() throws Exception {
        dropDatabaseTables();
        TeradataSplitByHashFunctionalityTest.dbConnection.close();
    }

    @Test
    public void testValidateConfigurationFailNumMappers2() {
        try {
            final TeradataSplitByHashProcessor processor = new TeradataSplitByHashProcessor();
            final JobContext context = (JobContext) new Job();
            final Configuration configuration = context.getConfiguration();
            configuration.set("tdch.num.mappers", "2");
            TeradataPlugInConfiguration.setInputJdbcDriverClass(configuration, TeradataSplitByHashFunctionalityTest.dbJdbcDriverClass);
            TeradataPlugInConfiguration.setInputJdbcUrl(configuration, TeradataSplitByHashFunctionalityTest.dbJdbcUrl);
            TeradataPlugInConfiguration.setInputTable(configuration, TeradataSplitByHashFunctionalityTest.dbDatabase + ".TDCHIMPORTTEST2");
            TeradataPlugInConfiguration.setInputTeradataUserName(context, TeradataSplitByHashFunctionalityTest.dbUsername.getBytes(StandardCharsets.UTF_8));
            TeradataPlugInConfiguration.setInputTeradataPassword(context, TeradataSplitByHashFunctionalityTest.dbPassword.getBytes(StandardCharsets.UTF_8));
            TeradataSchemaUtils.setupTeradataSourceTableSchema(configuration, TeradataSplitByHashFunctionalityTest.dbConnection);
            processor.inputPreProcessor(context);
        } catch (ConnectorException e) {
            e.printStackTrace();
        } catch (IOException e2) {
            e2.printStackTrace();
        }
    }

    @Test
    public void testValidateConfigurationNOPINumMappers2() throws Exception {
        final TeradataSplitByHashProcessor processor = new TeradataSplitByHashProcessor();
        final JobContext context = (JobContext) new Job();
        final Configuration configuration = context.getConfiguration();
        configuration.set("tdch.num.mappers", "2");
        TeradataPlugInConfiguration.setInputJdbcDriverClass(configuration, TeradataSplitByHashFunctionalityTest.dbJdbcDriverClass);
        TeradataPlugInConfiguration.setInputJdbcUrl(configuration, TeradataSplitByHashFunctionalityTest.dbJdbcUrl);
        TeradataPlugInConfiguration.setInputTable(configuration, TeradataSplitByHashFunctionalityTest.dbDatabase + ".TDCHIMPORTTEST");
        TeradataPlugInConfiguration.setInputTeradataUserName(context, TeradataSplitByHashFunctionalityTest.dbUsername.getBytes(StandardCharsets.UTF_8));
        TeradataPlugInConfiguration.setInputTeradataPassword(context, TeradataSplitByHashFunctionalityTest.dbPassword.getBytes(StandardCharsets.UTF_8));
        TeradataSchemaUtils.setupTeradataSourceTableSchema(configuration, TeradataSplitByHashFunctionalityTest.dbConnection);
        processor.inputPreProcessor(context);
        final String splitColumn = TeradataPlugInConfiguration.getInputSplitByColumn(configuration);
        Assert.assertTrue(splitColumn.toUpperCase().equals("NAME"));
    }

    @Test
    public void testValidateConfigurationNOPINumMappers1() throws Exception {
        final TeradataSplitByHashProcessor processor = new TeradataSplitByHashProcessor();
        final JobContext context = (JobContext) new Job();
        final Configuration configuration = context.getConfiguration();
        configuration.set("tdch.num.mappers", "1");
        TeradataPlugInConfiguration.setInputJdbcDriverClass(configuration, TeradataSplitByHashFunctionalityTest.dbJdbcDriverClass);
        TeradataPlugInConfiguration.setInputJdbcUrl(configuration, TeradataSplitByHashFunctionalityTest.dbJdbcUrl);
        TeradataPlugInConfiguration.setInputTable(configuration, TeradataSplitByHashFunctionalityTest.dbDatabase + ".TDCHIMPORTTEST");
        TeradataPlugInConfiguration.setInputTeradataUserName(context, TeradataSplitByHashFunctionalityTest.dbUsername.getBytes(StandardCharsets.UTF_8));
        TeradataPlugInConfiguration.setInputTeradataPassword(context, TeradataSplitByHashFunctionalityTest.dbPassword.getBytes(StandardCharsets.UTF_8));
        TeradataSchemaUtils.setupTeradataSourceTableSchema(configuration, TeradataSplitByHashFunctionalityTest.dbConnection);
        processor.inputPreProcessor(context);
        final String splitColumn = TeradataPlugInConfiguration.getInputSplitByColumn(configuration);
        Assert.assertTrue(splitColumn.toUpperCase().equals(""));
    }

    @Test
    public void testValidateConfigurationPINumMappers1() throws Exception {
        final TeradataSplitByHashProcessor processor = new TeradataSplitByHashProcessor();
        final JobContext context = (JobContext) new Job();
        final Configuration configuration = context.getConfiguration();
        configuration.set("tdch.num.mappers", "1");
        TeradataPlugInConfiguration.setInputJdbcDriverClass(configuration, TeradataSplitByHashFunctionalityTest.dbJdbcDriverClass);
        TeradataPlugInConfiguration.setInputJdbcUrl(configuration, TeradataSplitByHashFunctionalityTest.dbJdbcUrl);
        TeradataPlugInConfiguration.setInputTable(configuration, TeradataSplitByHashFunctionalityTest.dbDatabase + ".TDCHIMPORTTESTPI");
        TeradataPlugInConfiguration.setInputTeradataUserName(context, TeradataSplitByHashFunctionalityTest.dbUsername.getBytes(StandardCharsets.UTF_8));
        TeradataPlugInConfiguration.setInputTeradataPassword(context, TeradataSplitByHashFunctionalityTest.dbPassword.getBytes(StandardCharsets.UTF_8));
        TeradataSchemaUtils.setupTeradataSourceTableSchema(configuration, TeradataSplitByHashFunctionalityTest.dbConnection);
        processor.inputPreProcessor(context);
        final String splitColumn = TeradataPlugInConfiguration.getInputSplitByColumn(configuration);
        Assert.assertTrue(splitColumn.toUpperCase().equals(""));
    }

    @Test
    public void testValidateConfigurationPINumMappers2() throws Exception {
        final TeradataSplitByHashProcessor processor = new TeradataSplitByHashProcessor();
        final JobContext context = (JobContext) new Job();
        final Configuration configuration = context.getConfiguration();
        configuration.set("tdch.num.mappers", "2");
        TeradataPlugInConfiguration.setInputJdbcDriverClass(configuration, TeradataSplitByHashFunctionalityTest.dbJdbcDriverClass);
        TeradataPlugInConfiguration.setInputJdbcUrl(configuration, TeradataSplitByHashFunctionalityTest.dbJdbcUrl);
        TeradataPlugInConfiguration.setInputTable(configuration, TeradataSplitByHashFunctionalityTest.dbDatabase + ".TDCHIMPORTTESTPI");
        TeradataPlugInConfiguration.setInputTeradataUserName(context, TeradataSplitByHashFunctionalityTest.dbUsername.getBytes(StandardCharsets.UTF_8));
        TeradataPlugInConfiguration.setInputTeradataPassword(context, TeradataSplitByHashFunctionalityTest.dbPassword.getBytes(StandardCharsets.UTF_8));
        TeradataSchemaUtils.setupTeradataSourceTableSchema(configuration, TeradataSplitByHashFunctionalityTest.dbConnection);
        processor.inputPreProcessor(context);
        final String splitColumn = TeradataPlugInConfiguration.getInputSplitByColumn(configuration);
        Assert.assertTrue(splitColumn.toUpperCase().equals("NAME"));
    }

    @Test
    public void testValidateConfigurationNOPICol1NumMappers1() throws Exception {
        final TeradataSplitByHashProcessor processor = new TeradataSplitByHashProcessor();
        final JobContext context = (JobContext) new Job();
        final Configuration configuration = context.getConfiguration();
        configuration.set("tdch.num.mappers", "1");
        TeradataPlugInConfiguration.setInputJdbcDriverClass(configuration, TeradataSplitByHashFunctionalityTest.dbJdbcDriverClass);
        TeradataPlugInConfiguration.setInputJdbcUrl(configuration, TeradataSplitByHashFunctionalityTest.dbJdbcUrl);
        TeradataPlugInConfiguration.setInputTable(configuration, TeradataSplitByHashFunctionalityTest.dbDatabase + ".TDCHIMPORTTESTNOPI1");
        TeradataPlugInConfiguration.setInputTeradataUserName(context, TeradataSplitByHashFunctionalityTest.dbUsername.getBytes(StandardCharsets.UTF_8));
        TeradataPlugInConfiguration.setInputTeradataPassword(context, TeradataSplitByHashFunctionalityTest.dbPassword.getBytes(StandardCharsets.UTF_8));
        TeradataSchemaUtils.setupTeradataSourceTableSchema(configuration, TeradataSplitByHashFunctionalityTest.dbConnection);
        processor.inputPreProcessor(context);
        final String splitColumn = TeradataPlugInConfiguration.getInputSplitByColumn(configuration);
        Assert.assertTrue(splitColumn.toUpperCase().equals(""));
    }

    @Test
    public void testValidateConfigurationNOPICol1NumMappers2() throws Exception {
        final TeradataSplitByHashProcessor processor = new TeradataSplitByHashProcessor();
        final JobContext context = (JobContext) new Job();
        final Configuration configuration = context.getConfiguration();
        configuration.set("tdch.num.mappers", "2");
        TeradataPlugInConfiguration.setInputJdbcDriverClass(configuration, TeradataSplitByHashFunctionalityTest.dbJdbcDriverClass);
        TeradataPlugInConfiguration.setInputJdbcUrl(configuration, TeradataSplitByHashFunctionalityTest.dbJdbcUrl);
        TeradataPlugInConfiguration.setInputTable(configuration, TeradataSplitByHashFunctionalityTest.dbDatabase + ".TDCHIMPORTTESTNOPI1");
        TeradataPlugInConfiguration.setInputTeradataUserName(context, TeradataSplitByHashFunctionalityTest.dbUsername.getBytes(StandardCharsets.UTF_8));
        TeradataPlugInConfiguration.setInputTeradataPassword(context, TeradataSplitByHashFunctionalityTest.dbPassword.getBytes(StandardCharsets.UTF_8));
        TeradataSchemaUtils.setupTeradataSourceTableSchema(configuration, TeradataSplitByHashFunctionalityTest.dbConnection);
        processor.inputPreProcessor(context);
        final String splitColumn = TeradataPlugInConfiguration.getInputSplitByColumn(configuration);
        Assert.assertTrue(splitColumn.toUpperCase().equals("NAME"));
    }

    private static void createDatabaseTables() throws Exception {
        dropDatabaseTables();
        TeradataSplitByHashFunctionalityTest.dbConnection.executeDDL("CREATE MULTISET TABLE " + TeradataSplitByHashFunctionalityTest.dbDatabase + ".TDCHIMPORTTEST (NAME VARCHAR(10),ADDR VARCHAR(10)) NO PRIMARY INDEX;");
        TeradataSplitByHashFunctionalityTest.dbConnection.executeDDL("INSERT INTO " + TeradataSplitByHashFunctionalityTest.dbDatabase + ".TDCHIMPORTTEST VALUES ('jimmy','boston');");
        TeradataSplitByHashFunctionalityTest.dbConnection.executeDDL("INSERT INTO " + TeradataSplitByHashFunctionalityTest.dbDatabase + ".TDCHIMPORTTEST VALUES ('Tom','Cumberland');");
        TeradataSplitByHashFunctionalityTest.dbConnection.executeDDL("INSERT INTO " + TeradataSplitByHashFunctionalityTest.dbDatabase + ".TDCHIMPORTTEST VALUES ('jimmy','Cumberland');");
        TeradataSplitByHashFunctionalityTest.dbConnection.executeDDL("CREATE MULTISET TABLE " + TeradataSplitByHashFunctionalityTest.dbDatabase + ".TDCHIMPORTTESTPI (NAME VARCHAR(10),ADDR VARCHAR(10)) UNIQUE PRIMARY INDEX ( NAME );");
        TeradataSplitByHashFunctionalityTest.dbConnection.executeDDL("INSERT INTO " + TeradataSplitByHashFunctionalityTest.dbDatabase + ".TDCHIMPORTTESTPI VALUES ('jimmy','boston');");
        TeradataSplitByHashFunctionalityTest.dbConnection.executeDDL("INSERT INTO " + TeradataSplitByHashFunctionalityTest.dbDatabase + ".TDCHIMPORTTESTPI VALUES ('Tom','Cumberland');");
        TeradataSplitByHashFunctionalityTest.dbConnection.executeDDL("CREATE MULTISET TABLE " + TeradataSplitByHashFunctionalityTest.dbDatabase + ".TDCHIMPORTTESTNOPI1 (NAME VARCHAR(10)) NO PRIMARY INDEX;");
        TeradataSplitByHashFunctionalityTest.dbConnection.executeDDL("INSERT INTO " + TeradataSplitByHashFunctionalityTest.dbDatabase + ".TDCHIMPORTTESTNOPI1 VALUES ('jimmy');");
        TeradataSplitByHashFunctionalityTest.dbConnection.executeDDL("INSERT INTO " + TeradataSplitByHashFunctionalityTest.dbDatabase + ".TDCHIMPORTTESTNOPI1 VALUES ('Tom');");
        TeradataSplitByHashFunctionalityTest.dbConnection.executeDDL("INSERT INTO " + TeradataSplitByHashFunctionalityTest.dbDatabase + ".TDCHIMPORTTESTNOPI1 VALUES ('jimmy');");
    }

    private static void dropDatabaseTables() throws Exception {
        try {
            TeradataSplitByHashFunctionalityTest.dbConnection.dropTable(TeradataSplitByHashFunctionalityTest.dbDatabase + ".TDCHIMPORTTEST");
        } catch (SQLException ex) {
        }
        try {
            TeradataSplitByHashFunctionalityTest.dbConnection.dropTable(TeradataSplitByHashFunctionalityTest.dbDatabase + ".TDCHIMPORTTESTPI");
        } catch (SQLException ex2) {
        }
        try {
            TeradataSplitByHashFunctionalityTest.dbConnection.dropTable(TeradataSplitByHashFunctionalityTest.dbDatabase + ".TDCHIMPORTTESTNOPI1");
        } catch (SQLException ex3) {
        }
    }

    static {
        TeradataSplitByHashFunctionalityTest.logger = LogFactory.getLog((Class) TeradataInputProcessor.class);
    }
}
