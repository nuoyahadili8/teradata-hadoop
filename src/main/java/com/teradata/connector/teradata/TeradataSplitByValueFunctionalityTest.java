package com.teradata.connector.teradata;

import com.teradata.connector.common.exception.ConnectorException;
import com.teradata.connector.common.utils.ConnectorConfiguration;
import com.teradata.connector.common.utils.StandardCharsets;
import com.teradata.connector.teradata.db.TeradataConnection;
import com.teradata.connector.teradata.processor.TeradataInputProcessor;
import com.teradata.connector.teradata.processor.TeradataSplitByValueProcessor;
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


public class TeradataSplitByValueFunctionalityTest {
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
        TeradataSplitByValueFunctionalityTest.dbIpAddress = System.getProperty("test.database.ipaddress", "10.25.35.197");
        TeradataSplitByValueFunctionalityTest.dbDatabase = System.getProperty("test.database.databasename", "tdch_testing2");
        TeradataSplitByValueFunctionalityTest.dbUsername = System.getProperty("test.database.username", "tdch_testing2");
        TeradataSplitByValueFunctionalityTest.dbPassword = System.getProperty("test.database.userpassword", "tdch_testing2");
        TeradataSplitByValueFunctionalityTest.dbJdbcDriverClass = System.getProperty("test.database.jdbcdriver.class", "com.teradata.jdbc.TeraDriver");
        TeradataSplitByValueFunctionalityTest.dbJdbcUrl = "jdbc:teradata://" + TeradataSplitByValueFunctionalityTest.dbIpAddress + "/database=" + TeradataSplitByValueFunctionalityTest.dbDatabase;
        (TeradataSplitByValueFunctionalityTest.dbConnection = new TeradataConnection(TeradataSplitByValueFunctionalityTest.dbJdbcDriverClass, TeradataSplitByValueFunctionalityTest.dbJdbcUrl, TeradataSplitByValueFunctionalityTest.dbUsername, TeradataSplitByValueFunctionalityTest.dbPassword, false)).connect();
        createDatabaseTables();
    }

    @AfterClass
    public static void cleanupTests() throws Exception {
        dropDatabaseTables();
        TeradataSplitByValueFunctionalityTest.dbConnection.close();
    }

    @Test
    public void testValidateConfigurationFailNumMappers2() {
        try {
            final TeradataSplitByValueProcessor processor = new TeradataSplitByValueProcessor();
            final JobContext context = (JobContext) new Job();
            final Configuration configuration = context.getConfiguration();
            configuration.set("tdch.num.mappers", "2");
            TeradataPlugInConfiguration.setInputJdbcDriverClass(configuration, TeradataSplitByValueFunctionalityTest.dbJdbcDriverClass);
            TeradataPlugInConfiguration.setInputJdbcUrl(configuration, TeradataSplitByValueFunctionalityTest.dbJdbcUrl);
            TeradataPlugInConfiguration.setInputTable(configuration, TeradataSplitByValueFunctionalityTest.dbDatabase + ".TDCHIMPORTTEST2");
            TeradataPlugInConfiguration.setInputTeradataUserName(context, TeradataSplitByValueFunctionalityTest.dbUsername.getBytes(StandardCharsets.UTF_8));
            TeradataPlugInConfiguration.setInputTeradataPassword(context, TeradataSplitByValueFunctionalityTest.dbPassword.getBytes(StandardCharsets.UTF_8));
            TeradataSchemaUtils.setupTeradataSourceTableSchema(configuration, TeradataSplitByValueFunctionalityTest.dbConnection);
            processor.inputPreProcessor(context);
        } catch (ConnectorException e) {
            e.printStackTrace();
        } catch (IOException e2) {
            e2.printStackTrace();
        }
    }

    @Test
    public void testValidateConfigurationNOPINumMappers2() throws Exception {
        final TeradataSplitByValueProcessor processor = new TeradataSplitByValueProcessor();
        final JobContext context = (JobContext) new Job();
        final Configuration configuration = context.getConfiguration();
        configuration.set("tdch.num.mappers", "2");
        TeradataPlugInConfiguration.setInputJdbcDriverClass(configuration, TeradataSplitByValueFunctionalityTest.dbJdbcDriverClass);
        TeradataPlugInConfiguration.setInputJdbcUrl(configuration, TeradataSplitByValueFunctionalityTest.dbJdbcUrl);
        TeradataPlugInConfiguration.setInputTable(configuration, TeradataSplitByValueFunctionalityTest.dbDatabase + ".TDCHIMPORTTEST");
        TeradataPlugInConfiguration.setInputTeradataUserName(context, TeradataSplitByValueFunctionalityTest.dbUsername.getBytes(StandardCharsets.UTF_8));
        TeradataPlugInConfiguration.setInputTeradataPassword(context, TeradataSplitByValueFunctionalityTest.dbPassword.getBytes(StandardCharsets.UTF_8));
        TeradataSchemaUtils.setupTeradataSourceTableSchema(configuration, TeradataSplitByValueFunctionalityTest.dbConnection);
        processor.inputPreProcessor(context);
        final String splitColumn = TeradataPlugInConfiguration.getInputSplitByColumn(configuration);
        Assert.assertTrue(splitColumn.toUpperCase().equals("NAME"));
    }

    @Test
    public void testValidateConfigurationNOPINumMappers1() throws Exception {
        final TeradataSplitByValueProcessor processor = new TeradataSplitByValueProcessor();
        final JobContext context = (JobContext) new Job();
        final Configuration configuration = context.getConfiguration();
        configuration.set("tdch.num.mappers", "1");
        TeradataPlugInConfiguration.setInputJdbcDriverClass(configuration, TeradataSplitByValueFunctionalityTest.dbJdbcDriverClass);
        TeradataPlugInConfiguration.setInputJdbcUrl(configuration, TeradataSplitByValueFunctionalityTest.dbJdbcUrl);
        TeradataPlugInConfiguration.setInputTable(configuration, TeradataSplitByValueFunctionalityTest.dbDatabase + ".TDCHIMPORTTEST");
        TeradataPlugInConfiguration.setInputTeradataUserName(context, TeradataSplitByValueFunctionalityTest.dbUsername.getBytes(StandardCharsets.UTF_8));
        TeradataPlugInConfiguration.setInputTeradataPassword(context, TeradataSplitByValueFunctionalityTest.dbPassword.getBytes(StandardCharsets.UTF_8));
        TeradataSchemaUtils.setupTeradataSourceTableSchema(configuration, TeradataSplitByValueFunctionalityTest.dbConnection);
        processor.inputPreProcessor(context);
        final String splitColumn = TeradataPlugInConfiguration.getInputSplitByColumn(configuration);
        Assert.assertTrue(splitColumn.toUpperCase().equals(""));
    }

    @Test
    public void testValidateConfigurationPINumMappers1() throws Exception {
        final TeradataSplitByValueProcessor processor = new TeradataSplitByValueProcessor();
        final JobContext context = (JobContext) new Job();
        final Configuration configuration = context.getConfiguration();
        configuration.set("tdch.num.mappers", "1");
        TeradataPlugInConfiguration.setInputJdbcDriverClass(configuration, TeradataSplitByValueFunctionalityTest.dbJdbcDriverClass);
        TeradataPlugInConfiguration.setInputJdbcUrl(configuration, TeradataSplitByValueFunctionalityTest.dbJdbcUrl);
        TeradataPlugInConfiguration.setInputTable(configuration, TeradataSplitByValueFunctionalityTest.dbDatabase + ".TDCHIMPORTTESTPI");
        TeradataPlugInConfiguration.setInputTeradataUserName(context, TeradataSplitByValueFunctionalityTest.dbUsername.getBytes(StandardCharsets.UTF_8));
        TeradataPlugInConfiguration.setInputTeradataPassword(context, TeradataSplitByValueFunctionalityTest.dbPassword.getBytes(StandardCharsets.UTF_8));
        TeradataSchemaUtils.setupTeradataSourceTableSchema(configuration, TeradataSplitByValueFunctionalityTest.dbConnection);
        processor.inputPreProcessor(context);
        final String splitColumn = TeradataPlugInConfiguration.getInputSplitByColumn(configuration);
        Assert.assertTrue(splitColumn.toUpperCase().equals(""));
    }

    @Test
    public void testValidateConfigurationPINumMappers2() throws Exception {
        final TeradataSplitByValueProcessor processor = new TeradataSplitByValueProcessor();
        final JobContext context = (JobContext) new Job();
        final Configuration configuration = context.getConfiguration();
        configuration.set("tdch.num.mappers", "2");
        TeradataPlugInConfiguration.setInputJdbcDriverClass(configuration, TeradataSplitByValueFunctionalityTest.dbJdbcDriverClass);
        TeradataPlugInConfiguration.setInputJdbcUrl(configuration, TeradataSplitByValueFunctionalityTest.dbJdbcUrl);
        TeradataPlugInConfiguration.setInputTable(configuration, TeradataSplitByValueFunctionalityTest.dbDatabase + ".TDCHIMPORTTESTPI");
        TeradataPlugInConfiguration.setInputTeradataUserName(context, TeradataSplitByValueFunctionalityTest.dbUsername.getBytes(StandardCharsets.UTF_8));
        TeradataPlugInConfiguration.setInputTeradataPassword(context, TeradataSplitByValueFunctionalityTest.dbPassword.getBytes(StandardCharsets.UTF_8));
        TeradataSchemaUtils.setupTeradataSourceTableSchema(configuration, TeradataSplitByValueFunctionalityTest.dbConnection);
        processor.inputPreProcessor(context);
        final String splitColumn = TeradataPlugInConfiguration.getInputSplitByColumn(configuration);
        Assert.assertTrue(splitColumn.toUpperCase().equals("NAME"));
    }

    @Test
    public void testValidateConfigurationNOPICol1NumMappers1() throws Exception {
        final TeradataSplitByValueProcessor processor = new TeradataSplitByValueProcessor();
        final JobContext context = (JobContext) new Job();
        final Configuration configuration = context.getConfiguration();
        configuration.set("tdch.num.mappers", "1");
        TeradataPlugInConfiguration.setInputJdbcDriverClass(configuration, TeradataSplitByValueFunctionalityTest.dbJdbcDriverClass);
        TeradataPlugInConfiguration.setInputJdbcUrl(configuration, TeradataSplitByValueFunctionalityTest.dbJdbcUrl);
        TeradataPlugInConfiguration.setInputTable(configuration, TeradataSplitByValueFunctionalityTest.dbDatabase + ".TDCHIMPORTTESTNOPI1");
        TeradataPlugInConfiguration.setInputTeradataUserName(context, TeradataSplitByValueFunctionalityTest.dbUsername.getBytes(StandardCharsets.UTF_8));
        TeradataPlugInConfiguration.setInputTeradataPassword(context, TeradataSplitByValueFunctionalityTest.dbPassword.getBytes(StandardCharsets.UTF_8));
        TeradataSchemaUtils.setupTeradataSourceTableSchema(configuration, TeradataSplitByValueFunctionalityTest.dbConnection);
        processor.inputPreProcessor(context);
        final String splitColumn = TeradataPlugInConfiguration.getInputSplitByColumn(configuration);
        Assert.assertTrue(splitColumn.toUpperCase().equals(""));
    }

    @Test
    public void testValidateConfigurationNOPICol1NumMappers2() throws Exception {
        final TeradataSplitByValueProcessor processor = new TeradataSplitByValueProcessor();
        final JobContext context = (JobContext) new Job();
        final Configuration configuration = context.getConfiguration();
        configuration.set("tdch.num.mappers", "2");
        TeradataPlugInConfiguration.setInputJdbcDriverClass(configuration, TeradataSplitByValueFunctionalityTest.dbJdbcDriverClass);
        TeradataPlugInConfiguration.setInputJdbcUrl(configuration, TeradataSplitByValueFunctionalityTest.dbJdbcUrl);
        TeradataPlugInConfiguration.setInputTable(configuration, TeradataSplitByValueFunctionalityTest.dbDatabase + ".TDCHIMPORTTESTNOPI1");
        TeradataPlugInConfiguration.setInputTeradataUserName(context, TeradataSplitByValueFunctionalityTest.dbUsername.getBytes(StandardCharsets.UTF_8));
        TeradataPlugInConfiguration.setInputTeradataPassword(context, TeradataSplitByValueFunctionalityTest.dbPassword.getBytes(StandardCharsets.UTF_8));
        TeradataSchemaUtils.setupTeradataSourceTableSchema(configuration, TeradataSplitByValueFunctionalityTest.dbConnection);
        processor.inputPreProcessor(context);
        final String splitColumn = TeradataPlugInConfiguration.getInputSplitByColumn(configuration);
        Assert.assertTrue(splitColumn.toUpperCase().equals("NAME"));
    }

    private static void createDatabaseTables() throws Exception {
        dropDatabaseTables();
        TeradataSplitByValueFunctionalityTest.dbConnection.executeDDL("CREATE MULTISET TABLE " + TeradataSplitByValueFunctionalityTest.dbDatabase + ".TDCHIMPORTTEST (NAME VARCHAR(10),ADDR VARCHAR(10)) NO PRIMARY INDEX;");
        TeradataSplitByValueFunctionalityTest.dbConnection.executeDDL("INSERT INTO " + TeradataSplitByValueFunctionalityTest.dbDatabase + ".TDCHIMPORTTEST VALUES ('jimmy','boston');");
        TeradataSplitByValueFunctionalityTest.dbConnection.executeDDL("INSERT INTO " + TeradataSplitByValueFunctionalityTest.dbDatabase + ".TDCHIMPORTTEST VALUES ('Tom','Cumberland');");
        TeradataSplitByValueFunctionalityTest.dbConnection.executeDDL("INSERT INTO " + TeradataSplitByValueFunctionalityTest.dbDatabase + ".TDCHIMPORTTEST VALUES ('jimmy','Cumberland');");
        TeradataSplitByValueFunctionalityTest.dbConnection.executeDDL("CREATE MULTISET TABLE " + TeradataSplitByValueFunctionalityTest.dbDatabase + ".TDCHIMPORTTESTPI (NAME VARCHAR(10),ADDR VARCHAR(10)) UNIQUE PRIMARY INDEX ( NAME );");
        TeradataSplitByValueFunctionalityTest.dbConnection.executeDDL("INSERT INTO " + TeradataSplitByValueFunctionalityTest.dbDatabase + ".TDCHIMPORTTESTPI VALUES ('jimmy','boston');");
        TeradataSplitByValueFunctionalityTest.dbConnection.executeDDL("INSERT INTO " + TeradataSplitByValueFunctionalityTest.dbDatabase + ".TDCHIMPORTTESTPI VALUES ('Tom','Cumberland');");
        TeradataSplitByValueFunctionalityTest.dbConnection.executeDDL("CREATE MULTISET TABLE " + TeradataSplitByValueFunctionalityTest.dbDatabase + ".TDCHIMPORTTESTNOPI1 (NAME VARCHAR(10)) NO PRIMARY INDEX;");
        TeradataSplitByValueFunctionalityTest.dbConnection.executeDDL("INSERT INTO " + TeradataSplitByValueFunctionalityTest.dbDatabase + ".TDCHIMPORTTESTNOPI1 VALUES ('jimmy');");
        TeradataSplitByValueFunctionalityTest.dbConnection.executeDDL("INSERT INTO " + TeradataSplitByValueFunctionalityTest.dbDatabase + ".TDCHIMPORTTESTNOPI1 VALUES ('Tom');");
        TeradataSplitByValueFunctionalityTest.dbConnection.executeDDL("INSERT INTO " + TeradataSplitByValueFunctionalityTest.dbDatabase + ".TDCHIMPORTTESTNOPI1 VALUES ('jimmy');");
    }

    private static void dropDatabaseTables() throws Exception {
        try {
            TeradataSplitByValueFunctionalityTest.dbConnection.dropTable(TeradataSplitByValueFunctionalityTest.dbDatabase + ".TDCHIMPORTTEST");
        } catch (SQLException ex) {
        }
        try {
            TeradataSplitByValueFunctionalityTest.dbConnection.dropTable(TeradataSplitByValueFunctionalityTest.dbDatabase + ".TDCHIMPORTTESTPI");
        } catch (SQLException ex2) {
        }
        try {
            TeradataSplitByValueFunctionalityTest.dbConnection.dropTable(TeradataSplitByValueFunctionalityTest.dbDatabase + ".TDCHIMPORTTESTNOPI1");
        } catch (SQLException ex3) {
        }
    }

    static {
        TeradataSplitByValueFunctionalityTest.logger = LogFactory.getLog((Class) TeradataInputProcessor.class);
    }
}
