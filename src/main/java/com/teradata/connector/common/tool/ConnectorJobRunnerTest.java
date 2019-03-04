package com.teradata.connector.common.tool;

import com.teradata.connector.common.exception.ConnectorException;
import com.teradata.connector.common.utils.ConnectorConfiguration;
import com.teradata.connector.common.utils.StandardCharsets;
import com.teradata.connector.teradata.utils.TeradataPlugInConfiguration;
import com.teradata.connector.test.group.SlowTests;
import java.io.IOException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.JobContext;
import org.junit.Assert;
import org.junit.Test;
import org.junit.experimental.categories.Category;


@Category({SlowTests.class})
public class ConnectorJobRunnerTest {
    public static final String VALUE_TERADATA_INPUT_SPLIT_BY_AMP_PROCESSOR = "com.teradata.connector.teradata.processor.TeradataSplitByAmpProcessor";
    public static final String VALUE_TERADATA_SPLIT_BY_AMP_INPUT_FORMAT = "com.teradata.connector.teradata.TeradataSplitByAmpInputFormat";
    public static final String VALUE_TERADATA_SERDE = "com.teradata.connector.teradata.serde.TeradataSerDe";
    public static final String VALUE_TERADATA_OUTPUT_BATCH_INSERT_PROCESSOR = "com.teradata.connector.teradata.processor.TeradataBatchInsertProcessor";
    public static final String VALUE_TERADATA_BATCH_INSERT_OUTPUT_FORMAT = "com.teradata.connector.teradata.TeradataBatchInsertOutputFormat";
    public static final String VALUE_TERADATA_CONVERTER = "com.teradata.connector.teradata.converter.TeradataConverter";
    final String dbs;
    final String databasename;
    final String user;
    final String password;

    public ConnectorJobRunnerTest() {
        this.dbs = System.getProperty("dbs");
        this.databasename = System.getProperty("databasename");
        this.user = System.getProperty("user");
        this.password = System.getProperty("password");
    }

    @Test
    public void testRunJob() {
        Job job;
        try {
            job = new Job();
        } catch (IOException e) {
            e.printStackTrace();
            return;
        }
        final Configuration configuration = job.getConfiguration();
        TeradataPlugInConfiguration.setInputJdbcDriverClass(configuration, "com.teradata.jdbc.TeraDriver");
        TeradataPlugInConfiguration.setInputJdbcUrl(configuration, "jdbc:teradata://" + this.dbs + "/database=" + this.databasename);
        TeradataPlugInConfiguration.setInputTeradataUserName((JobContext) job, this.user.getBytes(StandardCharsets.UTF_8));
        TeradataPlugInConfiguration.setInputTeradataPassword((JobContext) job, this.password.getBytes(StandardCharsets.UTF_8));
        TeradataPlugInConfiguration.setOutputJdbcDriverClass(configuration, "com.teradata.jdbc.TeraDriver");
        TeradataPlugInConfiguration.setOutputJdbcUrl(configuration, "jdbc:teradata://" + this.dbs + "/database=" + this.databasename);
        TeradataPlugInConfiguration.setOutputTeradataUserName((JobContext) job, this.user.getBytes(StandardCharsets.UTF_8));
        TeradataPlugInConfiguration.setOutputTeradataPassword((JobContext) job, this.password.getBytes(StandardCharsets.UTF_8));
        TeradataPlugInConfiguration.setInputTable(configuration, "import_case");
        TeradataPlugInConfiguration.setInputDatabase(configuration, "tdch_tests");
        TeradataPlugInConfiguration.setOutputTable(configuration, "export_case");
        TeradataPlugInConfiguration.setOutputDatabase(configuration, "tdch_tests");
        ConnectorConfiguration.setUseCombinedInputFormat(configuration, false);
        configuration.set("tdch.plugin.input.processor", "com.teradata.connector.teradata.processor.TeradataSplitByAmpProcessor");
        configuration.set("tdch.plugin.input.format", "com.teradata.connector.teradata.TeradataSplitByAmpInputFormat");
        ConnectorConfiguration.setInputSerDe(configuration, "com.teradata.connector.teradata.serde.TeradataSerDe");
        configuration.set("tdch.plugin.output.processor", "com.teradata.connector.teradata.processor.TeradataBatchInsertProcessor");
        configuration.set("tdch.plugin.output.format", "com.teradata.connector.teradata.TeradataBatchInsertOutputFormat");
        ConnectorConfiguration.setOutputSerDe(configuration, "com.teradata.connector.teradata.serde.TeradataSerDe");
        ConnectorConfiguration.setDataConverter(configuration, "com.teradata.connector.teradata.converter.TeradataConverter");
        try {
            final int result = ConnectorJobRunner.runJob(job);
            Assert.assertEquals(result, 0);
        } catch (ConnectorException e2) {
            e2.printStackTrace();
        }
    }
}
