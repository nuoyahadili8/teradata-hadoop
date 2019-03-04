package com.teradata.connector.teradata;

import com.teradata.connector.common.exception.ConnectorException;
import com.teradata.connector.common.exception.ConnectorException.ErrorCode;
import com.teradata.connector.common.utils.ConnectorConfiguration;
import com.teradata.connector.teradata.db.TeradataConnection;
import com.teradata.connector.teradata.processor.TeradataBatchInsertProcessor;
import com.teradata.connector.teradata.utils.LogContainer;
import com.teradata.connector.teradata.utils.TeradataPlugInConfiguration;
import com.teradata.connector.teradata.utils.TeradataSchemaUtils;
import com.teradata.connector.teradata.utils.TeradataUtils;

import java.io.IOException;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.Arrays;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.OutputCommitter;
import org.apache.hadoop.mapreduce.OutputFormat;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.db.DBWritable;
import org.apache.hadoop.mapreduce.lib.output.FileOutputCommitter;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;


public class TeradataBatchInsertOutputFormat<K, V> extends OutputFormat<K, DBWritable> {
    private static Log logger = LogFactory.getLog(TeradataBatchInsertOutputFormat.class);
    protected TeradataConnection connection;

    public class TeradataRecordWriter<K, V> extends RecordWriter<K, DBWritable> {
        protected int batchCount;
        protected int batchSize;
        protected Connection connection;
        protected long end_timestamp;
        protected PreparedStatement preparedStatement;
        protected long start_timestamp;

        public TeradataRecordWriter() {
            this.connection = null;
            this.preparedStatement = null;
            this.batchSize = 0;
            this.batchCount = 0;
            this.end_timestamp = 0;
            this.start_timestamp = 0;
            this.start_timestamp = System.currentTimeMillis();
            TeradataBatchInsertOutputFormat.logger.info("recordwriter class " + getClass().getName() + "initialize time is:  " + this.start_timestamp);
        }

        public TeradataRecordWriter(TeradataBatchInsertOutputFormat this$0, Connection connection, PreparedStatement preparedStatement, int batchSize) {
            this();
            this.connection = connection;
            this.preparedStatement = preparedStatement;
            this.batchSize = batchSize;
        }

        @Override
        public void write(K k, DBWritable value) throws IOException {
            try {
                value.write(this.preparedStatement);
                this.preparedStatement.addBatch();
                this.batchCount++;
            } catch (NullPointerException | SQLException e) {
            }
            try {
                if (this.batchCount >= this.batchSize) {
                    this.preparedStatement.executeBatch();
                    this.batchCount = 0;
                }
            } catch (SQLException e2) {
                SQLException e3 = e2;
                ConnectorException start = new ConnectorException(e3.getMessage(), (Throwable) e3);
                while (e3 != null) {
                    StackTraceElement[] elements = e3.getStackTrace();
                    int n = elements.length;
                    for (int i = 0; i < n; i++) {
                        TeradataBatchInsertOutputFormat.logger.error(elements[i].getFileName() + ":" + elements[i].getLineNumber() + ">> " + elements[i].getMethodName() + "()");
                    }
                    e3 = e3.getNextException();
                }
                TeradataUtils.closeConnection(this.connection);
                throw start;
            }
        }

        @Override
        public void close(TaskAttemptContext context) throws IOException {
            try {
                if (this.batchCount > 0) {
                    this.preparedStatement.executeBatch();
                }
                this.connection.commit();
                this.end_timestamp = System.currentTimeMillis();
                TeradataBatchInsertOutputFormat.logger.info("recordwriter class " + getClass().getName() + "close time is:  " + this.end_timestamp);
                TeradataBatchInsertOutputFormat.logger.info("the total elapsed time of recordwriter " + getClass().getName() + ((this.end_timestamp - this.start_timestamp) / 1000) + "s");
                TeradataUtils.closeConnection(this.connection);
                if (ConnectorConfiguration.getEnableHdfsLoggingFlag(context.getConfiguration()) && LogContainer.getInstance().listSize() > 0) {
                    LogContainer.getInstance().writeHdfsLogs(context.getConfiguration());
                }
            } catch (SQLException e) {
                SQLException e2 = e;
                ConnectorException start = new ConnectorException(e2.getMessage(), (Throwable) e2);
                while (e2 != null) {
                    StackTraceElement[] elements = e2.getStackTrace();
                    int n = elements.length;
                    for (int i = 0; i < n; i++) {
                        TeradataBatchInsertOutputFormat.logger.error(elements[i].getFileName() + ":" + elements[i].getLineNumber() + ">> " + elements[i].getMethodName() + "()");
                    }
                    e2 = e2.getNextException();
                }
                throw start;
            } catch (Throwable th) {
                TeradataUtils.closeConnection(this.connection);
            }
        }
    }

    /* Access modifiers changed, original: protected */
    public void validateConfiguration(JobContext context) throws ConnectorException {
        Configuration configuration = context.getConfiguration();
        this.connection = TeradataUtils.openOutputConnection(context);
        if (ConnectorConfiguration.getPlugInOutputProcessor(configuration).isEmpty()) {
            TeradataUtils.validateOutputTeradataProperties(configuration, this.connection);
            if (TeradataPlugInConfiguration.getOutputBatchSize(configuration) > TeradataPlugInConfiguration.BATCH_SIZE_MAX) {
                TeradataPlugInConfiguration.setOutputBatchSize(configuration, TeradataPlugInConfiguration.BATCH_SIZE_MAX);
            }
            TeradataSchemaUtils.setupTeradataTargetTableSchema(configuration, this.connection);
        }
    }

    @Override
    public void checkOutputSpecs(JobContext context) throws IOException, InterruptedException {
        if (ConnectorConfiguration.getPlugInOutputProcessor(context.getConfiguration()).isEmpty()) {
            validateConfiguration(context);
        }
    }

    @Override
    public OutputCommitter getOutputCommitter(TaskAttemptContext context) throws IOException, InterruptedException {
        return new FileOutputCommitter(FileOutputFormat.getOutputPath(context), context);
    }

    @Override
    public RecordWriter<K, DBWritable> getRecordWriter(TaskAttemptContext context) throws IOException, InterruptedException {
        boolean stageEnabled = TeradataPlugInConfiguration.getOutputStageEnabled(context.getConfiguration());
        boolean disableFailover = TeradataPlugInConfiguration.getOutputBIDisableFailoverSupport(context.getConfiguration());
        String taskID = context.getTaskAttemptID().getTaskID().toString();
        String[] outputTableFieldNames = TeradataPlugInConfiguration.getOutputFieldNamesArray(context.getConfiguration());
        Configuration configuration = context.getConfiguration();
        String insertStmt = "";
        try {
            if (context.getTaskAttemptID().getId() <= 0 || (stageEnabled && !disableFailover)) {
                String outputDatabase = TeradataPlugInConfiguration.getOutputDatabase(configuration);
                String outputTableName = TeradataPlugInConfiguration.getOutputTable(configuration);
                outputTableName = TeradataConnection.getQuotedEscapedName(outputDatabase, outputTableName);
                this.connection = TeradataUtils.openOutputConnection(context);
                if (context.getTaskAttemptID().getId() > 0 && stageEnabled && !disableFailover) {
                    long numRows = this.connection.getTableRowCount(outputTableName, "TDCH_BI_TASKID = '" + taskID + "'");
                    if (numRows != 0) {
                        logger.info("Task ID " + taskID + " was restarted, deleting " + numRows + " records loaded by failed task from stage table");
                        this.connection.deleteTable(outputTableName, "TDCH_BI_TASKID = '" + taskID + "'");
                    }
                }
                if (!stageEnabled || disableFailover) {
                    insertStmt = TeradataConnection.getInsertPreparedStatmentSQL(outputTableName, outputTableFieldNames);
                } else {
                    outputTableFieldNames = (String[]) Arrays.copyOf(outputTableFieldNames, outputTableFieldNames.length + 1);
                    outputTableFieldNames[outputTableFieldNames.length - 1] = TeradataBatchInsertProcessor.taskIDColumnName;
                    insertStmt = TeradataConnection.getInsertPreparedStatmentSQLWithTaskID(outputTableName, outputTableFieldNames, taskID);
                }
                logger.info(insertStmt);
                int batchSize = TeradataPlugInConfiguration.getOutputBatchSize(configuration);
                Connection jdbcConnection = this.connection.getConnection();
                return new TeradataRecordWriter(this, jdbcConnection, jdbcConnection.prepareStatement(insertStmt), batchSize);
            }
            throw new ConnectorException((int) ErrorCode.BATCH_INSERT_FAILED_TASK_RESTART);
        } catch (SQLException e) {
            throw new ConnectorException(e.getMessage(), e);
        } catch (ConnectorException e2) {
            throw new ConnectorException(e2.getMessage(), e2);
        }
    }
}
