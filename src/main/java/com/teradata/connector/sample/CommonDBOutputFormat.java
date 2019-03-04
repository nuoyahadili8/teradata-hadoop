package com.teradata.connector.sample;

import com.teradata.connector.common.exception.ConnectorException;
import com.teradata.connector.common.exception.ConnectorException.ErrorCode;
import com.teradata.connector.sample.plugin.utils.CommonDBConfiguration;
import com.teradata.connector.sample.plugin.utils.CommonDBUtils;
import java.io.IOException;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
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

public abstract class CommonDBOutputFormat<K, V> extends OutputFormat<K, DBWritable> {
    private static Log logger = LogFactory.getLog(CommonDBOutputFormat.class);

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
            CommonDBOutputFormat.logger.info("recordwriter class " + getClass().getName() + "initialize time is:  " + this.start_timestamp);
        }

        public TeradataRecordWriter(CommonDBOutputFormat this$0, Connection connection, PreparedStatement preparedStatement, int batchSize) {
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
                if (this.batchCount >= this.batchSize) {
                    this.preparedStatement.executeBatch();
                    this.batchCount = 0;
                }
            } catch (SQLException e) {
                SQLException e2 = e;
                ConnectorException start = new ConnectorException(e2.getMessage(), (Throwable) e2);
                while (e2 != null) {
                    StackTraceElement[] elements = e2.getStackTrace();
                    int n = elements.length;
                    for (int i = 0; i < n; i++) {
                        CommonDBOutputFormat.logger.error(elements[i].getFileName() + ":" + elements[i].getLineNumber() + ">> " + elements[i].getMethodName() + "()");
                    }
                    e2 = e2.getNextException();
                }
                CommonDBUtils.CloseConnection(this.connection);
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
                CommonDBOutputFormat.logger.info("recordwriter class " + getClass().getName() + "close time is:  " + this.end_timestamp);
                CommonDBOutputFormat.logger.info("the total elapsed time of recordwriter " + getClass().getName() + ((this.end_timestamp - this.start_timestamp) / 1000) + "s");
                CommonDBUtils.CloseConnection(this.connection);
            } catch (SQLException e) {
                SQLException e2 = e;
                ConnectorException start = new ConnectorException(e2.getMessage(), (Throwable) e2);
                while (e2 != null) {
                    StackTraceElement[] elements = e2.getStackTrace();
                    int n = elements.length;
                    for (int i = 0; i < n; i++) {
                        CommonDBOutputFormat.logger.error(elements[i].getFileName() + ":" + elements[i].getLineNumber() + ">> " + elements[i].getMethodName() + "()");
                    }
                    e2 = e2.getNextException();
                }
                throw start;
            } catch (Throwable th) {
                CommonDBUtils.CloseConnection(this.connection);
            }
        }
    }

    public abstract Connection getConnection(Configuration configuration) throws ConnectorException;

    public abstract String getInsertPreparedStatmentSQL(Configuration configuration);

    @Override
    public void checkOutputSpecs(JobContext context) throws IOException, InterruptedException {
    }

    @Override
    public OutputCommitter getOutputCommitter(TaskAttemptContext context) throws IOException, InterruptedException {
        return new FileOutputCommitter(FileOutputFormat.getOutputPath(context), context);
    }

    @Override
    public RecordWriter<K, DBWritable> getRecordWriter(TaskAttemptContext context) throws IOException, InterruptedException {
        Configuration configuration = context.getConfiguration();
        String insertStmt = "";
        try {
            if (context.getTaskAttemptID().getId() > 0) {
                throw new ConnectorException((int) ErrorCode.BATCH_INSERT_FAILED);
            }
            Connection connection = getConnection(configuration);
            connection.setAutoCommit(false);
            insertStmt = getInsertPreparedStatmentSQL(configuration);
            logger.info(insertStmt);
            return new TeradataRecordWriter(this, connection, connection.prepareStatement(insertStmt), CommonDBConfiguration.getOutputBatchSize(configuration));
        } catch (SQLException e) {
            throw new ConnectorException(e.getMessage(), e);
        } catch (ConnectorException e2) {
            throw new ConnectorException(e2.getMessage(), e2);
        }
    }
}
