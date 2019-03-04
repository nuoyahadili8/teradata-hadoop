package com.teradata.connector.teradata;

import com.teradata.connector.common.ConnectorRecord;
import com.teradata.connector.common.exception.ConnectorException;
import com.teradata.connector.common.utils.ConnectorConfiguration;
import com.teradata.connector.teradata.db.TeradataConnection;
import com.teradata.connector.teradata.utils.TeradataPlugInConfiguration;
import com.teradata.connector.teradata.utils.TeradataSchemaUtils;
import com.teradata.connector.teradata.utils.TeradataUtils;
import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.InputFormat;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;


public abstract class TeradataInputFormat extends InputFormat<LongWritable, ConnectorRecord> {
    protected static final int SPLIT_LOCATIONS_MAX = 6;
    protected TeradataConnection connection;
    protected String inputTableName;
    protected String inputConditions;
    protected String[] inputFieldNamesArray;

    public TeradataInputFormat() {
        this.connection = null;
        this.inputTableName = null;
        this.inputConditions = null;
        this.inputFieldNamesArray = null;
    }

    public void validateConfiguration(final JobContext context) throws ConnectorException {
        final Configuration configuration = context.getConfiguration();
        this.connection = TeradataUtils.openInputConnection(context);
        final String inputProcessor = ConnectorConfiguration.getPlugInInputProcessor(configuration);
        if (inputProcessor.isEmpty()) {
            TeradataUtils.validateInputTeradataProperties(configuration, this.connection);
            TeradataSchemaUtils.setupTeradataSourceTableSchema(configuration, this.connection);
        }
    }

    @Override
    public RecordReader<LongWritable, ConnectorRecord> createRecordReader(final InputSplit split, final TaskAttemptContext context) throws IOException, InterruptedException {
        final String splitSql = ((TeradataInputSplit) split).getSplitSql();
        return new TeradataRecordReader(splitSql);
    }

    public class TeradataRecordReader extends RecordReader<LongWritable, ConnectorRecord> {
        private Log logger;
        private Connection connection;
        private PreparedStatement preparedStatement;
        private long resultCount;
        private TeradataObjectArrayWritable curValue;
        private ResultSet resultset;
        private String splitSQL;
        private long end_timestamp;
        private long start_timestamp;
        private String[] sourceFields;
        private int[] sourceFieldMapping;

        public TeradataRecordReader(final String splitSQL) {
            this.logger = LogFactory.getLog((Class) TeradataRecordReader.class);
            this.connection = null;
            this.preparedStatement = null;
            this.resultCount = 0L;
            this.curValue = null;
            this.resultset = null;
            this.end_timestamp = 0L;
            this.start_timestamp = 0L;
            this.sourceFields = null;
            this.sourceFieldMapping = null;
            this.splitSQL = splitSQL;
            this.start_timestamp = System.currentTimeMillis();
            this.logger.info((Object) ("recordreader class " + this.getClass().getName() + "initialize time is:  " + this.start_timestamp));
        }

        @Override
        public void initialize(final InputSplit split, final TaskAttemptContext context) throws IOException, InterruptedException {
            final Configuration configuration = context.getConfiguration();
            try {
                if (this.connection == null) {
                    this.connection = TeradataUtils.openInputConnection((JobContext) context).getConnection();
                }
                if (this.connection != null) {
                    this.logger.debug((Object) ("split sql is: " + this.splitSQL));
                    (this.preparedStatement = this.connection.prepareStatement(this.splitSQL)).setFetchSize(TeradataPlugInConfiguration.getInputBatchSize(configuration));
                }
                final String sourceTableDescText = TeradataPlugInConfiguration.getInputTableDesc(configuration);
                this.sourceFields = TeradataPlugInConfiguration.getInputFieldNamesArray(configuration);
                this.sourceFieldMapping = TeradataSchemaUtils.lookupMappingFromTableDescText(sourceTableDescText, this.sourceFields);
                this.curValue = new TeradataObjectArrayWritable(this.sourceFieldMapping.length);
                final String[] typeNames = TeradataSchemaUtils.lookupTypeNamesFromTableDescText(sourceTableDescText, this.sourceFields);
                this.curValue.setRecordTypes(typeNames);
            } catch (SQLException e) {
                try {
                    if (this.connection != null) {
                        this.connection.close();
                    }
                } catch (SQLException ex) {
                }
                this.connection = null;
                throw new ConnectorException(e.getMessage(), e);
            }
        }

        @Override
        public LongWritable getCurrentKey() throws IOException, InterruptedException {
            return new LongWritable(this.resultCount);
        }

        @Override
        public ConnectorRecord getCurrentValue() throws IOException, InterruptedException {
            return this.curValue;
        }

        @Override
        public float getProgress() throws IOException, InterruptedException {
            return 0.0f;
        }

        @Override
        public boolean nextKeyValue() throws IOException, InterruptedException {
            try {
                if (this.resultset == null) {
                    this.resultset = this.preparedStatement.executeQuery();
                }
                if (!this.resultset.next()) {
                    return false;
                }
                this.curValue.readFields(this.resultset);
                ++this.resultCount;
            } catch (SQLException e) {
                throw new ConnectorException(e.getMessage(), e);
            }
            return true;
        }

        @Override
        public void close() throws IOException {
            try {
                if (this.preparedStatement != null) {
                    this.preparedStatement.close();
                    this.preparedStatement = null;
                }
                this.end_timestamp = System.currentTimeMillis();
                this.logger.info((Object) ("recordreader class " + this.getClass().getName() + "close time is:  " + this.end_timestamp));
                this.logger.info((Object) ("the total elapsed time of recordreader " + this.getClass().getName() + (this.end_timestamp - this.start_timestamp) / 1000L + "s"));
            } catch (SQLException e) {
                throw new ConnectorException(e.getMessage(), e);
            } finally {
                TeradataUtils.closeConnection(this.connection);
            }
        }
    }

    public static class TeradataInputSplit extends InputSplit implements Writable {
        private String[] locations;
        private String splitSql;

        public TeradataInputSplit() {
            this.locations = null;
            this.splitSql = "";
        }

        public TeradataInputSplit(final String sql) {
            this.locations = null;
            this.splitSql = "";
            this.splitSql = sql;
        }

        public void setLocations(final String[] locations) {
            this.locations = locations;
        }

        @Override
        public String[] getLocations() throws IOException, InterruptedException {
            return (this.locations == null) ? new String[0] : this.locations;
        }

        @Override
        public long getLength() throws IOException, InterruptedException {
            return 1L;
        }

        @Override
        public void readFields(final DataInput input) throws IOException {
            this.splitSql = Text.readString(input);
        }

        @Override
        public void write(final DataOutput output) throws IOException {
            Text.writeString(output, this.splitSql);
        }

        public String getSplitSql() {
            return this.splitSql;
        }

        public void setSplitSql(final String sql) {
            this.splitSql = sql;
        }
    }
}
