package com.teradata.connector.sample;

import com.teradata.connector.common.ConnectorRecord;
import com.teradata.connector.common.exception.ConnectorException;
import com.teradata.connector.common.exception.ConnectorException.ErrorCode;
import com.teradata.connector.common.utils.ConnectorConfiguration;
import com.teradata.connector.common.utils.HadoopConfigurationUtils;
import com.teradata.connector.sample.plugin.utils.CommonDBConfiguration;
import com.teradata.connector.sample.plugin.utils.CommonDBSchemaUtils;
import com.teradata.connector.sample.plugin.utils.CommonDBSplitUtils;
import com.teradata.connector.sample.plugin.utils.CommonDBUtils;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;

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


public abstract class CommonDBInputFormat extends InputFormat<LongWritable, ConnectorRecord> {
    protected static Log logger = LogFactory.getLog((Class) CommonDBInputFormat.class);
    protected static final int SPLIT_LOCATIONS_MAX = 6;

    public abstract String getSplitRangeSQL(final Configuration p0);

    public abstract String getMinMaxSQL(final Configuration p0);

    public abstract String getOneAmpSQL(final Configuration p0);

    public abstract void validataConfiguration(final Configuration p0) throws ConnectorException;

    public abstract Connection getConnection(final Configuration p0) throws ConnectorException;

    public abstract String getSplitColumn(final Configuration p0);

    @Override
    public List<InputSplit> getSplits(final JobContext context) throws IOException, InterruptedException {
        final Configuration configuration = context.getConfiguration();
        this.validataConfiguration(configuration);
        final String[] locations = HadoopConfigurationUtils.getAllActiveHosts(context);
        final int numMappers = ConnectorConfiguration.getNumMappers(configuration);
        final List<InputSplit> splits = new ArrayList<InputSplit>();
        final String splitSql = this.getSplitRangeSQL(configuration);
        CommonDBConfiguration.setInputSplitSql(configuration, splitSql);
        ResultSet resultSetMinMaxValues = null;
        Statement statement = null;
        Connection sqlConnection = null;
        if (numMappers == 1) {
            final CommonDBInputSplit split = new CommonDBInputSplit(this.getOneAmpSQL(configuration));
            split.setLocations(HadoopConfigurationUtils.selectUniqueActiveHosts(locations, 6));
            splits.add(split);
        } else {
            try {
                sqlConnection = this.getConnection(configuration);
                if (sqlConnection != null) {
                    final String sql = this.getMinMaxSQL(configuration);
                    CommonDBInputFormat.logger.info((Object) sql);
                    statement = sqlConnection.createStatement();
                    resultSetMinMaxValues = statement.executeQuery(sql);
                }
                if (resultSetMinMaxValues == null || !resultSetMinMaxValues.next()) {
                    throw new ConnectorException(23003);
                }
                if (resultSetMinMaxValues.getString(1) == null || resultSetMinMaxValues.getString(2) == null) {
                    throw new ConnectorException(23004);
                }
            } catch (SQLException e) {
                CommonDBUtils.CloseConnection(sqlConnection);
                throw new ConnectorException(e.getMessage(), e);
            }
            final String splitColumnName = this.getSplitColumn(configuration);
            final List<CommonDBInputSplit> columnSplits = CommonDBSplitUtils.getSplitsByColumnType(configuration, splitColumnName, resultSetMinMaxValues);
            for (int i = 0; i < columnSplits.size(); ++i) {
                final CommonDBInputSplit split2 = columnSplits.get(i);
                split2.setLocations(HadoopConfigurationUtils.selectUniqueActiveHosts(locations, 6));
                splits.add(split2);
            }
        }
        try {
            if (resultSetMinMaxValues != null) {
                resultSetMinMaxValues.close();
                resultSetMinMaxValues = null;
            }
            if (statement != null) {
                statement.close();
                statement = null;
            }
        } catch (SQLException e) {
            throw new ConnectorException(e.getMessage(), e);
        } finally {
            CommonDBUtils.CloseConnection(sqlConnection);
        }
        return splits;
    }

    public RecordReader<LongWritable, ConnectorRecord> createRecordReader(final InputSplit split, final TaskAttemptContext context) throws IOException, InterruptedException {
        return new CommonDBRecordReader(((CommonDBInputSplit) split).getSplitSql());
    }

    public static class CommonDBInputSplit extends InputSplit implements Writable {
        private String[] locations;
        private String splitSql;

        public CommonDBInputSplit() {
            this.locations = null;
            this.splitSql = "";
        }

        public CommonDBInputSplit(final String sql) {
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

    public class CommonDBRecordReader extends RecordReader<LongWritable, ConnectorRecord> {
        private Log logger;
        private Connection connection;
        private PreparedStatement preparedStatement;
        private long resultCount;
        private CommonDBObjectArrayWritable curValue;
        private ResultSet resultset;
        private String splitSQL;
        private long end_timestamp;
        private long start_timestamp;
        private String[] sourceFields;
        private int[] sourceFieldMapping;

        public CommonDBRecordReader(final String splitSQL) {
            this.logger = LogFactory.getLog((Class) CommonDBRecordReader.class);
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
                    this.connection = CommonDBInputFormat.this.getConnection(configuration);
                }
                if (this.connection != null) {
                    this.logger.info((Object) ("split sql is: " + this.splitSQL));
                    (this.preparedStatement = this.connection.prepareStatement(this.splitSQL)).setFetchSize(CommonDBConfiguration.getInputBatchSize(configuration));
                }
                final String sourceTableDescText = CommonDBConfiguration.getInputTableDesc(configuration);
                this.sourceFields = CommonDBConfiguration.getInputFieldNamesArray(configuration);
                this.sourceFieldMapping = CommonDBSchemaUtils.lookupMappingFromTableDescText(sourceTableDescText, this.sourceFields);
                this.curValue = new CommonDBObjectArrayWritable(this.sourceFieldMapping.length);
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
                CommonDBUtils.CloseConnection(this.connection);
            }
        }
    }
}
