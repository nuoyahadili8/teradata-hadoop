package com.teradata.connector.idatastream;

import com.teradata.connector.common.exception.ConnectorException;
import com.teradata.connector.common.exception.ConnectorException.ErrorCode;
import com.teradata.connector.idatastream.utils.IDataStreamPlugInConfiguration;
import java.io.IOException;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.InputFormat;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.OutputCommitter;
import org.apache.hadoop.mapreduce.OutputFormat;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.output.FileOutputCommitter;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.ReflectionUtils;


public class IDataStreamOutputFormat<K, V extends IDataStreamByteArray> extends OutputFormat<K, IDataStreamByteArray> {
    @Override
    public void checkOutputSpecs(final JobContext arg0) throws IOException, InterruptedException {
    }

    @Override
    public OutputCommitter getOutputCommitter(final TaskAttemptContext context) throws IOException, InterruptedException {
        return (OutputCommitter) new FileOutputCommitter(FileOutputFormat.getOutputPath((JobContext) context), context);
    }

    public RecordWriter<K, IDataStreamByteArray> getRecordWriter(final TaskAttemptContext context) throws IOException, InterruptedException {
        if (context.getTaskAttemptID().getId() > 0) {
            throw new ConnectorException(60006);
        }
        return new IDataStreamRecordWriter((JobContext) context);
    }

    public class IDataStreamRecordWriter extends RecordWriter<K, IDataStreamByteArray> {
        private Log logger;
        private static final int batchSize = 75000;
        private int batchCount;
        private long end_timestamp;
        private long start_timestamp;
        private IDataStreamConnection connection;

        public IDataStreamRecordWriter(final JobContext context) throws ConnectorException {
            this.logger = LogFactory.getLog((Class) IDataStreamRecordWriter.class);
            this.batchCount = 0;
            this.end_timestamp = 0L;
            this.start_timestamp = 0L;
            this.connection = null;
            this.start_timestamp = System.currentTimeMillis();
            this.logger.info((Object) ("IDataStreamRecordWriter starts at " + this.start_timestamp));
            final Configuration configuration = context.getConfiguration();
            int nummappers = 0;
            try {
                nummappers = ((InputFormat) ReflectionUtils.newInstance(context.getInputFormatClass(), configuration)).getSplits(context).size();
            } catch (Exception e) {
                throw new ConnectorException(e.getMessage(), e);
            }
            final byte[] ba = {(byte) (nummappers >> 8 & 0xFF), (byte) (nummappers & 0xFF)};
            try {
                final String host = IDataStreamPlugInConfiguration.getOutputSocketHost(configuration);
                final int port = Integer.parseInt(IDataStreamPlugInConfiguration.getOutputSocketPort(configuration));
                (this.connection = new IDataStreamConnection(host, port)).connect();
                this.connection.getOutputStream().write(ba);
            } catch (Exception e2) {
                this.connection = null;
                throw new ConnectorException(e2.getMessage(), e2);
            }
        }

        public void close(final TaskAttemptContext arg0) throws IOException, InterruptedException {
            try {
                if (this.batchCount > 0) {
                    this.connection.getOutputStream().flush();
                }
                this.connection.getOutputStream().write(new String("EOD").getBytes());
                this.connection.getOutputStream().flush();
                this.connection.disconnect();
                this.end_timestamp = System.currentTimeMillis();
                this.logger.info((Object) ("IDataStreamRecordWriter ends at " + this.end_timestamp));
                this.logger.info((Object) ("IDataStreamRecordWriter time is " + (this.end_timestamp - this.start_timestamp) / 1000L + "s"));
            } catch (Exception e) {
                throw new ConnectorException(e.getMessage(), e);
            } finally {
                try {
                    if (this.connection != null && !this.connection.isClosed()) {
                        this.connection.disconnect();
                    }
                } catch (Exception ex) {
                }
                this.connection = null;
            }
        }

        public void write(final K key, final IDataStreamByteArray barray) throws IOException, InterruptedException {
            this.connection.getOutputStream().write(barray.getByteArray());
            this.batchCount += barray.datalen;
            if (this.batchCount > 75000) {
                this.connection.getOutputStream().flush();
                this.batchCount = 0;
            }
        }
    }
}
