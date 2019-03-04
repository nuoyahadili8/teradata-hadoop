package com.teradata.connector.common;

import com.teradata.connector.common.exception.ConnectorException;
import com.teradata.connector.common.utils.ConnectorConfiguration;

import java.io.IOException;
import java.util.List;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.InputFormat;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;


/**
 * @author Administrator
 */
public class ConnectorInputFormat<K, V> extends InputFormat<K, ConnectorRecord> {
    InputFormat<K, Writable> plugedInInputFormat;

    public ConnectorInputFormat() {
        this.plugedInInputFormat = null;
    }

    @Override
    public List<InputSplit> getSplits(final JobContext context) throws IOException, InterruptedException {
        if (this.plugedInInputFormat == null) {
            this.configurePlugedInInuputFormat(context);
        }
        return (List<InputSplit>) this.plugedInInputFormat.getSplits(context);
    }

    @Override
    public RecordReader<K, ConnectorRecord> createRecordReader(final InputSplit split, final TaskAttemptContext context) throws IOException, InterruptedException {
        return new ConnectorRecordReader();
    }

    protected Path[] filterPath(final Path[] paths) {
        return null;
    }

    protected void configurePlugedInInuputFormat(final JobContext context) throws ConnectorException {
        final Configuration configuration = context.getConfiguration();
        try {
            this.plugedInInputFormat = (InputFormat<K, Writable>) Class.forName(ConnectorConfiguration.getPlugInInputFormat(configuration)).newInstance();
        } catch (InstantiationException e) {
            throw new ConnectorException(e.getMessage(), e);
        } catch (IllegalAccessException e2) {
            throw new ConnectorException(e2.getMessage(), e2);
        } catch (ClassNotFoundException e3) {
            throw new ConnectorException(e3.getMessage(), e3);
        }
    }

    public class ConnectorRecordReader extends RecordReader<K, ConnectorRecord> {
        private Log logger;
        protected ConnectorSerDe sourceSerDe;
        protected RecordReader<K, Writable> plugedInRecordReader;
        protected ConnectorRecord connectorRecord;
        protected Configuration configuration;
        private long end_timestamp;
        private long start_timestamp;

        public ConnectorRecordReader() {
            this.logger = LogFactory.getLog((Class) ConnectorRecordReader.class);
            this.end_timestamp = 0L;
            this.start_timestamp = 0L;
        }

        @Override
        public void initialize(final InputSplit split, final TaskAttemptContext context) throws IOException, InterruptedException {
            this.start_timestamp = System.currentTimeMillis();
            this.logger.info((Object) ("recordreader class " + this.getClass().getName() + "initialize time is:  " + this.start_timestamp));
            if (ConnectorInputFormat.this.plugedInInputFormat == null) {
                ConnectorInputFormat.this.configurePlugedInInuputFormat((JobContext) context);
            }
            this.configuration = context.getConfiguration();
            try {
                (this.sourceSerDe = (ConnectorSerDe) Class.forName(ConnectorConfiguration.getInputSerDe(this.configuration)).newInstance()).initialize((JobContext) context, ConnectorConfiguration.direction.input);
            } catch (InstantiationException e) {
                throw new ConnectorException(e.getMessage(), e);
            } catch (IllegalAccessException e2) {
                throw new ConnectorException(e2.getMessage(), e2);
            } catch (ClassNotFoundException e3) {
                throw new ConnectorException(e3.getMessage(), e3);
            }
            (this.plugedInRecordReader = (RecordReader<K, Writable>) ConnectorInputFormat.this.plugedInInputFormat.createRecordReader(split, context)).initialize(split, context);
        }

        @Override
        public boolean nextKeyValue() throws IOException, InterruptedException {
            return this.plugedInRecordReader.nextKeyValue();
        }

        @Override
        public K getCurrentKey() throws IOException, InterruptedException {
            return (K) this.plugedInRecordReader.getCurrentKey();
        }

        @Override
        public ConnectorRecord getCurrentValue() throws IOException, InterruptedException {
            return this.sourceSerDe.deserialize((Writable) this.plugedInRecordReader.getCurrentValue());
        }

        @Override
        public float getProgress() throws IOException, InterruptedException {
            return this.plugedInRecordReader.getProgress();
        }

        @Override
        public void close() throws IOException {
            this.plugedInRecordReader.close();
            this.end_timestamp = System.currentTimeMillis();
            this.logger.info((Object) ("recordreader class " + this.getClass().getName() + "close time is:  " + this.end_timestamp));
            this.logger.info((Object) ("the total elapsed time of recordreader " + this.getClass().getName() + (this.end_timestamp - this.start_timestamp) / 1000L + "s"));
        }
    }
}
