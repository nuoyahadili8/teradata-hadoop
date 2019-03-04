package com.teradata.connector.common;

import com.teradata.connector.common.converter.ConnectorConverter;
import com.teradata.connector.common.exception.ConnectorException;
import com.teradata.connector.common.utils.ConnectorConfiguration;
import com.teradata.connector.common.utils.ConnectorSchemaUtils;

import java.io.IOException;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.OutputCommitter;
import org.apache.hadoop.mapreduce.OutputFormat;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;


/**
 * @author Administrator
 */
public class ConnectorOutputFormat<K, V> extends OutputFormat<ConnectorRecord, Writable> {

    OutputFormat<K, Writable> plugedInOutputFormat;

    public ConnectorOutputFormat() {
        this.plugedInOutputFormat = null;
    }

    @Override
    public RecordWriter<ConnectorRecord, Writable> getRecordWriter(final TaskAttemptContext context) throws IOException, InterruptedException {
        if (ConnectorConfiguration.getDebugOption(context.getConfiguration()) == 1) {
            return new DummyRecordWriter(context);
        }
        return new ConnectorFileRecordWriter(context);
    }

    @Override
    public void checkOutputSpecs(final JobContext context) throws IOException, InterruptedException {
        if (this.plugedInOutputFormat == null) {
            this.configurePlugedInOutputFormat(context.getConfiguration());
        }
        this.plugedInOutputFormat.checkOutputSpecs(context);
    }

    @Override
    public OutputCommitter getOutputCommitter(final TaskAttemptContext context) throws IOException, InterruptedException {
        if (this.plugedInOutputFormat == null) {
            this.configurePlugedInOutputFormat(context.getConfiguration());
        }
        return this.plugedInOutputFormat.getOutputCommitter(context);
    }

    protected void configurePlugedInOutputFormat(final Configuration configuration) throws IOException, InterruptedException {
        try {
            this.plugedInOutputFormat = (OutputFormat<K, Writable>) Class.forName(ConnectorConfiguration.getPlugInOutputFormat(configuration)).newInstance();
        } catch (InstantiationException e) {
            throw new ConnectorException(e.getMessage(), e);
        } catch (IllegalAccessException e2) {
            throw new ConnectorException(e2.getMessage(), e2);
        } catch (ClassNotFoundException e3) {
            throw new ConnectorException(e3.getMessage(), e3);
        }
    }

    protected ConnectorRecordSchema getSourceConnectorRecordSchema(final Configuration configuration) {
        ConnectorRecordSchema schema = null;
        try {
            schema = ConnectorSchemaUtils.recordSchemaFromString(ConnectorConfiguration.getInputConverterRecordSchema(configuration));
        } catch (ConnectorException e) {
            e.printStackTrace();
        }
        return schema;
    }

    class ConnectorFileRecordWriter extends RecordWriter<ConnectorRecord, Writable> {
        private Log logger;
        protected RecordWriter<Writable, Writable> baseRecordWriter;
        protected ConnectorSerDe targetSerDe;
        protected ConnectorConverter dataConverter;
        protected Configuration configuration;
        protected ConnectorRecord targetConnectorRecord;
        private long end_timestamp;
        private long start_timestamp;

        public ConnectorFileRecordWriter(final TaskAttemptContext context) throws IOException, InterruptedException {
            this.logger = LogFactory.getLog((Class) ConnectorFileRecordWriter.class);
            this.baseRecordWriter = null;
            this.targetSerDe = null;
            this.dataConverter = null;
            this.targetConnectorRecord = null;
            this.end_timestamp = 0L;
            this.start_timestamp = 0L;
            this.start_timestamp = System.currentTimeMillis();
            this.logger.info((Object) ("recordwriter class " + this.getClass().getName() + "initialize time is:  " + this.start_timestamp));
            this.configuration = context.getConfiguration();
            if (ConnectorOutputFormat.this.plugedInOutputFormat == null) {
                ConnectorOutputFormat.this.configurePlugedInOutputFormat(this.configuration);
            }
            this.baseRecordWriter = (RecordWriter<Writable, Writable>) ConnectorOutputFormat.this.plugedInOutputFormat.getRecordWriter(context);
            try {
                (this.targetSerDe = (ConnectorSerDe) Class.forName(ConnectorConfiguration.getOutputSerDe(this.configuration)).newInstance()).initialize((JobContext) context, ConnectorConfiguration.direction.output);
                final String converterClass = ConnectorConfiguration.getDataConverter(this.configuration);
                if (!converterClass.isEmpty()) {
                    (this.dataConverter = (ConnectorConverter) Class.forName(ConnectorConfiguration.getDataConverter(this.configuration)).newInstance()).initialize((JobContext) context);
                    this.dataConverter.lookupConverter(ConnectorOutputFormat.this.getSourceConnectorRecordSchema(this.configuration));
                }
            } catch (InstantiationException e) {
                throw new ConnectorException(e.getMessage(), e);
            } catch (IllegalAccessException e2) {
                throw new ConnectorException(e2.getMessage(), e2);
            } catch (ClassNotFoundException e3) {
                e3.printStackTrace();
            }
        }

        @Override
        public void write(ConnectorRecord key, Writable value) throws IOException, InterruptedException {
            if (this.dataConverter != null) {
                this.targetConnectorRecord = this.dataConverter.convert(key);
                this.baseRecordWriter.write(NullWritable.get(), this.targetSerDe.serialize(this.targetConnectorRecord));
                return;
            }
            this.baseRecordWriter.write(NullWritable.get(), this.targetSerDe.serialize(key));
        }


        @Override
        public void close(final TaskAttemptContext context) throws IOException, InterruptedException {
            this.baseRecordWriter.close(context);
            this.end_timestamp = System.currentTimeMillis();
            this.logger.info((Object) ("recordwriter class " + this.getClass().getName() + "close time is:  " + this.end_timestamp));
            this.logger.info((Object) ("the total elapsed time of recordwriter " + this.getClass().getName() + (this.end_timestamp - this.start_timestamp) / 1000L + "s"));
        }
    }

    class DummyRecordWriter extends RecordWriter<ConnectorRecord, Writable> {
        private Log logger;
        protected RecordWriter<Writable, Writable> baseRecordWriter;
        protected ConnectorSerDe targetSerDe;
        protected ConnectorConverter dataConverter;
        protected Configuration configuration;
        protected ConnectorRecord targetConnectorRecord;
        private long end_timestamp;
        private long start_timestamp;

        public DummyRecordWriter(final TaskAttemptContext context) throws IOException, InterruptedException {
            this.logger = LogFactory.getLog((Class) DummyRecordWriter.class);
            this.baseRecordWriter = null;
            this.targetSerDe = null;
            this.dataConverter = null;
            this.targetConnectorRecord = null;
            this.end_timestamp = 0L;
            this.start_timestamp = 0L;
            this.start_timestamp = System.currentTimeMillis();
            this.logger.info((Object) ("recordwriter class " + this.getClass().getName() + "initialize time is:  " + this.start_timestamp));
            this.configuration = context.getConfiguration();
            if (ConnectorOutputFormat.this.plugedInOutputFormat == null) {
                ConnectorOutputFormat.this.configurePlugedInOutputFormat(this.configuration);
            }
            this.baseRecordWriter = (RecordWriter<Writable, Writable>) ConnectorOutputFormat.this.plugedInOutputFormat.getRecordWriter(context);
            try {
                (this.targetSerDe = (ConnectorSerDe) Class.forName(ConnectorConfiguration.getOutputSerDe(this.configuration)).newInstance()).initialize((JobContext) context, ConnectorConfiguration.direction.output);
                final String converterClass = ConnectorConfiguration.getDataConverter(this.configuration);
                if (!converterClass.isEmpty()) {
                    (this.dataConverter = (ConnectorConverter) Class.forName(ConnectorConfiguration.getDataConverter(this.configuration)).newInstance()).initialize((JobContext) context);
                    this.dataConverter.lookupConverter(ConnectorOutputFormat.this.getSourceConnectorRecordSchema(this.configuration));
                }
            } catch (InstantiationException e) {
                throw new ConnectorException(e.getMessage(), e);
            } catch (IllegalAccessException e2) {
                throw new ConnectorException(e2.getMessage(), e2);
            } catch (ClassNotFoundException e3) {
                e3.printStackTrace();
            }
        }

        @Override
        public void write(final ConnectorRecord key, final Writable value) throws IOException, InterruptedException {
            if (this.dataConverter != null) {
                this.targetConnectorRecord = this.dataConverter.convert(key);
            }
        }

        @Override
        public void close(final TaskAttemptContext context) throws IOException, InterruptedException {
            this.baseRecordWriter.close(context);
            this.end_timestamp = System.currentTimeMillis();
            this.logger.info((Object) ("recordwriter class " + this.getClass().getName() + "close time is:  " + this.end_timestamp));
            this.logger.info((Object) ("the total elapsed time of recordwriter " + this.getClass().getName() + (this.end_timestamp - this.start_timestamp) / 1000L + "s"));
        }
    }
}
