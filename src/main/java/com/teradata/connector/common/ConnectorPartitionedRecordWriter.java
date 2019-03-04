package com.teradata.connector.common;

import com.teradata.connector.common.converter.*;
import org.apache.hadoop.conf.*;
import org.apache.commons.logging.*;
import org.apache.hadoop.mapreduce.*;

import java.io.*;

import com.teradata.connector.common.exception.*;
import org.apache.hadoop.fs.*;
import com.teradata.connector.common.utils.*;
import org.apache.hadoop.io.*;

import java.util.*;

class ConnectorPartitionedRecordWriter<K, V> extends RecordWriter<ConnectorRecord, Writable> {
    private Log logger;
    protected long startTimestamp;
    private TaskAttemptContext context;
    protected ConnectorSerDe targetSerDe;
    protected ConnectorConverter dataConverter;
    private Map<String, RecordWriter> baseRecordWriters;
    private Map<String, OutputFormat> baseOutputFormats;
    private Map<String, TaskAttemptContext> taskContexts;
    private Configuration configuration;
    private long end_timestamp;
    private long start_timestamp;

    public ConnectorPartitionedRecordWriter(final TaskAttemptContext context) throws IOException, InterruptedException {
        this.logger = LogFactory.getLog((Class) ConnectorPartitionedRecordWriter.class);
        this.startTimestamp = 0L;
        this.targetSerDe = null;
        this.dataConverter = null;
        this.end_timestamp = 0L;
        this.start_timestamp = 0L;
        this.start_timestamp = System.currentTimeMillis();
        this.logger.info((Object) ("recordwriter class " + this.getClass().getName() + "initialize time is:  " + this.start_timestamp));
        this.context = context;
        this.configuration = context.getConfiguration();
        this.baseOutputFormats = new HashMap<>();
        this.baseRecordWriters = new HashMap<>();
        this.taskContexts = new HashMap<>();
        try {
            (this.targetSerDe = (ConnectorSerDe) Class.forName(ConnectorConfiguration.getOutputSerDe(this.configuration)).newInstance()).initialize((JobContext) context, ConnectorConfiguration.direction.output);
            final String converterClass = ConnectorConfiguration.getDataConverter(this.configuration);
            if (!converterClass.isEmpty()) {
                (this.dataConverter = (ConnectorConverter) Class.forName(ConnectorConfiguration.getDataConverter(this.configuration)).newInstance()).initialize((JobContext) context);
                this.dataConverter.lookupConverter(this.getSourceConnectorRecordSchema(this.configuration));
            }
        } catch (InstantiationException e) {
            e.printStackTrace();
        } catch (IllegalAccessException e2) {
            e2.printStackTrace();
        } catch (ClassNotFoundException e3) {
            e3.printStackTrace();
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

    @Override
    public void write(final ConnectorRecord key, final Writable value) throws IOException, InterruptedException {
        ConnectorPartitionedWritable partWritable;
        if (this.dataConverter != null) {
            partWritable = (ConnectorPartitionedWritable) this.targetSerDe.serialize(this.dataConverter.convert(key));
        } else {
            partWritable = (ConnectorPartitionedWritable) this.targetSerDe.serialize(key);
        }

        final String pathString = partWritable.getPartitionPath();
        final Writable rec = partWritable.getRecord();
        if (!this.baseRecordWriters.containsKey(pathString)) {
            final Path path = new Path(pathString);
            final Configuration subConfiguration = new Configuration(this.context.getConfiguration());
            subConfiguration.setBoolean("mapreduce.fileoutputcommitter.marksuccessfuljobs", false);
            subConfiguration.set("mapred.output.dir", path.toString());
            subConfiguration.set("mapred.work.output.dir", path.toString());
            final TaskAttemptContext taskContext = ConnectorMapredUtils.createTaskAttemptContext(subConfiguration, this.context);
            OutputFormat<K, Writable> pluginOutputFormat = null;
            try {
                pluginOutputFormat = (OutputFormat) Class.forName(ConnectorConfiguration.getPlugInOutputFormat(this.configuration)).newInstance();
            } catch (InstantiationException e) {
                e.printStackTrace();
            } catch (IllegalAccessException e2) {
                e2.printStackTrace();
            } catch (ClassNotFoundException e3) {
                e3.printStackTrace();
            }

            final RecordWriter baseRecordWriter = pluginOutputFormat.getRecordWriter(taskContext);
            this.baseOutputFormats.put(pathString, pluginOutputFormat);
            this.baseRecordWriters.put(pathString, baseRecordWriter);
            this.taskContexts.put(pathString, taskContext);
        }
        this.baseRecordWriters.get(pathString).write((Object) NullWritable.get(), (Object) rec);
    }

    @Override
    public void close(final TaskAttemptContext context) throws IOException, InterruptedException {
        for (final String key : this.taskContexts.keySet()) {
            final RecordWriter<?, ?> baseRecordWriter = (RecordWriter<?, ?>) this.baseRecordWriters.get(key);
            final TaskAttemptContext taskContext = this.taskContexts.get(key);
            baseRecordWriter.close(taskContext);
        }
        this.end_timestamp = System.currentTimeMillis();
        this.logger.info((Object) ("recordwriter class " + this.getClass().getName() + "close time is:  " + this.end_timestamp));
        this.logger.info((Object) ("the total elapsed time of recordwriter " + this.getClass().getName() + (this.end_timestamp - this.start_timestamp) / 1000L + "s"));
    }
}
