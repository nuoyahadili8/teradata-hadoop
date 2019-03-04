package com.teradata.connector.common;

import com.teradata.connector.common.converter.ConnectorConverter;
import com.teradata.connector.common.exception.ConnectorException;
import com.teradata.connector.common.utils.ConnectorConfiguration;
import com.teradata.connector.common.utils.ConnectorMapredUtils;
import com.teradata.connector.common.utils.ConnectorSchemaUtils;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.OutputCommitter;
import org.apache.hadoop.mapreduce.OutputFormat;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;

/**
 * @author Administrator
 */
public class DummyRecordWriter<K, V> extends RecordWriter<ConnectorRecord, Writable> {
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

    public DummyRecordWriter(final TaskAttemptContext context) throws IOException, InterruptedException {
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
        this.baseOutputFormats = new HashMap<String, OutputFormat>();
        this.baseRecordWriters = new HashMap<String, RecordWriter>();
        this.taskContexts = new HashMap<String, TaskAttemptContext>();
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
            ConnectorRecord targetConnectorRecord = null;
            targetConnectorRecord = this.dataConverter.convert(key);
            partWritable = (ConnectorPartitionedWritable) this.targetSerDe.serialize(targetConnectorRecord);
        } else {
            partWritable = (ConnectorPartitionedWritable) this.targetSerDe.serialize(key);
        }
        final String pathString = partWritable.getPartitionPath();
        if (!this.baseRecordWriters.containsKey(pathString)) {
            final Path path = new Path(pathString);
            final Configuration subConfiguration = new Configuration(this.context.getConfiguration());
            subConfiguration.setBoolean("mapreduce.fileoutputcommitter.marksuccessfuljobs", false);
            subConfiguration.set("mapred.output.dir", path.toString());
            subConfiguration.set("mapred.work.output.dir", path.toString());
            final TaskAttemptContext taskContext = ConnectorMapredUtils.createTaskAttemptContext(subConfiguration, this.context);
            final JobContext jobContext = ConnectorMapredUtils.createJobContext(taskContext);
            OutputFormat<K, Writable> pluginOutputFormat = null;
            try {
                pluginOutputFormat = (OutputFormat<K, Writable>) Class.forName(ConnectorConfiguration.getPlugInOutputFormat(this.configuration)).newInstance();
            } catch (InstantiationException e) {
                e.printStackTrace();
            } catch (IllegalAccessException e2) {
                e2.printStackTrace();
            } catch (ClassNotFoundException e3) {
                e3.printStackTrace();
            }
            pluginOutputFormat.checkOutputSpecs(jobContext);
            final OutputCommitter baseOutputCommitter = pluginOutputFormat.getOutputCommitter(taskContext);
            baseOutputCommitter.setupJob(jobContext);
            baseOutputCommitter.setupTask(taskContext);
            final RecordWriter baseRecordWriter = pluginOutputFormat.getRecordWriter(taskContext);
            this.baseOutputFormats.put(pathString, pluginOutputFormat);
            this.baseRecordWriters.put(pathString, baseRecordWriter);
            this.taskContexts.put(pathString, taskContext);
        }
    }

    @Override
    public void close(final TaskAttemptContext context) throws IOException, InterruptedException {
        for (final String key : this.taskContexts.keySet()) {
            final OutputFormat<?, ?> baseOutputFormat = (OutputFormat<?, ?>) this.baseOutputFormats.get(key);
            final RecordWriter<?, ?> baseRecordWriter = (RecordWriter<?, ?>) this.baseRecordWriters.get(key);
            final TaskAttemptContext taskContext = this.taskContexts.get(key);
            baseRecordWriter.close(taskContext);
            final OutputCommitter baseOutputCommitter = baseOutputFormat.getOutputCommitter(taskContext);
            baseOutputCommitter.commitTask(context);
            final JobContext jobContext = ConnectorMapredUtils.createJobContext(taskContext);
            baseOutputCommitter.commitJob(jobContext);
        }
        this.end_timestamp = System.currentTimeMillis();
        this.logger.info((Object) ("recordwriter class " + this.getClass().getName() + "close time is:  " + this.end_timestamp));
        this.logger.info((Object) ("the total elapsed time of recordwriter " + this.getClass().getName() + (this.end_timestamp - this.start_timestamp) / 1000L + "s"));
    }
}
