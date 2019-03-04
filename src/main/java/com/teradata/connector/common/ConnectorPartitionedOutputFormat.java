package com.teradata.connector.common;

import com.teradata.connector.common.exception.ConnectorException;
import com.teradata.connector.common.utils.ConnectorConfiguration;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.OutputCommitter;
import org.apache.hadoop.mapreduce.OutputFormat;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;

/**
 * @author Administrator
 */
public class ConnectorPartitionedOutputFormat<K, V> extends OutputFormat<ConnectorRecord, Writable> {
    OutputFormat<K, Writable> plugedInOutputFormat;

    public ConnectorPartitionedOutputFormat() {
        this.plugedInOutputFormat = null;
    }

    @Override
    public RecordWriter<ConnectorRecord, Writable> getRecordWriter(final TaskAttemptContext context) throws IOException, InterruptedException {
        if (ConnectorConfiguration.getDebugOption(context.getConfiguration()) == 1) {
            return new DummyRecordWriter<Object, Object>(context);
        }
        return new ConnectorPartitionedRecordWriter<Object, Object>(context);
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
}
