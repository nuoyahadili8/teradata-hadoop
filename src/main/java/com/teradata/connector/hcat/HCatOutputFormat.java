package com.teradata.connector.hcat;

import com.teradata.connector.common.exception.ConnectorException;
import java.io.IOException;
import java.lang.reflect.Constructor;
import java.lang.reflect.Method;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.OutputCommitter;
import org.apache.hadoop.mapreduce.OutputFormat;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;

/**
 * @author Administrator
 */
@Deprecated
public class HCatOutputFormat<K, V> extends OutputFormat<K, V> {
    private Class<?> HCatOutputFormatClass;
    private Constructor<?> HCatOutputFormatConstructor;
    private Object HCatOutputFormatInstance;
    private Method HCatOutputFormatCheckOutputSpecsMethod;
    private Method HCatOutputFormatGetOutputCommitterMethod;
    private Method HCatOutputFormatGetRecordWriterMethod;

    public HCatOutputFormat() {
        this.HCatOutputFormatClass = null;
        this.HCatOutputFormatConstructor = null;
        this.HCatOutputFormatInstance = null;
        this.HCatOutputFormatCheckOutputSpecsMethod = null;
        this.HCatOutputFormatGetOutputCommitterMethod = null;
        this.HCatOutputFormatGetRecordWriterMethod = null;
        try {
            this.HCatOutputFormatClass = Class.forName("org.apache.hive.hcatalog.mapreduce.HCatOutputFormat");
        } catch (ClassNotFoundException e) {
            try {
                this.HCatOutputFormatClass = Class.forName("org.apache.hcatalog.mapreduce.HCatOutputFormat");
            } catch (ClassNotFoundException e3) {
                throw new RuntimeException(e);
            }
        }
        try {
            this.HCatOutputFormatConstructor = this.HCatOutputFormatClass.getConstructor((Class<?>[]) new Class[0]);
            this.HCatOutputFormatInstance = this.HCatOutputFormatConstructor.newInstance(new Object[0]);
            this.HCatOutputFormatCheckOutputSpecsMethod = this.HCatOutputFormatClass.getMethod("checkOutputSpecs", JobContext.class);
            this.HCatOutputFormatGetOutputCommitterMethod = this.HCatOutputFormatClass.getMethod("getOutputCommitter", TaskAttemptContext.class);
            this.HCatOutputFormatGetRecordWriterMethod = this.HCatOutputFormatClass.getMethod("getRecordWriter", TaskAttemptContext.class);
        } catch (Exception e2) {
            throw new RuntimeException(e2);
        }
    }

    @Override
    public void checkOutputSpecs(final JobContext jobContext) throws IOException, InterruptedException {
        try {
            this.HCatOutputFormatCheckOutputSpecsMethod.invoke(this.HCatOutputFormatInstance, jobContext);
        } catch (Exception e) {
            throw new ConnectorException(e.getMessage(), e);
        }
    }

    @Override
    public OutputCommitter getOutputCommitter(final TaskAttemptContext taskAttemptContext) throws IOException, InterruptedException {
        Object OutputCommitterInstance = null;
        try {
            OutputCommitterInstance = this.HCatOutputFormatGetOutputCommitterMethod.invoke(this.HCatOutputFormatInstance, taskAttemptContext);
        } catch (Exception e) {
            throw new ConnectorException(e.getMessage(), e);
        }
        return (OutputCommitter) OutputCommitterInstance;
    }

    @Override
    public RecordWriter<K, V> getRecordWriter(final TaskAttemptContext taskAttemptContext) throws IOException, InterruptedException {
        Object RecordWriterInstance = null;
        try {
            RecordWriterInstance = this.HCatOutputFormatGetRecordWriterMethod.invoke(this.HCatOutputFormatInstance, taskAttemptContext);
        } catch (Exception e) {
            throw new ConnectorException(e.getMessage(), e);
        }
        return (RecordWriter<K, V>) RecordWriterInstance;
    }
}
