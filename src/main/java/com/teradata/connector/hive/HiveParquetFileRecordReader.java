package com.teradata.connector.hive;

import java.io.IOException;
import java.util.Map;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.ArrayWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.ObjectWritable;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.parquet.hadoop.ParquetInputFormat;
import org.apache.parquet.hadoop.util.ContextUtil;
import parquet.schema.MessageType;


/**
 * @author Administrator
 */
public class HiveParquetFileRecordReader<K, V> extends RecordReader<NullWritable, ArrayWritable> {
    private RecordReader<Void, ObjectWritable> reader = null;
    private NullWritable key = null;
    private Object value = null;
    MessageType fileSchema;
    Map<String, String> fileMetaData;
    private ArrayWritable returnvalue;
    protected Configuration configuration;

    @Override
    public float getProgress() throws IOException {
        try {
            return this.reader.getProgress();
        } catch (InterruptedException e) {
            e.printStackTrace();
            return 0.0f;
        }
    }

    @Override
    public void close() throws IOException {
        this.reader.close();
    }

    @Override
    public NullWritable getCurrentKey() throws IOException, InterruptedException {
        return this.key;
    }

    @Override
    public ArrayWritable getCurrentValue() throws IOException, InterruptedException {
        this.value = this.reader.getCurrentValue();
        final HiveParquetPlainToWritable s = new HiveParquetPlainToWritable();
        if (this.value != null) {
            this.returnvalue = s.getArrayWritable((HiveParquetReadSupportExt.PlainParquetExt) this.value, this.configuration);
        }
        return this.returnvalue;
    }

    @Override
    public void initialize(final InputSplit split, final TaskAttemptContext context) throws IOException, InterruptedException {
        this.configuration = ContextUtil.getConfiguration(context);
        final FileSplit fileSplit = (FileSplit) split;
        final org.apache.hadoop.mapred.FileSplit oldFileSplit = new org.apache.hadoop.mapred.FileSplit(fileSplit.getPath(), fileSplit.getStart(), fileSplit.getLength(), fileSplit.getLocations());
        final ParquetInputFormat<ObjectWritable> ParquetInputFormat = (ParquetInputFormat<ObjectWritable>) new ParquetInputFormat();
        (this.reader = ParquetInputFormat.createRecordReader(oldFileSplit, context)).initialize(oldFileSplit, context);
    }

    @Override
    public boolean nextKeyValue() throws IOException, InterruptedException {
        this.value = this.reader.getCurrentValue();
        return this.reader.nextKeyValue();
    }
}
