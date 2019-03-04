package com.teradata.connector.hive;

import java.io.IOException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.ql.io.avro.AvroContainerInputFormat;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.ObjectWritable;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

/**
 * @author Administrator
 */
public class HiveAvroFileRecordReader<K, V> extends RecordReader<NullWritable, ObjectWritable> {
    private org.apache.hadoop.mapred.RecordReader reader = null;
    private NullWritable key = null;
    private Object value = null;
    private ObjectWritable returnvalue = new ObjectWritable();
    ;
    protected Configuration configuration;

    @Override
    public float getProgress() throws IOException {
        return this.reader.getProgress();
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
    public ObjectWritable getCurrentValue() throws IOException, InterruptedException {
        this.returnvalue.set(this.value);
        return this.returnvalue;
    }

    @Override
    public void initialize(final InputSplit split, final TaskAttemptContext context) throws IOException, InterruptedException {
        final AvroContainerInputFormat avroinputformat = new AvroContainerInputFormat();
        final JobConf conf = new JobConf();
        final FileSplit fileSplit = (FileSplit) split;
        final org.apache.hadoop.mapred.FileSplit oldFileSplit = new org.apache.hadoop.mapred.FileSplit(fileSplit.getPath(), fileSplit.getStart(), fileSplit.getLength(), fileSplit.getLocations());
        this.reader = avroinputformat.getRecordReader((org.apache.hadoop.mapred.InputSplit) oldFileSplit, conf, Reporter.NULL);
    }

    @Override
    public boolean nextKeyValue() throws IOException, InterruptedException {
        if (this.value == null) {
            this.value = this.reader.createValue();
        }
        return this.reader.next((Object) this.key, this.value);
    }
}
