package com.teradata.connector.hive;

import java.io.IOException;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;


/**
 * @author Administrator
 */
public class HiveRecordWriter<K, V> extends RecordWriter<K, V> {
    private RecordWriter<K, V> writer;

    public HiveRecordWriter(RecordWriter<K, V> recordWriter) {
        this.writer = recordWriter;
    }

    @Override
    public void write(K k, V value) throws IOException, InterruptedException {
        this.writer.write((K) NullWritable.get(), value);
    }

    @Override
    public void close(final TaskAttemptContext context) throws IOException, InterruptedException {
        this.writer.close(context);
    }
}
