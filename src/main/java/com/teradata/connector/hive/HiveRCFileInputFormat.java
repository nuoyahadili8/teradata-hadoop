package com.teradata.connector.hive;

import java.io.IOException;

import org.apache.hadoop.hive.serde2.columnar.BytesRefArrayWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;

/**
 * @author Administrator
 */
public class HiveRCFileInputFormat<K extends LongWritable, V extends BytesRefArrayWritable> extends FileInputFormat<K, V> {
    @Override
    public RecordReader<K, V> createRecordReader(final InputSplit split, final TaskAttemptContext context) throws IOException, InterruptedException {
        return new HiveRCFileRecordReader();
    }
}
