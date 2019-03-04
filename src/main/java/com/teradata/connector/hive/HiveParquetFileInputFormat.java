package com.teradata.connector.hive;

import java.io.IOException;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.ObjectWritable;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;


/**
 * @author Administrator
 */
public class HiveParquetFileInputFormat<K extends NullWritable, V extends ObjectWritable> extends FileInputFormat<K, V> {
    @Override
    public RecordReader<K, V> createRecordReader(final InputSplit split, final TaskAttemptContext context) throws IOException, InterruptedException {
        return new HiveParquetFileRecordReader();
    }
}
