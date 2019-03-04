package com.teradata.connector.hive;

import com.teradata.connector.common.utils.HadoopConfigurationUtils;

import java.io.IOException;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;


public class HiveSequenceFileOutputFormat<K, V> extends SequenceFileOutputFormat<NullWritable, V> {
    @Override
    public RecordWriter<NullWritable, V> getRecordWriter(final TaskAttemptContext context) throws IOException, InterruptedException {
        return new HiveRecordWriter(super.getRecordWriter(context));

    }

    @Override
    public Path getDefaultWorkFile(final TaskAttemptContext context, final String extension) throws IOException {
        return new Path(context.getConfiguration().get("mapred.output.dir"), FileOutputFormat.getUniqueFile(context, HadoopConfigurationUtils.getOutputBaseName((JobContext) context), extension));
    }
}
