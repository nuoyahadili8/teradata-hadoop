package com.teradata.connector.hive;

import com.teradata.connector.common.utils.HadoopConfigurationUtils;

import java.io.IOException;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;


/**
 * @author Administrator
 */
public class HiveTextFileOutputFormat<K, V> extends TextOutputFormat<NullWritable, V> {
    @Override
    public RecordWriter<NullWritable, V> getRecordWriter(final TaskAttemptContext arg0) throws IOException, InterruptedException {
        return new HiveRecordWriter<NullWritable, V>((RecordWriter<NullWritable, V>) super.getRecordWriter(arg0));
    }

    @Override
    public Path getDefaultWorkFile(final TaskAttemptContext context, final String extension) throws IOException {
        return new Path(context.getConfiguration().get("mapred.output.dir"), FileOutputFormat.getUniqueFile(context, HadoopConfigurationUtils.getOutputBaseName((JobContext) context), extension));
    }
}
