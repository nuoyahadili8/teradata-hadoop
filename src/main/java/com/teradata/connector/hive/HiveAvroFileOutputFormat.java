package com.teradata.connector.hive;

import com.teradata.connector.common.utils.ConnectorConfiguration;
import com.teradata.connector.common.utils.HadoopConfigurationUtils;
import java.io.IOException;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.ql.io.avro.AvroContainerOutputFormat;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Progressable;


public class HiveAvroFileOutputFormat<K, V> extends FileOutputFormat<NullWritable, Writable> {
    @Override
    public RecordWriter getRecordWriter(final TaskAttemptContext context) throws IOException {
        final AvroContainerOutputFormat output = new AvroContainerOutputFormat();
        final JobConf conf = new JobConf(context.getConfiguration());
        String outputPath = null;
        final String outputdir = context.getConfiguration().get("mapred.output.dir");
        final String partition = FileOutputFormat.getUniqueFile(context, HadoopConfigurationUtils.getOutputBaseName((JobContext) context), "");
        if (outputdir != null) {
            outputPath = outputdir + "/" + partition;
        }
        final FileSystem fs = new Path(outputPath).getFileSystem(context.getConfiguration());
        final org.apache.hadoop.mapred.RecordWriter recordWriter = output.getRecordWriter(fs, conf, outputPath, (Progressable) null);
        return new TeradataAvroRecordWriter(recordWriter);
    }

    private static class TeradataAvroRecordWriter extends RecordWriter<Writable, Writable> {
        org.apache.hadoop.mapred.RecordWriter writer;

        TeradataAvroRecordWriter(final org.apache.hadoop.mapred.RecordWriter recordWriter) {
            this.writer = null;
            this.writer = recordWriter;
        }

        @Override
        public void write(final Writable nullWritable, final Writable row) throws IOException {
            this.writer.write(NullWritable.get(), row);
        }

        @Override
        public void close(final TaskAttemptContext context) throws IOException {
            this.writer.close(null);
        }
    }
}
