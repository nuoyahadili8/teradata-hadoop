package com.teradata.connector.hdfs;

import java.io.IOException;
import org.apache.avro.mapreduce.AvroKeyOutputFormat;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.ObjectWritable;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

/**
 * @author Administrator
 */
public class HdfsAvroOutputFormat extends FileOutputFormat<Writable, Writable> {
    @Override
    public RecordWriter getRecordWriter(final TaskAttemptContext context) throws IOException {
        final AvroKeyOutputFormat avrooutputformat = new AvroKeyOutputFormat();
        return new AvroRecordWriter(avrooutputformat.getRecordWriter(context));
    }

    public class AvroRecordWriter extends RecordWriter<Writable, Writable> {
        RecordWriter writer;

        AvroRecordWriter(final RecordWriter recordWriter) {
            this.writer = null;
            this.writer = recordWriter;
        }

        @Override
        public void write(final Writable writable, final Writable row) throws IOException, InterruptedException {
            this.writer.write(((ObjectWritable) row).get(), (Object) NullWritable.get());
        }

        @Override
        public void close(final TaskAttemptContext context) throws IOException, InterruptedException {
            this.writer.close((TaskAttemptContext) null);
        }
    }
}
