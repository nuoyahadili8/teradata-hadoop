package com.teradata.connector.hdfs;

import java.io.IOException;
import org.apache.avro.mapreduce.AvroKeyInputFormat;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.ObjectWritable;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;

/**
 * @author Administrator
 */
public class HdfsAvroInputFormat<WritableComparable, Writable> extends FileInputFormat<WritableComparable, Writable> {
    @Override
    public RecordReader<WritableComparable, Writable> createRecordReader(final InputSplit split, final TaskAttemptContext context) throws IOException, InterruptedException {
        return new HdfsAvroRecordReader(split, context);
    }

    public class HdfsAvroRecordReader extends RecordReader<WritableComparable, Writable> {
        private RecordReader reader;
        protected Configuration configuration;
        private ObjectWritable returnvalue;

        public HdfsAvroRecordReader(final InputSplit split, final TaskAttemptContext context) {
            this.reader = null;
            this.returnvalue = new ObjectWritable();
        }

        @Override
        public float getProgress() throws IOException, InterruptedException {
            return this.reader.getProgress();
        }

        @Override
        public void close() throws IOException {
            this.reader.close();
        }

        @Override
        public WritableComparable getCurrentKey() throws IOException, InterruptedException {
            return (WritableComparable) NullWritable.get();
        }

        @Override
        public Writable getCurrentValue() throws IOException, InterruptedException {
            this.returnvalue.set(this.reader.getCurrentKey());
            return (Writable) this.returnvalue;
        }

        @Override
        public void initialize(final InputSplit split, final TaskAttemptContext context) throws IOException, InterruptedException {
            final AvroKeyInputFormat avroinputformat = new AvroKeyInputFormat();
            (this.reader = avroinputformat.createRecordReader(split, context)).initialize(split, context);
        }

        @Override
        public boolean nextKeyValue() throws IOException, InterruptedException {
            return this.reader.nextKeyValue();
        }
    }
}
