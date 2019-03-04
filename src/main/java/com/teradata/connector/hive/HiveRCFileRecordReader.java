package com.teradata.connector.hive;

import java.io.IOException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.ql.io.RCFile;
import org.apache.hadoop.hive.ql.io.RCFile.Reader;
import org.apache.hadoop.hive.serde2.columnar.BytesRefArrayWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;


public class HiveRCFileRecordReader<K, V> extends RecordReader<LongWritable, BytesRefArrayWritable> {
    private RCFile.Reader reader;
    private long start;
    private long end;
    private boolean endReached;
    private LongWritable key;
    private BytesRefArrayWritable value;
    protected Configuration configuration;

    public HiveRCFileRecordReader() {
        this.endReached = false;
        this.key = null;
        this.value = null;
    }

    @Override
    public float getProgress() throws IOException {
        if (this.end == this.start) {
            return 0.0f;
        }
        return Math.min(1.0f, (this.reader.getPosition() - this.start) / (float) (this.end - this.start));
    }

    @Override
    public void close() throws IOException {
        this.reader.close();
    }

    @Override
    public LongWritable getCurrentKey() throws IOException, InterruptedException {
        return this.key;
    }

    @Override
    public BytesRefArrayWritable getCurrentValue() throws IOException, InterruptedException {
        return this.value;
    }

    @Override
    public void initialize(final InputSplit split, final TaskAttemptContext context) throws IOException, InterruptedException {
        final FileSplit fileSplit = (FileSplit) split;
        this.configuration = context.getConfiguration();
        final Path path = fileSplit.getPath();
        final FileSystem fileSystem = path.getFileSystem(this.configuration);
        this.reader = new RCFile.Reader(fileSystem, path, this.configuration);
        this.end = fileSplit.getStart() + fileSplit.getLength();
        if (fileSplit.getStart() > this.reader.getPosition()) {
            this.reader.sync(fileSplit.getStart());
        }
        this.start = this.reader.getPosition();
        this.endReached = (this.start >= this.end);
    }

    @Override
    public boolean nextKeyValue() throws IOException, InterruptedException {
        if (this.endReached) {
            return false;
        }
        if (this.key == null) {
            this.key = new LongWritable();
        }
        if (this.value == null) {
            this.value = new BytesRefArrayWritable();
        }
        this.endReached = !this.reader.next(this.key);
        if (this.endReached) {
            return false;
        }
        final long lastSeenSyncPos = this.reader.lastSeenSyncPos();
        if (lastSeenSyncPos >= this.end) {
            this.endReached = true;
            return false;
        }
        this.reader.getCurrentRow(this.value);
        return true;
    }
}
