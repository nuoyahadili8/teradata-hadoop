package com.teradata.connector.hcat;

import com.teradata.connector.common.ConnectorSerDe;
import com.teradata.connector.common.exception.ConnectorException;
import com.teradata.connector.common.utils.ConnectorConfiguration;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.InputFormat;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.CombineFileInputFormat;

/**
 * @author Administrator
 */
@Deprecated
public class ConnectorCombineFileHCatInputFormat<K, V> extends CombineFileInputFormat<K, V> {
    private static Log logger;
    InputFormat<K, Writable> plugedInInputFormat;

    public ConnectorCombineFileHCatInputFormat() {
        this.plugedInInputFormat = null;
    }

    @Override
    public List<InputSplit> getSplits(final JobContext job) throws IOException {
        List<InputSplit> splits = null;
        try {
            this.configurePlugedInInputFormat();
            splits = (List<InputSplit>) this.plugedInInputFormat.getSplits(job);
        } catch (InterruptedException e) {
            throw new ConnectorException(e.getMessage(), e);
        }
        final int actualSize = splits.size();
        int numSplits = ConnectorConfiguration.getNumMappers(job.getConfiguration());
        if (numSplits == 0) {
            numSplits = actualSize;
        }
        Collections.sort(splits, new Comparator<InputSplit>() {
            @Override
            public int compare(final InputSplit o1, final InputSplit o2) {
                try {
                    return (int) (o2.getLength() - o1.getLength());
                } catch (Exception ex) {
                    return 0;
                }
            }
        });
        ConnectorCombineFileHCatInputFormat.logger.info((Object) ("expected split number is " + numSplits));
        ConnectorCombineFileHCatInputFormat.logger.info((Object) ("hcat split number is " + splits.size()));
        final List<InputSplit> combinedSplits = new ArrayList<InputSplit>();
        if (actualSize <= numSplits) {
            for (final InputSplit split : splits) {
                combinedSplits.add(new ConnectorCombineFileHCatSplit(new InputSplit[]{split}));
            }
        } else {
            final List<List<InputSplit>> packageSplits = new ArrayList<List<InputSplit>>();
            for (int i = 0; i < numSplits; ++i) {
                packageSplits.add(new ArrayList<InputSplit>());
            }
            boolean odd = true;
            int lastRound = 0;
            for (int j = 0; j < actualSize; ++j) {
                final int lane = j % numSplits;
                final int round = j / numSplits;
                if (round != lastRound) {
                    odd = !odd;
                }
                if (odd) {
                    packageSplits.get(lane).add(splits.get(j));
                } else {
                    packageSplits.get(numSplits - 1 - lane).add(splits.get(j));
                }
                lastRound = round;
            }
            for (int j = 0; j < numSplits; ++j) {
                final ConnectorCombineFileHCatSplit combineSplit = new ConnectorCombineFileHCatSplit(packageSplits.get(j).toArray(new InputSplit[0]));
                combinedSplits.add(combineSplit);
            }
        }
        ConnectorCombineFileHCatInputFormat.logger.info((Object) ("final split number is " + combinedSplits.size()));
        return combinedSplits;
    }

    @Override
    public RecordReader<K, V> createRecordReader(final InputSplit split, final TaskAttemptContext taskContext) throws IOException {
        return new ConnectorCombineFileHCatRecordReader(split, taskContext, this);
    }

    public RecordReader<K, V> createBaseRecordReader(final InputSplit split, final TaskAttemptContext taskContext) throws IOException, InterruptedException {
        this.configurePlugedInInputFormat();
        return (RecordReader<K, V>) this.plugedInInputFormat.createRecordReader(split, taskContext);
    }

    protected void configurePlugedInInputFormat() {
        try {
            if (this.plugedInInputFormat == null) {
                Class<?> HCatInputFormatClass = null;
                try {
                    HCatInputFormatClass = Class.forName("org.apache.hive.hcatalog.mapreduce.HCatInputFormat");
                } catch (ClassNotFoundException e) {
                    try {
                        HCatInputFormatClass = Class.forName("org.apache.hcatalog.mapreduce.HCatInputFormat");
                    } catch (ClassNotFoundException e4) {
                        throw new RuntimeException(e);
                    }
                }
                this.plugedInInputFormat = (InputFormat<K, Writable>) HCatInputFormatClass.newInstance();
            }
        } catch (InstantiationException e2) {
            e2.printStackTrace();
        } catch (IllegalAccessException e3) {
            e3.printStackTrace();
        }
    }

    static {
        ConnectorCombineFileHCatInputFormat.logger = LogFactory.getLog((Class) ConnectorCombineFileHCatInputFormat.class);
    }

    @Deprecated
    class ConnectorCombineFileHCatRecordReader extends RecordReader<K, V> {
        protected ConnectorCombineFileHCatInputFormat hcatInputFormat;
        protected ConnectorCombineFileHCatSplit split;
        protected TaskAttemptContext context;
        protected int index;
        protected long progress;
        protected RecordReader<K, V> currentRecordReader;
        protected ConnectorSerDe sourceSerDe;

        public ConnectorCombineFileHCatRecordReader(final InputSplit split, final TaskAttemptContext context, final ConnectorCombineFileHCatInputFormat hcatInputFormat) throws IOException {
            this.split = (ConnectorCombineFileHCatSplit) split;
            this.context = context;
            this.index = 0;
            this.currentRecordReader = null;
            this.progress = 0L;
            this.hcatInputFormat = hcatInputFormat;
            try {
                this.initNextRecordReader();
            } catch (Exception e) {
                throw new ConnectorException(e.getMessage());
            }
        }

        @Override
        public void initialize(final InputSplit split, final TaskAttemptContext context) throws IOException, InterruptedException {
            this.split = (ConnectorCombineFileHCatSplit) split;
            this.context = context;
            if (this.currentRecordReader != null) {
                this.currentRecordReader.initialize(((ConnectorCombineFileHCatSplit) split).get(0), context);
            }
        }

        @Override
        public boolean nextKeyValue() throws IOException, InterruptedException {
            while (this.currentRecordReader == null || !this.currentRecordReader.nextKeyValue()) {
                if (!this.initNextRecordReader()) {
                    return false;
                }
            }
            return true;
        }

        @Override
        public K getCurrentKey() throws IOException, InterruptedException {
            return (K) this.currentRecordReader.getCurrentKey();
        }

        @Override
        public V getCurrentValue() throws IOException, InterruptedException {
            return (V) this.currentRecordReader.getCurrentValue();
        }

        @Override
        public void close() throws IOException {
            if (this.currentRecordReader != null) {
                this.currentRecordReader.close();
                this.currentRecordReader = null;
            }
        }

        @Override
        public float getProgress() throws IOException, InterruptedException {
            long currentProgress = 0L;
            if (null != this.currentRecordReader) {
                currentProgress = (long) (this.currentRecordReader.getProgress() * this.split.get(this.index - 1).getLength());
            }
            final long splitLength = this.split.getLength();
            return Math.min(1.0f, (this.progress + currentProgress) / (float) splitLength);
        }

        protected boolean initNextRecordReader() throws IOException, InterruptedException {
            if (this.currentRecordReader != null) {
                this.currentRecordReader.close();
                this.currentRecordReader = null;
                if (this.index > 0) {
                    this.progress += this.split.get(this.index - 1).getLength();
                }
            }
            if (this.split.length() == this.index) {
                return false;
            }
            try {
                final Configuration configuration = this.context.getConfiguration();
                this.currentRecordReader = ConnectorCombineFileHCatInputFormat.this.createBaseRecordReader(this.split.get(this.index), this.context);
                (this.sourceSerDe = (ConnectorSerDe) Class.forName(ConnectorConfiguration.getInputSerDe(configuration)).newInstance()).initialize((JobContext) this.context, ConnectorConfiguration.direction.input);
                if (this.index > 0) {
                    this.currentRecordReader.initialize(this.split.get(this.index), this.context);
                }
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
            ++this.index;
            return true;
        }
    }
}
