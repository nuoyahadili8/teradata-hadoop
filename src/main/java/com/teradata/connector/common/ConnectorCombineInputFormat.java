package com.teradata.connector.common;

import com.teradata.connector.common.exception.ConnectorException;
import com.teradata.connector.common.utils.ConnectorConfiguration;
import com.teradata.connector.common.utils.ConnectorConfiguration.direction;
import com.teradata.connector.common.utils.HadoopConfigurationUtils;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Set;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.InputFormat;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.CombineFileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.CombineFileSplit;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;


/**
 * @author Administrator
 */
public class ConnectorCombineInputFormat<K, V> extends InputFormat<K, V> {
    InputFormat<K, Writable> plugedInInputFormat;

    public ConnectorCombineInputFormat() {
        this.plugedInInputFormat = null;
    }

    @Override
    public List<InputSplit> getSplits(final JobContext job) throws IOException, InterruptedException {
        try {
            if (!FileInputFormat.class.isAssignableFrom(Class.forName(ConnectorConfiguration.getPlugInInputFormat(job.getConfiguration())))) {
                return this.getNoneFileSplit(job);
            }
        } catch (Exception e1) {
            throw new ConnectorException(e1.getMessage(), e1);
        }
        return new ConnectorCombineFileInputFormat().getSplits(job);
    }

    private List<InputSplit> getNoneFileSplit(final JobContext job) throws IOException, InterruptedException, InstantiationException, IllegalAccessException, ClassNotFoundException {
        final Configuration configuration = job.getConfiguration();
        final int numMappers = ConnectorConfiguration.getNumMappers(configuration);
        if (this.plugedInInputFormat == null) {
            this.configurePlugedInInuputFormat(job);
        }
        final List<InputSplit> inputSplits = (List<InputSplit>) this.plugedInInputFormat.getSplits(job);
        final List<InputSplit> combinedSplits = new ArrayList<InputSplit>();
        final String inputSplitClass = ConnectorConfiguration.getInputSplit(configuration);
        if (numMappers >= inputSplits.size()) {
            for (final InputSplit split : inputSplits) {
                final InputSplit[] splits = {split};
                combinedSplits.add(new ConnectorCombineSplit(splits, inputSplitClass));
            }
        } else {
            final List<List<InputSplit>> packageSplits = new ArrayList<List<InputSplit>>();
            for (int i = 0; i < numMappers; ++i) {
                packageSplits.add(new ArrayList<InputSplit>());
            }
            boolean odd = true;
            int lastRound = 0;
            for (int j = 0; j < inputSplits.size(); ++j) {
                final int lane = j % numMappers;
                final int round = j / numMappers;
                if (round != lastRound) {
                    odd = !odd;
                }
                if (odd) {
                    packageSplits.get(lane).add(inputSplits.get(j));
                } else {
                    packageSplits.get(numMappers - 1 - lane).add(inputSplits.get(j));
                }
                lastRound = round;
            }
            for (int j = 0; j < numMappers; ++j) {
                final ConnectorCombineSplit combineSplit = new ConnectorCombineSplit(packageSplits.get(j).toArray(new InputSplit[0]), inputSplitClass);
                combinedSplits.add(combineSplit);
            }
        }
        return combinedSplits;
    }

    @Override
    public RecordReader<K, V> createRecordReader(final InputSplit split, final TaskAttemptContext context) throws IOException, InterruptedException {
        if (split instanceof CombineFileSplit) {
            return new ConnectorCombineFileRecordReader((CombineFileSplit) split, context);
        }
        return new ConnectorCombinePlugedinInputRecordReader((ConnectorCombineSplit) split, context);
    }

    protected void configurePlugedInInuputFormat(final JobContext context) {
        final Configuration configuration = context.getConfiguration();
        try {
            this.plugedInInputFormat = (InputFormat<K, Writable>) Class.forName(ConnectorConfiguration.getPlugInInputFormat(configuration)).newInstance();
        } catch (InstantiationException e) {
            e.printStackTrace();
        } catch (IllegalAccessException e2) {
            e2.printStackTrace();
        } catch (ClassNotFoundException e3) {
            e3.printStackTrace();
        }
    }

    public static class ConnectorCombineSplit extends InputSplit implements Writable {
        protected InputSplit[] inputSplits;
        private String[] locations;
        private long totalLength;
        private String inputSplitClassName;

        public ConnectorCombineSplit() {
        }

        public ConnectorCombineSplit(final InputSplit[] splits, final String inputSplitClassName) {
            this.inputSplits = splits;
            this.inputSplitClassName = inputSplitClassName;
        }

        public String getInputSplitClassName() {
            return this.inputSplitClassName;
        }

        public void setInputSplitClassName(final String inputSplitClassName) {
            this.inputSplitClassName = inputSplitClassName;
        }

        public InputSplit[] getInputSplits() {
            return this.inputSplits;
        }

        public void setInputSplits(final InputSplit[] inputSplits) {
            this.inputSplits = inputSplits;
        }

        public void setLocations(final String[] locations) {
            this.locations = locations;
        }

        @Override
        public void write(final DataOutput out) throws IOException {
            out.writeUTF(this.inputSplitClassName);
            out.writeLong(this.totalLength);
            out.writeInt(this.inputSplits.length);
            for (final InputSplit split : this.inputSplits) {
                ((Writable) split).write(out);
            }
        }

        @Override
        public void readFields(final DataInput in) throws IOException {
            this.inputSplitClassName = in.readUTF();
            this.totalLength = in.readLong();
            final int arrLength = in.readInt();
            this.inputSplits = new InputSplit[arrLength];
            for (int i = 0; i < arrLength; ++i) {
                try {
                    this.inputSplits[i] = (InputSplit) Class.forName(this.inputSplitClassName).newInstance();
                } catch (Exception e) {
                    throw new ConnectorException(e.getMessage(), e);
                }
                ((Writable) this.inputSplits[i]).readFields(in);
            }
        }

        @Override
        public long getLength() throws IOException, InterruptedException {
            if (this.totalLength == 0L) {
                for (final InputSplit split : this.inputSplits) {
                    this.totalLength += split.getLength();
                }
            }
            return this.totalLength;
        }

        @Override
        public String[] getLocations() throws IOException, InterruptedException {
            if (this.locations == null) {
                final Set<String> locations = new HashSet<String>();
                for (final InputSplit split : this.inputSplits) {
                    locations.addAll(Arrays.asList(split.getLocations()));
                }
                this.locations = locations.toArray(new String[0]);
            }
            return this.locations;
        }

        @Override
        public String toString() {
            final StringBuffer sb = new StringBuffer();
            sb.append("inputSplitClass:" + this.inputSplitClassName + "\n");
            for (int i = 0; i < this.inputSplits.length; ++i) {
                sb.append("split" + i + ":" + this.inputSplits[i] + "\n");
            }
            return sb.toString();
        }
    }

    private class ConnectorCombineFileInputFormat extends CombineFileInputFormat<K, V> {
        public static final int SPLIT_LOCATIONS_MAX = 6;

        @Override
        public RecordReader<K, V> createRecordReader(final InputSplit split, final TaskAttemptContext context) throws IOException {
            return null;
        }

        @Override
        public List<InputSplit> getSplits(final JobContext job) throws IOException {
            final long numSplits = ConnectorConfiguration.getNumMappers(job.getConfiguration());
            long numFileBytes = 0L;
            long splitSize = 0L;
            if (numSplits != 0L) {
                numFileBytes = this.getJobSize(job);
                splitSize = numFileBytes / numSplits;
                this.setMaxSplitSize(splitSize);
                this.setMinSplitSizeRack(splitSize);
            }
            final List<InputSplit> splits = (List<InputSplit>) super.getSplits(job);
            final int splitNumber = splits.size();
            String[] hosts = null;
            for (int i = splitNumber - 1; i >= 0; --i) {
                final InputSplit split = splits.get(i);
                try {
                    final String[] locations = split.getLocations();
                    if (locations != null && locations.length != 0) {
                        break;
                    }
                    if (hosts == null) {
                        hosts = HadoopConfigurationUtils.getAllActiveHosts(job);
                    }
                    final CombineFileSplit combineSplit = (CombineFileSplit) split;
                    final CombineFileSplit newCombineSplit = new CombineFileSplit(combineSplit.getPaths(), combineSplit.getStartOffsets(), combineSplit.getLengths(), HadoopConfigurationUtils.selectUniqueActiveHosts(hosts, 6));
                    splits.remove(i);
                    splits.add((InputSplit) newCombineSplit);
                } catch (InterruptedException e) {
                    throw new ConnectorException(e.getMessage(), e);
                } catch (IOException e2) {
                    throw new ConnectorException(e2.getMessage(), e2);
                }
            }
            return splits;
        }

        protected long getJobSize(final JobContext job) throws IOException {
            final List<FileStatus> stats = (List<FileStatus>) this.listStatus(job);
            long count = 0L;
            for (final FileStatus stat : stats) {
                count += stat.getLen();
            }
            return count;
        }
    }

    private class ConnectorCombineFileRecordReader extends RecordReader<K, V> {
        private Log logger;
        protected CombineFileSplit split;
        protected TaskAttemptContext context;
        protected int index;
        protected long progress;
        protected RecordReader<K, V> currentPlugedInRecordReader;
        protected ConnectorSerDe sourceSerDe;
        private long end_timestamp;
        private long start_timestamp;

        public ConnectorCombineFileRecordReader(final CombineFileSplit split, final TaskAttemptContext context) throws IOException {
            this.logger = LogFactory.getLog((Class) ConnectorCombineFileRecordReader.class);
            this.end_timestamp = 0L;
            this.start_timestamp = 0L;
            this.start_timestamp = System.currentTimeMillis();
            this.logger.info((Object) ("recordreader class " + this.getClass().getName() + "initialize time is:  " + this.start_timestamp));
            this.split = split;
            this.context = context;
            this.index = 0;
            this.currentPlugedInRecordReader = null;
            this.progress = 0L;
            this.initNextRecordReader();
        }

        @Override
        public void initialize(final InputSplit split, final TaskAttemptContext context) throws IOException, InterruptedException {
            final FileSplit fileSplit = new FileSplit(((CombineFileSplit) split).getPath(this.index), ((CombineFileSplit) split).getOffset(this.index), ((CombineFileSplit) split).getLength(this.index), split.getLocations());
            this.currentPlugedInRecordReader.initialize((InputSplit) fileSplit, context);
            ++this.index;
        }

        @Override
        public boolean nextKeyValue() throws IOException, InterruptedException {
            while (this.currentPlugedInRecordReader == null || !this.currentPlugedInRecordReader.nextKeyValue()) {
                if (!this.initNextRecordReader()) {
                    return false;
                }
            }
            return true;
        }

        @Override
        public K getCurrentKey() throws IOException, InterruptedException {
            return (K) this.currentPlugedInRecordReader.getCurrentKey();
        }

        @Override
        public V getCurrentValue() throws IOException, InterruptedException {
            return (V) this.sourceSerDe.deserialize((Writable) this.currentPlugedInRecordReader.getCurrentValue());
        }

        @Override
        public void close() throws IOException {
            if (this.currentPlugedInRecordReader != null) {
                this.currentPlugedInRecordReader.close();
                this.currentPlugedInRecordReader = null;
            }
            this.end_timestamp = System.currentTimeMillis();
            this.logger.info((Object) ("recordreader class " + this.getClass().getName() + "close time is:  " + this.end_timestamp));
            this.logger.info((Object) ("the total elapsed time of recordreader " + this.getClass().getName() + (this.end_timestamp - this.start_timestamp) / 1000L + "s"));
        }

        @Override
        public float getProgress() throws IOException, InterruptedException {
            long currentProgress = 0L;
            if (this.currentPlugedInRecordReader != null) {
                currentProgress = (long) (this.currentPlugedInRecordReader.getProgress() * this.split.getLength(this.index - 1));
            }
            final long length = this.split.getLength();
            return Math.min(1.0f, (this.progress + currentProgress) / (float) length);
        }

        protected boolean initNextRecordReader() throws IOException {
            if (this.currentPlugedInRecordReader != null) {
                this.currentPlugedInRecordReader.close();
                this.currentPlugedInRecordReader = null;
                if (this.index > 0 && this.index < this.split.getNumPaths()) {
                    this.progress += this.split.getLength(this.index);
                }
            }
            if (this.split.getNumPaths() == this.index) {
                return false;
            }
            try {
                final Configuration configuration = this.context.getConfiguration();
                configuration.set("map.input.file", this.split.getPath(this.index).toString());
                configuration.setLong("map.input.start", this.split.getOffset(this.index));
                configuration.setLong("map.input.length", this.split.getLength(this.index));
                if (ConnectorCombineInputFormat.this.plugedInInputFormat == null) {
                    ConnectorCombineInputFormat.this.configurePlugedInInuputFormat((JobContext) this.context);
                }
                this.currentPlugedInRecordReader = (RecordReader<K, V>) ConnectorCombineInputFormat.this.plugedInInputFormat.createRecordReader((InputSplit) this.split, this.context);
                (this.sourceSerDe = (ConnectorSerDe) Class.forName(ConnectorConfiguration.getInputSerDe(configuration)).newInstance()).initialize((JobContext) this.context, ConnectorConfiguration.direction.input);
                if (this.index > 0) {
                    final FileSplit fileSplit = new FileSplit(this.split.getPath(this.index), this.split.getOffset(this.index), this.split.getLength(this.index), this.split.getLocations());
                    this.currentPlugedInRecordReader.initialize((InputSplit) fileSplit, this.context);
                    ++this.index;
                }
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
            return true;
        }
    }

    private class ConnectorCombinePlugedinInputRecordReader extends RecordReader<K, V> {
        private Log logger;
        protected ConnectorCombineSplit split;
        protected TaskAttemptContext context;
        protected int index;
        protected long progress;
        protected RecordReader<K, V> currentPlugedInRecordReader;
        protected ConnectorSerDe sourceSerDe;
        private long end_timestamp;
        private long start_timestamp;

        public ConnectorCombinePlugedinInputRecordReader(final ConnectorCombineSplit split, final TaskAttemptContext context) throws IOException {
            this.logger = LogFactory.getLog((Class) ConnectorCombinePlugedinInputRecordReader.class);
            this.end_timestamp = 0L;
            this.start_timestamp = 0L;
            this.start_timestamp = System.currentTimeMillis();
            this.logger.info((Object) ("recordreader class " + this.getClass().getName() + "initialize time is:  " + this.start_timestamp));
            this.split = split;
            this.context = context;
            this.index = 0;
            this.currentPlugedInRecordReader = null;
            this.progress = 0L;
            try {
                this.initNextRecordReader();
            } catch (InterruptedException e) {
                throw new ConnectorException(e.getMessage(), e);
            }
        }

        @Override
        public void initialize(final InputSplit split, final TaskAttemptContext context) throws IOException, InterruptedException {
            final InputSplit inputSplit = ((ConnectorCombineSplit) split).getInputSplits()[this.index];
            this.currentPlugedInRecordReader.initialize(inputSplit, context);
            ++this.index;
        }

        @Override
        public boolean nextKeyValue() throws IOException, InterruptedException {
            while (this.currentPlugedInRecordReader == null || !this.currentPlugedInRecordReader.nextKeyValue()) {
                if (!this.initNextRecordReader()) {
                    return false;
                }
            }
            return true;
        }

        @Override
        public K getCurrentKey() throws IOException, InterruptedException {
            return (K) this.currentPlugedInRecordReader.getCurrentKey();
        }

        @Override
        public V getCurrentValue() throws IOException, InterruptedException {
            return (V) this.sourceSerDe.deserialize((Writable) this.currentPlugedInRecordReader.getCurrentValue());
        }

        @Override
        public void close() throws IOException {
            if (this.currentPlugedInRecordReader != null) {
                this.currentPlugedInRecordReader.close();
                this.currentPlugedInRecordReader = null;
            }
            this.end_timestamp = System.currentTimeMillis();
            this.logger.info((Object) ("recordreader class " + this.getClass().getName() + "close time is:  " + this.end_timestamp));
            this.logger.info((Object) ("the total elapsed time of recordreader " + this.getClass().getName() + (this.end_timestamp - this.start_timestamp) / 1000L + "s"));
        }

        @Override
        public float getProgress() throws IOException, InterruptedException {
            long currentProgress = 0L;
            if (this.currentPlugedInRecordReader != null) {
                final long currentLen = this.split.getInputSplits()[this.index - 1].getLength();
                currentProgress = (long) (this.currentPlugedInRecordReader.getProgress() * currentLen);
            }
            final long length = this.split.getLength();
            return Math.min(1.0f, (this.progress + currentProgress) / (float) length);
        }

        protected boolean initNextRecordReader() throws IOException, InterruptedException {
            if (this.currentPlugedInRecordReader != null) {
                this.currentPlugedInRecordReader.close();
                this.currentPlugedInRecordReader = null;
                if (this.index > 0 && this.index < this.split.getInputSplits().length) {
                    this.progress += this.split.getInputSplits()[this.index].getLength();
                }
            }
            if (this.split.getInputSplits().length == this.index) {
                return false;
            }
            try {
                final Configuration configuration = this.context.getConfiguration();
                if (ConnectorCombineInputFormat.this.plugedInInputFormat == null) {
                    ConnectorCombineInputFormat.this.configurePlugedInInuputFormat((JobContext) this.context);
                }
                (this.sourceSerDe = (ConnectorSerDe) Class.forName(ConnectorConfiguration.getInputSerDe(configuration)).newInstance()).initialize((JobContext) this.context, ConnectorConfiguration.direction.input);
                final InputSplit connectorSplit = this.split.getInputSplits()[this.index];
                this.currentPlugedInRecordReader = (RecordReader<K, V>) ConnectorCombineInputFormat.this.plugedInInputFormat.createRecordReader(connectorSplit, this.context);
                if (this.index > 0) {
                    this.currentPlugedInRecordReader.initialize(connectorSplit, this.context);
                    ++this.index;
                }
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
            return true;
        }
    }
}
