package com.teradata.connector.hcat;

import com.teradata.connector.common.exception.ConnectorException;
import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.lang.reflect.Constructor;
import java.lang.reflect.Method;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.InputSplit;

/**
 * @author Administrator
 */
@Deprecated
class ConnectorCombineFileHCatSplit extends InputSplit implements Writable {
    private InputSplit[] splits;
    private String[] locations;
    private long totalLength;
    Constructor<?> HCatSplitConstructor;
    Method HCatSplitWriteMethod;
    Method HCatSplitReadFieldsMethod;

    public ConnectorCombineFileHCatSplit() throws IOException {
        this.HCatSplitConstructor = null;
        this.HCatSplitWriteMethod = null;
        this.HCatSplitReadFieldsMethod = null;
        Class<?> HCatSplitClass = null;
        try {
            HCatSplitClass = Class.forName("org.apache.hive.hcatalog.mapreduce.HCatSplit");
        } catch (ClassNotFoundException e) {
            try {
                HCatSplitClass = Class.forName("org.apache.hcatalog.mapreduce.HCatSplit");
            } catch (ClassNotFoundException e3) {
                throw new ConnectorException(e.getMessage(), e);
            }
        }
        try {
            this.HCatSplitConstructor = HCatSplitClass.getConstructor((Class<?>[]) new Class[0]);
            this.HCatSplitWriteMethod = HCatSplitClass.getMethod("write", DataOutput.class);
            this.HCatSplitReadFieldsMethod = HCatSplitClass.getMethod("readFields", DataInput.class);
        } catch (Exception e2) {
            throw new ConnectorException(e2.getMessage(), e2);
        }
    }

    public ConnectorCombineFileHCatSplit(final InputSplit[] splits) throws IOException {
        this.HCatSplitConstructor = null;
        this.HCatSplitWriteMethod = null;
        this.HCatSplitReadFieldsMethod = null;
        this.splits = splits;
        final Set<String> locations = new HashSet<String>();
        for (final InputSplit split : splits) {
            try {
                this.totalLength += split.getLength();
                locations.addAll(Arrays.asList(split.getLocations()));
            } catch (Exception e) {
                throw new ConnectorException(e.getMessage());
            }
        }
        this.locations = locations.toArray(new String[0]);
        Class<?> HCatSplitClass = null;
        try {
            HCatSplitClass = Class.forName("org.apache.hive.hcatalog.mapreduce.HCatSplit");
        } catch (ClassNotFoundException e2) {
            try {
                HCatSplitClass = Class.forName("org.apache.hcatalog.mapreduce.HCatSplit");
            } catch (ClassNotFoundException e4) {
                throw new ConnectorException(e2.getMessage(), e2);
            }
        }
        try {
            this.HCatSplitConstructor = HCatSplitClass.getConstructor((Class<?>[]) new Class[0]);
            this.HCatSplitWriteMethod = HCatSplitClass.getMethod("write", DataOutput.class);
            this.HCatSplitReadFieldsMethod = HCatSplitClass.getMethod("readFields", DataInput.class);
        } catch (Exception e3) {
            throw new ConnectorException(e3.getMessage(), e3);
        }
    }

    public int length() {
        return this.splits.length;
    }

    public InputSplit get(final int index) {
        return this.splits[index];
    }

    @Override
    public long getLength() throws IOException, InterruptedException {
        if (this.totalLength == 0L) {
            for (final InputSplit split : this.splits) {
                this.totalLength += split.getLength();
            }
        }
        return this.totalLength;
    }

    @Override
    public String[] getLocations() throws IOException, InterruptedException {
        if (this.locations == null) {
            final Set<String> locations = new HashSet<String>();
            for (final InputSplit split : this.splits) {
                locations.addAll(Arrays.asList(split.getLocations()));
            }
            this.locations = locations.toArray(new String[0]);
        }
        return this.locations;
    }

    @Override
    public void write(final DataOutput out) throws IOException {
        out.writeLong(this.totalLength);
        out.writeInt(this.splits.length);
        for (final InputSplit split : this.splits) {
            try {
                this.HCatSplitWriteMethod.invoke(split, out);
            } catch (Exception e) {
                throw new ConnectorException(e.getMessage());
            }
        }
    }

    @Override
    public void readFields(final DataInput in) throws IOException {
        this.totalLength = in.readLong();
        final int arrLength = in.readInt();
        this.splits = new InputSplit[arrLength];
        for (int i = 0; i < arrLength; ++i) {
            try {
                this.splits[i] = (InputSplit) this.HCatSplitConstructor.newInstance(new Object[0]);
                this.HCatSplitReadFieldsMethod.invoke(this.splits[i], in);
            } catch (Exception e) {
                throw new ConnectorException(e.toString());
            }
        }
    }
}
