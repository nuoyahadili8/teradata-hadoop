package com.teradata.connector.hive;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import org.apache.hadoop.io.Writable;


/**
 * @author Administrator
 */
public class HiveWritableWrapper implements Writable {
    Writable w;
    String dynKey;
    String path;

    public HiveWritableWrapper(final Writable w, final String dynKey, final String path) {
        this.w = w;
        this.dynKey = dynKey;
        this.path = path;
    }

    public Writable getWritable() {
        return this.w;
    }

    public void setWritable(final Writable w) {
        this.w = w;
    }

    public String getdynKey() {
        return this.dynKey;
    }

    public void setdynKey(final String dynKey) {
        this.dynKey = dynKey;
    }

    public String getPath() {
        return this.path;
    }

    public void setPath(final String path) {
        this.path = path;
    }

    @Override
    public void write(final DataOutput out) throws IOException {
    }

    @Override
    public void readFields(final DataInput in) throws IOException {
    }
}
