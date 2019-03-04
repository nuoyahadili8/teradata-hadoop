package com.teradata.connector.hive;

import org.apache.parquet.Log;
import org.apache.parquet.io.api.Binary;
import org.apache.parquet.io.api.RecordConsumer;


abstract class ParquetExt extends ParquetValueSourceExt {
    private static final Log logger;
    private static final boolean DEBUG;

    public void add(final String field, final int value) {
        this.add(this.getType().getFieldIndex(field), value);
    }

    public void add(final String field, final long value) {
        this.add(this.getType().getFieldIndex(field), value);
    }

    public void add(final String field, final float value) {
        this.add(this.getType().getFieldIndex(field), value);
    }

    public void add(final String field, final double value) {
        this.add(this.getType().getFieldIndex(field), value);
    }

    public void add(final String field, final String value) {
        this.add(this.getType().getFieldIndex(field), value);
    }

    public void add(final String field, final ParquetNanoTime value) {
        this.add(this.getType().getFieldIndex(field), value);
    }

    public void add(final String field, final boolean value) {
        this.add(this.getType().getFieldIndex(field), value);
    }

    public void add(final String field, final Binary value) {
        this.add(this.getType().getFieldIndex(field), value);
    }

    public void add(final String field, final ParquetExt value) {
        this.add(this.getType().getFieldIndex(field), value);
    }

    public ParquetExt addGroup(final String field) {
        if (ParquetExt.DEBUG) {
            ParquetExt.logger.debug((Object) ("add group " + field + " to " + this.getType().getName()));
        }
        return this.addGroup(this.getType().getFieldIndex(field));
    }

    @Override
    public ParquetExt getGroup(final String field, final int index) {
        return this.getGroup(this.getType().getFieldIndex(field), index);
    }

    public abstract void add(final int p0, final int p1);

    public abstract void add(final int p0, final long p1);

    public abstract void add(final int p0, final String p1);

    public abstract void add(final int p0, final boolean p1);

    public abstract void add(final int p0, final ParquetNanoTime p1);

    public abstract void add(final int p0, final Binary p1);

    public abstract void add(final int p0, final float p1);

    public abstract void add(final int p0, final double p1);

    public abstract void add(final int p0, final ParquetExt p1);

    public abstract ParquetExt addGroup(final int p0);

    @Override
    public abstract ParquetExt getGroup(final int p0, final int p1);

    public ParquetExt asGroup() {
        return this;
    }

    public ParquetExt append(final String fieldName, final int value) {
        this.add(fieldName, value);
        return this;
    }

    public ParquetExt append(final String fieldName, final float value) {
        this.add(fieldName, value);
        return this;
    }

    public ParquetExt append(final String fieldName, final double value) {
        this.add(fieldName, value);
        return this;
    }

    public ParquetExt append(final String fieldName, final long value) {
        this.add(fieldName, value);
        return this;
    }

    public ParquetExt append(final String fieldName, final ParquetNanoTime value) {
        this.add(fieldName, value);
        return this;
    }

    public ParquetExt append(final String fieldName, final String value) {
        this.add(fieldName, Binary.fromString(value));
        return this;
    }

    public ParquetExt append(final String fieldName, final boolean value) {
        this.add(fieldName, value);
        return this;
    }

    public ParquetExt append(final String fieldName, final Binary value) {
        this.add(fieldName, value);
        return this;
    }

    public abstract void writeValue(final int p0, final int p1, final RecordConsumer p2);

    static {
        logger = Log.getLog((Class) ParquetExt.class);
        DEBUG = Log.DEBUG;
    }
}
