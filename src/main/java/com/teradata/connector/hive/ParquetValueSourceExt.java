package com.teradata.connector.hive;

import org.apache.hadoop.io.ObjectWritable;
import org.apache.parquet.io.api.Binary;
import org.apache.parquet.schema.GroupType;

abstract class ParquetValueSourceExt extends ObjectWritable {
    public int getFieldRepetitionCount(final String field) {
        return this.getFieldRepetitionCount(this.getType().getFieldIndex(field));
    }

    public ParquetValueSourceExt getGroup(final String field, final int index) {
        return this.getGroup(this.getType().getFieldIndex(field), index);
    }

    public String getString(final String field, final int index) {
        return this.getString(this.getType().getFieldIndex(field), index);
    }

    public int getInteger(final String field, final int index) {
        return this.getInteger(this.getType().getFieldIndex(field), index);
    }

    public long getLong(final String field, final int index) {
        return this.getLong(this.getType().getFieldIndex(field), index);
    }

    public double getDouble(final String field, final int index) {
        return this.getDouble(this.getType().getFieldIndex(field), index);
    }

    public float getFloat(final String field, final int index) {
        return this.getFloat(this.getType().getFieldIndex(field), index);
    }

    public boolean getBoolean(final String field, final int index) {
        return this.getBoolean(this.getType().getFieldIndex(field), index);
    }

    public Binary getBinary(final String field, final int index) {
        return this.getBinary(this.getType().getFieldIndex(field), index);
    }

    public Binary getInt96(final String field, final int index) {
        return this.getInt96(this.getType().getFieldIndex(field), index);
    }

    public abstract int getFieldRepetitionCount(final int p0);

    public abstract ParquetValueSourceExt getGroup(final int p0, final int p1);

    public abstract String getString(final int p0, final int p1);

    public abstract int getInteger(final int p0, final int p1);

    public abstract long getLong(final int p0, final int p1);

    public abstract double getDouble(final int p0, final int p1);

    public abstract float getFloat(final int p0, final int p1);

    public abstract boolean getBoolean(final int p0, final int p1);

    public abstract Binary getBinary(final int p0, final int p1);

    public abstract Binary getInt96(final int p0, final int p1);

    public abstract String getValueToString(final int p0, final int p1);

    public abstract GroupType getType();
}
