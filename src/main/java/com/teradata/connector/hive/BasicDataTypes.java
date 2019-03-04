package com.teradata.connector.hive;

import org.apache.parquet.io.api.Binary;
import org.apache.parquet.io.api.RecordConsumer;

abstract class BasicDataTypes {
    public String getString() {
        throw new UnsupportedOperationException();
    }

    public int getInteger() {
        throw new UnsupportedOperationException();
    }

    public long getLong() {
        throw new UnsupportedOperationException();
    }

    public boolean getBoolean() {
        throw new UnsupportedOperationException();
    }

    public Binary getBinary() {
        throw new UnsupportedOperationException();
    }

    public Binary getInt96() {
        throw new UnsupportedOperationException();
    }

    public float getFloat() {
        throw new UnsupportedOperationException();
    }

    public double getDouble() {
        throw new UnsupportedOperationException();
    }

    public abstract void writeValue(final RecordConsumer p0);
}
