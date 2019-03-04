package com.teradata.connector.hive;

import org.apache.parquet.io.api.Binary;
import org.apache.parquet.io.api.RecordConsumer;

class ParquetInt96Value extends BasicDataTypes {
    private final Binary value;

    public ParquetInt96Value(final Binary value) {
        this.value = value;
    }

    @Override
    public Binary getInt96() {
        return this.value;
    }

    @Override
    public void writeValue(final RecordConsumer recordConsumer) {
        recordConsumer.addBinary(this.value);
    }

    @Override
    public String toString() {
        return "ParquetInt96Value{" + String.valueOf(this.value) + "}";
    }
}
