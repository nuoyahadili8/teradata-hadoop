package com.teradata.connector.common;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.Writable;


/**
 * @author Administrator
 */
public class ConnectorPartitionedWritable implements Writable {
    private String partitionPath;
    private Writable record;

    public String getPartitionPath() {
        return this.partitionPath;
    }

    public void setPartitionPath(final String partitionPath) {
        this.partitionPath = partitionPath;
    }

    @Override
    public void write(final DataOutput out) throws IOException {
    }

    @Override
    public void readFields(final DataInput in) throws IOException {
    }

    public Writable getRecord() {
        return this.record;
    }

    public void setRecord(final Writable record) {
        this.record = record;
    }
}
