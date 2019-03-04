package com.teradata.connector.common;

import com.teradata.connector.common.converter.ConnectorDataWritable;
import com.teradata.connector.common.exception.ConnectorException;
import com.teradata.connector.common.utils.ConnectorSchemaUtils;
import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.List;
import org.apache.hadoop.io.Writable;

/**
 * @author Administrator
 */
public class ConnectorRecord implements Writable {
    protected Object[] record;

    public ConnectorRecord(final int objectCount) {
        this.record = null;
        this.record = new Object[objectCount];
    }

    public ConnectorRecord() {
        this.record = null;
        this.record = new Object[0];
    }

    public Object[] getAllObject() {
        Object[] result;
        if (this.record.length == 0) {
            result = new Object[0];
        } else {
            result = new Object[this.record.length];
            int i = 0;
            for (final Object cell : this.record) {
                result[i++] = cell;
            }
        }
        return result;
    }

    public Writable[] getAllWritableObject(final List<String> lsColumns) throws IOException {
        Writable[] result;
        if (this.record.length == 0) {
            result = new Writable[0];
        } else {
            result = new Writable[this.record.length];
            int i = 0;
            final List<String> columnTypes = ConnectorSchemaUtils.parseColumnTypes(lsColumns);
            for (final Object cell : this.record) {
                int type = ConnectorSchemaUtils.getGenericObjectType(cell);
                if (type == 1884) {
                    type = ConnectorSchemaUtils.convertNullDataTypeByName(columnTypes.get(i));
                }
                final Writable w = ConnectorDataWritable.connectorWritableFactoryParquet(type);
                ConnectorDataWritable.setWritableValueParquet(w, this.record[i], type);
                result[i++] = w;
            }
        }
        return result;
    }

    public void set(final int index, final Object object) throws ConnectorException {
        if (index >= this.record.length || index < 0) {
            throw new ConnectorException(15012);
        }
        this.record[index] = object;
    }

    public Object get(final int index) throws ConnectorException {
        if (index >= this.record.length || index < 0) {
            throw new ConnectorException(15012);
        }
        return this.record[index];
    }

    @Override
    public void write(final DataOutput out) throws IOException {
        out.writeInt(this.record.length);
        for (int i = 0; i < this.record.length; ++i) {
            int type = ConnectorSchemaUtils.getGenericObjectType(this.record[i]);
            final Writable w = ConnectorDataWritable.connectorWritableFactory(type);
            ConnectorDataWritable.setWritableValue(w, this.record[i], type);
            if (type == 2005) {
                type = 12;
            } else if (type == 2004) {
                type = -2;
            }
            out.writeInt(type);
            w.write(out);
        }
    }

    @Override
    public void readFields(final DataInput in) throws IOException {
        final int recordLen = in.readInt();
        this.record = new Object[recordLen];
        for (int i = 0; i < this.record.length; ++i) {
            final int type = in.readInt();
            final Writable w = ConnectorDataWritable.connectorWritableFactory(type);
            w.readFields(in);
            this.record[i] = ConnectorDataWritable.getWritableValue(w, type);
        }
    }
}
