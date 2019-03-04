package com.teradata.connector.common;

import com.teradata.connector.common.exception.ConnectorException;
import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import org.apache.hadoop.io.Writable;

/**
 * @author Administrator
 */
public class ConnectorRecordSchema implements Writable {
    int length;
    int[] types;
    String[] dataTypeConverters;
    String[][] parameters;

    public ConnectorRecordSchema() {
    }

    public ConnectorRecordSchema(final int valueLength) {
        this.length = valueLength;
        this.types = new int[valueLength];
        this.dataTypeConverters = new String[valueLength];
        this.parameters = new String[valueLength][];
    }

    public int getLength() {
        return this.length;
    }

    public void setFieldType(final int index, final int fieldType) throws ConnectorException {
        if (index > this.length || index < 0) {
            throw new ConnectorException(15012);
        }
        this.types[index] = fieldType;
    }

    public int getFieldType(final int index) throws ConnectorException {
        if (index > this.length || index < 0) {
            throw new ConnectorException(15012);
        }
        return this.types[index];
    }

    public int[] getFieldTypes() {
        return this.types;
    }

    public void setDataTypeConverter(final int index, final String dataTypeConverter) throws ConnectorException {
        if (index > this.length || index < 0) {
            throw new ConnectorException(15012);
        }
        this.dataTypeConverters[index] = dataTypeConverter;
    }

    public String getDataTypeConverter(final int index) throws ConnectorException {
        if (index > this.length || index < 0) {
            throw new ConnectorException(15012);
        }
        return this.dataTypeConverters[index];
    }

    public String[] getDataTypeConverters() throws ConnectorException {
        return this.dataTypeConverters;
    }

    public void setParameters(final int index, final String[] parameter) throws ConnectorException {
        if (index > this.length || index < 0) {
            throw new ConnectorException(15012);
        }
        this.parameters[index] = parameter;
    }

    public String[] getParameters(final int index) throws ConnectorException {
        if (index > this.length || index < 0) {
            throw new ConnectorException(15012);
        }
        return this.parameters[index];
    }

    public String[][] getParametersArray() throws ConnectorException {
        return this.parameters;
    }

    @Override
    public void write(final DataOutput out) throws IOException {
        int udfCount = 0;
        out.writeInt(this.length);
        for (int i = 0; i < this.length; ++i) {
            out.writeInt(this.types[i]);
            if (this.types[i] == 1883) {
                ++udfCount;
            }
        }
        out.writeInt(udfCount);
        if (udfCount != 0) {
            for (int i = 0; i < this.length; ++i) {
                if (this.dataTypeConverters[i] != null && !this.dataTypeConverters[i].isEmpty()) {
                    out.writeInt(i);
                    out.writeUTF(this.dataTypeConverters[i]);
                    if (this.parameters[i] != null) {
                        final int parameterCount = this.parameters[i].length;
                        out.writeInt(parameterCount);
                        for (int j = 0; j < parameterCount; ++j) {
                            out.writeUTF(this.parameters[i][j]);
                        }
                    } else {
                        out.writeInt(0);
                    }
                }
            }
        }
    }

    @Override
    public void readFields(final DataInput in) throws IOException {
        int i;
        this.length = in.readInt();
        this.types = new int[this.length];
        this.dataTypeConverters = new String[this.length];
        this.parameters = new String[this.length][];
        for (i = 0; i < this.length; i++) {
            this.types[i] = in.readInt();
        }
        int udfCount = in.readInt();
        if (udfCount != 0) {
            for (i = 0; i < udfCount; i++) {
                int j = in.readInt();
                this.dataTypeConverters[j] = in.readUTF();
                int m = in.readInt();
                this.parameters[j] = new String[m];
                for (int n = 0; n < m; n++) {
                    this.parameters[j][n] = in.readUTF();
                }
            }
        }
    }
}
