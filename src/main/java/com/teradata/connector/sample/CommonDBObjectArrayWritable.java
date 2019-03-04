package com.teradata.connector.sample;

import com.teradata.connector.common.ConnectorRecord;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;

import com.teradata.connector.common.converter.ConnectorDataTypeDefinition;
import org.apache.hadoop.mapreduce.lib.db.DBWritable;


public class CommonDBObjectArrayWritable extends ConnectorRecord implements DBWritable {
    protected int objectCount;
    protected int[] nullJdbcTypes;
    protected int[] nullJdbcScales;
    protected int nullJdbcTypeCount;
    protected Object[] nullDefaultValues;
    protected int nullDefaultValueCount;

    public CommonDBObjectArrayWritable() {
        this.nullJdbcTypes = null;
        this.nullJdbcScales = null;
        this.nullJdbcTypeCount = 0;
        this.nullDefaultValues = null;
        this.nullDefaultValueCount = 0;
        this.record = new Object[0];
        this.objectCount = 0;
    }

    public CommonDBObjectArrayWritable(final int objectCount) {
        this.nullJdbcTypes = null;
        this.nullJdbcScales = null;
        this.nullJdbcTypeCount = 0;
        this.nullDefaultValues = null;
        this.nullDefaultValueCount = 0;
        if (objectCount > 0) {
            this.record = new Object[objectCount];
            this.objectCount = objectCount;
        }
    }

    public Object[] getObjects() {
        return this.record;
    }

    public void setObjects(final Object[] objects) {
        this.record = objects;
        this.objectCount = objects.length;
    }

    @Override
    public Object get(final int position) {
        return this.record[position];
    }

    @Override
    public void set(final int position, final Object object) {
        this.record[position] = object;
    }

    public int[] getNullJdbcTypes() {
        return this.nullJdbcTypes;
    }

    public void setNullJdbcTypes(final int[] jdbcTypes) {
        if (jdbcTypes != null) {
            this.nullJdbcTypes = jdbcTypes;
            this.nullJdbcTypeCount = jdbcTypes.length;
        }
    }

    public int[] getNullJdbcScales() {
        return this.nullJdbcScales;
    }

    public void setNullJdbcScales(final int[] jdbcScales) {
        if (jdbcScales != null) {
            this.nullJdbcScales = jdbcScales;
        }
    }

    public Object[] getNullDefaultValues() {
        return this.nullDefaultValues;
    }

    public void setNullDefaultValues(final Object[] defaultValues) {
        if (defaultValues != null) {
            this.nullDefaultValues = defaultValues;
            this.nullDefaultValueCount = defaultValues.length;
        }
    }

    @Override
    public void readFields(final ResultSet resultSet) throws SQLException {
        final ResultSetMetaData metadata = resultSet.getMetaData();
        this.objectCount = metadata.getColumnCount();
        if (this.objectCount > 0) {
            this.record = new Object[this.objectCount];
        }
        for (int i = 0; i < this.objectCount; ++i) {
            this.record[i] = resultSet.getObject(i + 1);
        }
    }

    @Override
    public void write(PreparedStatement statement) throws SQLException {
        int i = 0;
        while (i < this.objectCount) {
            if (this.record[i] != null) {
                statement.setObject(i + 1, this.record[i]);
            } else if (this.nullDefaultValues == null || this.nullDefaultValueCount <= i) {
                if (this.nullJdbcTypes != null && this.nullJdbcTypeCount > i) {
                    switch (this.nullJdbcTypes[i]) {
                        case 2:
                        case ConnectorDataTypeDefinition.TYPE_DECIMAL /*3*/:
                            statement.setObject(i + 1, null, this.nullJdbcTypes[i], this.nullJdbcScales[i]);
                            break;
                        case 1111:
                        case ConnectorDataTypeDefinition.TYPE_PERIOD /*2002*/:
                        case ConnectorDataTypeDefinition.TYPE_ARRAY_TD /*2003*/:
                            statement.setObject(i + 1, null, 12);
                            break;
                        default:
                            statement.setNull(i + 1, this.nullJdbcTypes[i]);
                            break;
                    }
                }
                statement.setObject(i + 1, "");
            } else {
                statement.setObject(i + 1, this.nullDefaultValues[i]);
            }
            i++;
        }
    }


    @Override
    public void readFields(final DataInput arg0) throws IOException {
    }

    @Override
    public void write(final DataOutput arg0) throws IOException {
    }
}
