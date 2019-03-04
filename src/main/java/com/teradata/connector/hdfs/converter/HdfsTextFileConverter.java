package com.teradata.connector.hdfs.converter;

import com.teradata.connector.common.ConnectorRecord;
import com.teradata.connector.common.ConnectorRecordSchema;
import com.teradata.connector.common.converter.ConnectorConverter;
import com.teradata.connector.common.converter.ConnectorDataTypeConverter;
import com.teradata.connector.common.exception.ConnectorException;
import com.teradata.connector.common.utils.ConnectorConfiguration;
import com.teradata.connector.common.utils.ConnectorSchemaUtils;

import java.util.HashMap;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.JobContext;

/**
 * @author Administrator
 */
public class HdfsTextFileConverter extends ConnectorConverter {
    protected ConnectorRecordSchema targetRecordSchema;
    protected ConnectorRecordSchema sourceRecordSchema;
    protected int columnCount;
    protected ConnectorDataTypeConverter[] dataTypeConverters;
    protected Configuration configuration;
    protected ConnectorRecord targetRecord;

    public HdfsTextFileConverter() {
        this.targetRecordSchema = null;
        this.sourceRecordSchema = null;
        this.columnCount = 0;
        this.dataTypeConverters = null;
        this.targetRecord = null;
    }

    @Override
    public void lookupConverter(final ConnectorRecordSchema sourceRecordSchema) throws ConnectorException {
        this.dataTypeConverters = super.lookupConverter(this.configuration, sourceRecordSchema);
    }

    @Override
    public ConnectorRecord convert(final ConnectorRecord sourceRecord) throws ConnectorException {
        for (int i = 0; i < this.columnCount; ++i) {
            try {
                this.targetRecord.set(i, this.dataTypeConverters[i].convert(sourceRecord.get(i)));
            } catch (ConnectorException e) {
                throw new ConnectorException(e.getMessage(), e);
            }
        }
        return this.targetRecord;
    }

    @Override
    public void initialize(final JobContext context) throws ConnectorException {
        this.configuration = context.getConfiguration();
        this.sourceRecordSchema = ConnectorSchemaUtils.recordSchemaFromString(ConnectorConfiguration.getInputConverterRecordSchema(this.configuration));
        this.targetRecordSchema = ConnectorSchemaUtils.recordSchemaFromString(ConnectorConfiguration.getOutputConverterRecordSchema(this.configuration));
        if (this.sourceRecordSchema == null || this.sourceRecordSchema.getLength() == 0) {
            throw new ConnectorException("source record schema is missing");
        }
        this.columnCount = this.sourceRecordSchema.getLength();
        this.targetRecord = new ConnectorRecord(this.columnCount);
        if (this.targetRecordSchema == null) {
            ConnectorRecordSchema s = new ConnectorRecordSchema(this.columnCount);
            for (int i = 0; i < this.columnCount; i++) {
                s.setFieldType(i, 12);
            }
            ConnectorConfiguration.setOutputConverterRecordSchema(this.configuration, ConnectorSchemaUtils.recordSchemaToString(s));
        }
    }

    @Override
    public Map<Integer, Boolean> initializeNullable() throws ConnectorException {
        Map<Integer, Boolean> nullableMap = new HashMap();
        for (int i = 0; i < this.columnCount; i++) {
            nullableMap.put(Integer.valueOf(i), Boolean.valueOf(true));
        }
        return nullableMap;
    }

    @Override
    public Map<Integer, Object> initializeDefaultValue() throws ConnectorException {
        return null;
    }

    @Override
    public Map<Integer, Object> initializeFalseDefaultValue() throws ConnectorException {
        return null;
    }

    @Override
    public Map<Integer, Object> initializeTrueDefaultValue() throws ConnectorException {
        return null;
    }
}
