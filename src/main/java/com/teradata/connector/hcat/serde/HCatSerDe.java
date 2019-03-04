package com.teradata.connector.hcat.serde;

import com.teradata.connector.common.ConnectorRecord;
import com.teradata.connector.common.ConnectorSerDe;
import com.teradata.connector.common.converter.ConnectorDataTypeConverter;
import com.teradata.connector.common.exception.ConnectorException;
import com.teradata.connector.common.utils.ConnectorConfiguration;
import com.teradata.connector.hcat.utils.HCatPlugInConfiguration;
import com.teradata.connector.hcat.utils.HCatSchemaUtils;
import com.teradata.connector.hive.utils.HiveSchemaUtils;

import java.lang.reflect.Constructor;
import java.lang.reflect.Method;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.JobContext;


@Deprecated
public class HCatSerDe implements ConnectorSerDe {
    Class<?> DefaultHCatRecordClass = null;
    Constructor<?> DefaultHCatRecordConstructor = null;
    Method DefaultHCatRecordGetMethod = null;
    Object DefaultHCatRecordInstance = null;
    Method DefaultHCatRecordSetMethod = null;
    protected Configuration configuration;
    private ConnectorDataTypeConverter[] deserConverter;
    protected int fieldCount;
    String[] inputFieldNamesArray = null;
    int[] inputSchemaDataTypes;
    int[] mappings = null;
    String[] outputFieldNamesArray = null;
    int[] outputSchemaDataTypes;
    private ConnectorDataTypeConverter[] serConverter;
    protected ConnectorRecord sourceConnectorRecord;
    String[] sourceFieldNamesArray = null;
    String[] targetFieldNamesArray = null;


    @Override
    public void initialize(final JobContext context, final ConnectorConfiguration.direction direction) throws ConnectorException {
        Configuration configuration = context.getConfiguration();
        configuration = context.getConfiguration();
        if (direction == ConnectorConfiguration.direction.input) {
            this.inputFieldNamesArray = HCatPlugInConfiguration.getInputFieldNamesArray(configuration);
            this.sourceFieldNamesArray = HCatPlugInConfiguration.getSourceSchemaFieldNamesArray(configuration);
            this.mappings = HCatSchemaUtils.getColumnMapping(this.sourceFieldNamesArray, this.inputFieldNamesArray);
            this.fieldCount = this.inputFieldNamesArray.length;
            this.deserConverter = HiveSchemaUtils.initializeHiveTypeEncoder(HCatPlugInConfiguration.getInputTableSchema(configuration));
            this.inputSchemaDataTypes = HiveSchemaUtils.lookupHiveDataTypeByName(HCatPlugInConfiguration.getInputTableFieldTypes(configuration));
        } else {
            this.outputFieldNamesArray = HCatPlugInConfiguration.getOutputFieldNamesArray(configuration);
            this.targetFieldNamesArray = HCatPlugInConfiguration.getTargetSchemaFieldNamesArray(configuration);
            this.mappings = HCatSchemaUtils.getColumnMapping(this.targetFieldNamesArray, this.outputFieldNamesArray);
            this.fieldCount = this.outputFieldNamesArray.length;
            this.serConverter = HiveSchemaUtils.initializeHiveTypeDecoder(HCatPlugInConfiguration.getOutputTableSchema(configuration));
            this.outputSchemaDataTypes = HiveSchemaUtils.lookupHiveDataTypeByName(HCatPlugInConfiguration.getOutputTableFieldTypes(configuration));
        }
        try {
            this.DefaultHCatRecordClass = Class.forName("org.apache.hive.hcatalog.data.DefaultHCatRecord");
        } catch (ClassNotFoundException e) {
            try {
                this.DefaultHCatRecordClass = Class.forName("org.apache.hcatalog.data.DefaultHCatRecord");
            } catch (ClassNotFoundException e3) {
                throw new ConnectorException(e.getMessage(), e);
            }
        }
        try {
            this.DefaultHCatRecordConstructor = this.DefaultHCatRecordClass.getConstructor(Integer.TYPE);
            this.DefaultHCatRecordSetMethod = this.DefaultHCatRecordClass.getMethod("set", Integer.TYPE, Object.class);
            this.DefaultHCatRecordGetMethod = this.DefaultHCatRecordClass.getMethod("get", Integer.TYPE);
        } catch (Exception e2) {
            throw new ConnectorException(e2.getMessage(), e2);
        }
    }

    @Override
    public Writable serialize(final ConnectorRecord connectorRecord) throws ConnectorException {
        try {
            this.DefaultHCatRecordInstance = this.DefaultHCatRecordConstructor.newInstance(this.targetFieldNamesArray.length);
        } catch (Exception e) {
            throw new ConnectorException(e.getMessage());
        }
        for (int i = 0; i < this.fieldCount; ++i) {
            final Object object = this.hcatTypeDecoder(this.mappings[i], connectorRecord.get(i));
            try {
                this.DefaultHCatRecordSetMethod.invoke(this.DefaultHCatRecordInstance, this.mappings[i], object);
            } catch (Exception e2) {
                throw new ConnectorException(e2.getMessage());
            }
        }
        return (Writable) this.DefaultHCatRecordInstance;
    }

    @Override
    public ConnectorRecord deserialize(final Writable writable) throws ConnectorException {
        this.DefaultHCatRecordInstance = writable;
        this.sourceConnectorRecord = new ConnectorRecord(this.fieldCount);
        for (int i = 0; i < this.fieldCount; ++i) {
            Object object = null;
            try {
                object = this.hcatTypeEncoder(this.mappings[i], this.DefaultHCatRecordGetMethod.invoke(this.DefaultHCatRecordInstance, this.mappings[i]));
            } catch (Exception e) {
                throw new ConnectorException(e.getMessage());
            }
            this.sourceConnectorRecord.set(i, object);
        }
        return this.sourceConnectorRecord;
    }

    private Object hcatTypeEncoder(final int index, final Object o) {
        final int type = this.inputSchemaDataTypes[index];
        if (HiveSchemaUtils.isHiveComplexType(type)) {
            return this.deserConverter[index].convert(o);
        }
        return o;
    }

    private Object hcatTypeDecoder(final int index, final Object o) {
        final int type = this.outputSchemaDataTypes[index];
        if (HiveSchemaUtils.isHiveComplexType(type)) {
            return this.serConverter[index].convert(o);
        }
        return o;
    }
}
