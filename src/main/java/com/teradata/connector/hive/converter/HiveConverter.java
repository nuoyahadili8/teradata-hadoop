package com.teradata.connector.hive.converter;

import com.teradata.connector.common.ConnectorRecord;
import com.teradata.connector.common.ConnectorRecordSchema;
import com.teradata.connector.common.converter.ConnectorConverter;
import com.teradata.connector.common.converter.ConnectorDataTypeConverter;
import com.teradata.connector.common.exception.ConnectorException;
import com.teradata.connector.common.utils.ConnectorConfiguration;
import com.teradata.connector.common.utils.ConnectorSchemaUtils;
import com.teradata.connector.hive.utils.HivePlugInConfiguration;
import com.teradata.connector.hive.utils.HiveSchemaUtils;
import java.util.HashMap;
import java.util.Map;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.JobContext;

/**
 * @author Administrator
 */
public class HiveConverter extends ConnectorConverter {
    protected ConnectorDataTypeConverter[] converters;
    protected ConnectorDataTypeConverter[] partitionColumnConverters;
    protected ConnectorRecordSchema sourceRecordSchema;
    protected ConnectorRecordSchema targetRecordSchema;
    protected int[] sourceTypes;
    protected int[] targetTypes;
    protected int[] targetPrecisions;
    protected int[] targetScales;
    protected Configuration configuration;
    protected ConnectorRecord r;

    public HiveConverter() {
        this.converters = null;
        this.partitionColumnConverters = null;
    }

    @Override
    public void initialize(final JobContext context) throws ConnectorException {
        this.configuration = context.getConfiguration();
        this.sourceRecordSchema = ConnectorSchemaUtils.recordSchemaFromString(ConnectorConfiguration.getInputConverterRecordSchema(this.configuration));
        this.targetRecordSchema = ConnectorSchemaUtils.recordSchemaFromString(ConnectorConfiguration.getOutputConverterRecordSchema(this.configuration));
        if (this.targetRecordSchema == null || this.targetRecordSchema.getLength() == 0) {
            throw new ConnectorException(14018);
        }
        if (this.sourceRecordSchema != null && this.sourceRecordSchema.getLength() != 0 && this.sourceRecordSchema.getLength() != this.targetRecordSchema.getLength()) {
            throw new ConnectorException(14017);
        }
        if (this.sourceRecordSchema == null) {
            this.sourceRecordSchema = new ConnectorRecordSchema(this.targetRecordSchema.getLength());
            for (int i = 0; i < this.targetRecordSchema.getLength(); ++i) {
                this.sourceRecordSchema.setFieldType(i, 12);
            }
        }
        this.sourceTypes = this.sourceRecordSchema.getFieldTypes();
        this.targetTypes = this.targetRecordSchema.getFieldTypes();
        if (this.sourceTypes.length != this.targetTypes.length) {
            throw new RuntimeException(new ConnectorException(14009));
        }
        this.r = new ConnectorRecord(this.targetTypes.length);
        ConnectorConfiguration.setOutputConverterRecordSchema(this.configuration, ConnectorSchemaUtils.recordSchemaToString(this.targetRecordSchema));
        this.targetPrecisions = HiveSchemaUtils.lookupHiveDataTypePrecisions(HivePlugInConfiguration.getOutputTableFieldTypes(this.configuration));
        this.targetScales = HiveSchemaUtils.lookupHiveDataTypeScales(HivePlugInConfiguration.getOutputTableFieldTypes(this.configuration));
    }

    @Override
    public void lookupConverter(final ConnectorRecordSchema sourceRecordSchema) throws ConnectorException {
        this.converters = super.lookupConverter(this.configuration, sourceRecordSchema);
    }

    @Override
    public ConnectorRecord convert(final ConnectorRecord record) {
        for (int i = 0; i < this.converters.length; ++i) {
            try {
                final Object object = record.get(i);
                this.r.set(i, this.converters[i].convert(object));
            } catch (ConnectorException e) {
                e.printStackTrace();
            }
        }
        return this.r;
    }

    @Override
    public Map<Integer, Boolean> initializeNullable() throws ConnectorException {
        final Map<Integer, Boolean> nullableMap = new HashMap<Integer, Boolean>();
        for (int i = 0; i < this.sourceTypes.length; ++i) {
            nullableMap.put(i, true);
        }
        return nullableMap;
    }

    @Override
    public int[] initializePrecision() throws ConnectorException {
        return this.targetPrecisions;
    }

    @Override
    public int[] initializeScale() throws ConnectorException {
        return this.targetScales;
    }

    @Override
    public Map<Integer, Object> initializeDefaultValue() throws ConnectorException {
        final Map<Integer, Object> defValMap = new HashMap<Integer, Object>();
        defValMap.put(4, HiveDataTypeDefinition.INTEGER_NULL_VALUE);
        defValMap.put(-5, HiveDataTypeDefinition.BIGINT_NULL_VALUE);
        defValMap.put(5, HiveDataTypeDefinition.SMALLINT_NULL_VALUE);
        defValMap.put(-6, HiveDataTypeDefinition.TINYINT_NULL_VALUE);
        defValMap.put(3, HiveDataTypeDefinition.BIGDECIMAL_NULL_VALUE);
        defValMap.put(91, HiveDataTypeDefinition.DATE_NULL_VALUE);
        defValMap.put(92, HiveDataTypeDefinition.TIME_NULL_VALUE);
        defValMap.put(93, HiveDataTypeDefinition.TIMESTAMP_NULL_VALUE);
        defValMap.put(8, HiveDataTypeDefinition.DOUBLE_NULL_VALUE);
        defValMap.put(16, HiveDataTypeDefinition.BOOLEAN_NULL_VALUE);
        defValMap.put(-2, HiveDataTypeDefinition.BYTE_NULL_VALUE);
        defValMap.put(6, HiveDataTypeDefinition.FLOAT_NULL_VALUE);
        defValMap.put(12, "");
        defValMap.put(2002, "");
        defValMap.put(1111, "");
        defValMap.put(2003, "");
        return defValMap;
    }

    @Override
    public Map<Integer, Object> initializeFalseDefaultValue() throws ConnectorException {
        final Map<Integer, Object> defVal = new HashMap<Integer, Object>();
        defVal.put(4, HiveDataTypeDefinition.INTEGER_FALSE_VALUE);
        defVal.put(-5, HiveDataTypeDefinition.LONG_FALSE_VALUE);
        defVal.put(5, HiveDataTypeDefinition.SHORT_FALSE_VALUE);
        defVal.put(6, HiveDataTypeDefinition.FLOAT_FALSE_VALUE);
        defVal.put(8, HiveDataTypeDefinition.DOUBLE_FALSE_VALUE);
        defVal.put(7, HiveDataTypeDefinition.DOUBLE_FALSE_VALUE);
        defVal.put(2, HiveDataTypeDefinition.BIGDECIMAL_FALSE_VALUE);
        defVal.put(3, HiveDataTypeDefinition.BIGDECIMAL_FALSE_VALUE);
        defVal.put(-2, HiveDataTypeDefinition.BYTE_FALSE_VALUE);
        return defVal;
    }

    @Override
    public Map<Integer, Object> initializeTrueDefaultValue() throws ConnectorException {
        final Map<Integer, Object> defVal = new HashMap<Integer, Object>();
        defVal.put(4, HiveDataTypeDefinition.INTEGER_TRUE_VALUE);
        defVal.put(-5, HiveDataTypeDefinition.LONG_TRUE_VALUE);
        defVal.put(5, HiveDataTypeDefinition.SHORT_TRUE_VALUE);
        defVal.put(6, HiveDataTypeDefinition.FLOAT_TRUE_VALUE);
        defVal.put(8, HiveDataTypeDefinition.DOUBLE_TRUE_VALUE);
        defVal.put(7, HiveDataTypeDefinition.DOUBLE_TRUE_VALUE);
        defVal.put(2, HiveDataTypeDefinition.BIGDECIMAL_TRUE_VALUE);
        defVal.put(3, HiveDataTypeDefinition.BIGDECIMAL_TRUE_VALUE);
        defVal.put(-2, HiveDataTypeDefinition.BYTE_TRUE_VALUE);
        return defVal;
    }
}
