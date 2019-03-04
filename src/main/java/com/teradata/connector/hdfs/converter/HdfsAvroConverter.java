package com.teradata.connector.hdfs.converter;

import com.teradata.connector.common.ConnectorRecord;
import com.teradata.connector.common.ConnectorRecordSchema;
import com.teradata.connector.common.converter.ConnectorConverter;
import com.teradata.connector.common.converter.ConnectorDataTypeConverter;
import com.teradata.connector.common.converter.ConnectorDataTypeDefinition;
import com.teradata.connector.common.exception.ConnectorException;
import com.teradata.connector.common.exception.ConnectorException.ErrorCode;
import com.teradata.connector.common.utils.ConnectorConfiguration;
import com.teradata.connector.common.utils.ConnectorSchemaUtils;
import com.teradata.connector.hdfs.converter.HdfsAvroDataTypeConverter.AvroBinaryToBytes;
import com.teradata.connector.hdfs.converter.HdfsAvroDataTypeConverter.AvroObjectToJsonString;
import com.teradata.connector.hdfs.converter.HdfsAvroDataTypeConverter.AvroUnionToSimpleType;
import com.teradata.connector.hdfs.converter.HdfsAvroDataTypeConverter.ByteArrayToAvroBinary;
import com.teradata.connector.hdfs.converter.HdfsAvroDataTypeConverter.ClobToAvroObject;
import com.teradata.connector.hdfs.converter.HdfsAvroDataTypeConverter.JsonStringToAvroObject;
import com.teradata.connector.hdfs.converter.HdfsAvroDataTypeConverter.NonStringToAvroUnion;
import com.teradata.connector.hdfs.utils.HdfsSchemaUtils;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.avro.Schema;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.JobContext;


/**
 * @author Administrator
 */
public class HdfsAvroConverter extends ConnectorConverter {
    protected ConnectorRecordSchema targetRecordSchema;
    protected ConnectorRecordSchema sourceRecordSchema;
    protected int columnCount;
    protected ConnectorDataTypeConverter[] dataTypeConverters;
    protected ConnectorRecord targetRecord;
    protected Configuration configuration;

    public HdfsAvroConverter() {
        this.targetRecordSchema = null;
        this.sourceRecordSchema = null;
        this.columnCount = 0;
        this.dataTypeConverters = null;
        this.targetRecord = null;
    }

    @Override
    public void initialize(final JobContext context) throws ConnectorException {
        this.configuration = context.getConfiguration();
        this.sourceRecordSchema = ConnectorSchemaUtils.recordSchemaFromString(ConnectorConfiguration.getInputConverterRecordSchema(this.configuration));
        this.targetRecordSchema = ConnectorSchemaUtils.recordSchemaFromString(ConnectorConfiguration.getOutputConverterRecordSchema(this.configuration));
        if ((this.sourceRecordSchema == null || this.sourceRecordSchema.getLength() == 0) && (this.targetRecordSchema == null || this.targetRecordSchema.getLength() == 0)) {
            throw new ConnectorException("user must input source/target record schema");
        }
        if (this.sourceRecordSchema != null && (this.targetRecordSchema == null || this.targetRecordSchema.getLength() == 0)) {
            this.targetRecordSchema = this.sourceRecordSchema;
        } else if (this.targetRecordSchema != null && (this.sourceRecordSchema == null || this.sourceRecordSchema.getLength() == 0)) {
            ConnectorRecordSchema generateRecordSchema = new ConnectorRecordSchema(this.targetRecordSchema.getLength());
            for (int i = 0; i < this.targetRecordSchema.getLength(); i++) {
                generateRecordSchema.setFieldType(i, 12);
            }
            this.sourceRecordSchema = generateRecordSchema;
        }
        if (this.sourceRecordSchema.getLength() != this.targetRecordSchema.getLength()) {
            throw new ConnectorException((int) ErrorCode.COLUMN_LENGTH_OF_SOURCE_RECORD_SCHEMA_MISMATH_LENGTH_OF_TARGET_RECORD_SCHEMA);
        }
        this.columnCount = this.sourceRecordSchema.getLength();
        this.targetRecord = new ConnectorRecord(this.columnCount);
        ConnectorConfiguration.setOutputConverterRecordSchema(this.configuration, ConnectorSchemaUtils.recordSchemaToString(this.targetRecordSchema));
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

    public static ConnectorDataTypeConverter lookupSimpleTypeConverter(final JobContext context, final Schema sourceAvroSchema, final int sourceType, final int targetType) throws ConnectorException {
        Schema s = ((Schema.Field) sourceAvroSchema.getFields().get(0)).schema();
        ConnectorDataTypeConverter converter;
        switch (sourceType) {
            case HdfsAvroDataTypeDefinition.TYPE_AVRO_FIXED /*-2006*/:
            case HdfsAvroDataTypeDefinition.TYPE_AVRO_MAP /*-2004*/:
            case HdfsAvroDataTypeDefinition.TYPE_AVRO_ARRAY /*-2003*/:
            case HdfsAvroDataTypeDefinition.TYPE_AVRO_ENUM /*-2002*/:
            case -2001:
                converter = new AvroObjectToJsonString(s);
                converter.setDefaultValue(HdfsAvroDataTypeDefinition.getAvroDefaultNullJson(s));
                return converter;
            case HdfsAvroDataTypeDefinition.TYPE_AVRO_UNION /*-2005*/:
                return lookupAvroUnionToSimpleTypeConverter(context, s, targetType);
            case ConnectorDataTypeDefinition.TYPE_BINARY /*-2*/:
                converter = new AvroBinaryToBytes();
                converter.setDefaultValue(HdfsAvroDataTypeDefinition.getAvroDefaultNullJson(s));
                return converter;
            default:
                HdfsAvroConverter avroConverter = new HdfsAvroConverter();
                avroConverter.initialize(context);
                return avroConverter.lookupDataTypeConverter(sourceType, targetType, 6, 5, ConnectorDataTypeDefinition.MAX_STRING_LENGTH, avroConverter.initializeDefaultValue(), avroConverter.initializeFalseDefaultValue(), avroConverter.initializeTrueDefaultValue(), context.getConfiguration());
        }
    }

    public static HdfsAvroDataTypeConverter.AvroUnionToSimpleType lookupAvroUnionToSimpleTypeConverter(final JobContext context, final Schema unionSchema, final int targetType) throws ConnectorException {
        boolean foundConverters = false;
        List<Schema> schema = unionSchema.getTypes();
        ConnectorDataTypeConverter[] converters = new ConnectorDataTypeConverter[schema.size()];
        for (int i = 0; i < schema.size(); i++) {
            Schema s = (Schema) schema.get(i);
            try {
                int avroType = HdfsSchemaUtils.lookupHdfsAvroDatatype(s.getType().name());
                if (avroType != HdfsAvroDataTypeDefinition.TYPE_AVRO_NULL) {
                    List<Schema.Field> fields = new ArrayList();
                    fields.add(new Schema.Field("dummy_col", s, null, null));
                    Schema record = Schema.createRecord("dummy_record", null, null, false);
                    record.setFields(fields);
                    try {
                        ConnectorDataTypeConverter converter = lookupSimpleTypeConverter(context, record, avroType, targetType);
                        converters[i] = converter;
                        converter.setNullable(true);
                        foundConverters = true;
                    } catch (ConnectorException e) {
                    }
                }
            } catch (Exception e2) {
            }
        }
        if (foundConverters) {
            AvroUnionToSimpleType unionConvert = new AvroUnionToSimpleType();
            unionConvert.setConverter(converters);
            return unionConvert;
        }
        throw new ConnectorException((int) ErrorCode.FIELD_DATATYPE_CONVERSION_UNSUPPORTED, " for Avro type");

    }

    public static boolean isComplexType(final int type) {
        if (HdfsAvroDataTypeDefinition.TYPE_AVRO_UNION == type || HdfsAvroDataTypeDefinition.TYPE_AVRO_MAP == type || HdfsAvroDataTypeDefinition.TYPE_AVRO_ARRAY == type || -2001 == type || HdfsAvroDataTypeDefinition.TYPE_AVRO_ENUM == type || HdfsAvroDataTypeDefinition.TYPE_AVRO_FIXED == type || HdfsAvroDataTypeDefinition.TYPE_AVRO_NULL == type) {
            return true;
        }
        return false;
    }

    public static ConnectorDataTypeConverter lookupUnionConverter(final JobContext context, final Schema targetAvroSchema, final int sourceType, final int targetType) throws ConnectorException {
        Schema s = ((Schema.Field) targetAvroSchema.getFields().get(0)).schema();
        if (targetType == HdfsAvroDataTypeDefinition.TYPE_AVRO_UNION) {
            return lookupSimpleTypeToAvroUnionConverter(context, s, sourceType);
        }
        if (isComplexType(targetType)) {
            if (sourceType == ConnectorDataTypeDefinition.TYPE_CLOB) {
                return new ClobToAvroObject(s);
            }
            return new JsonStringToAvroObject(s);
        } else if (targetType == -2) {
            return new ByteArrayToAvroBinary();
        } else {
            HdfsAvroConverter avroConverter = new HdfsAvroConverter();
            avroConverter.initialize(context);
            return avroConverter.lookupDataTypeConverter(sourceType, targetType, 6, 5, ConnectorDataTypeDefinition.MAX_STRING_LENGTH, avroConverter.initializeDefaultValue(), avroConverter.initializeFalseDefaultValue(), avroConverter.initializeTrueDefaultValue(), context.getConfiguration());
        }
    }

    public static HdfsAvroDataTypeConverter.NonStringToAvroUnion lookupSimpleTypeToAvroUnionConverter(final JobContext context, final Schema unionSchema, final int sourceType) throws ConnectorException {
        boolean foundConverters = false;
        List<Schema> schema = unionSchema.getTypes();
        ConnectorDataTypeConverter[] converters = new ConnectorDataTypeConverter[schema.size()];
        for (int i = 0; i < schema.size(); i++) {
            Schema s = (Schema) schema.get(i);
            int avroType = HdfsSchemaUtils.lookupHdfsAvroDatatype(s.getType().name());
            if (avroType != HdfsAvroDataTypeDefinition.TYPE_AVRO_NULL) {
                List<Schema.Field> fields = new ArrayList();
                fields.add(new Schema.Field("dummy_col", s, null, null));
                Schema record = Schema.createRecord("dummy_record", null, null, false);
                record.setFields(fields);
                try {
                    converters[i] = lookupUnionConverter(context, record, sourceType, avroType);
                    foundConverters = true;
                } catch (ConnectorException e) {
                }
            }
        }
        if (foundConverters) {
            NonStringToAvroUnion unionConvert = new NonStringToAvroUnion();
            unionConvert.setConverter(converters);
            return unionConvert;
        }
        throw new ConnectorException((int) ErrorCode.FIELD_DATATYPE_CONVERSION_UNSUPPORTED, " for Avro type");
    }

    @Override
    public Map<Integer, Boolean> initializeNullable() throws ConnectorException {
        final Map<Integer, Boolean> nullableMap = new HashMap<Integer, Boolean>();
        for (int i = 0; i < this.columnCount; ++i) {
            nullableMap.put(i, true);
        }
        return nullableMap;
    }

    @Override
    public Map<Integer, Object> initializeDefaultValue() throws ConnectorException {
        Map<Integer, Object> defValMap = new HashMap();
        defValMap.put(Integer.valueOf(4), HdfsAvroDataTypeDefinition.INTEGER_NULL_VALUE);
        defValMap.put(Integer.valueOf(-5), HdfsAvroDataTypeDefinition.BIGINT_NULL_VALUE);
        defValMap.put(Integer.valueOf(5), HdfsAvroDataTypeDefinition.SMALLINT_NULL_VALUE);
        defValMap.put(Integer.valueOf(-6), HdfsAvroDataTypeDefinition.TINYINT_NULL_VALUE);
        defValMap.put(Integer.valueOf(3), HdfsAvroDataTypeDefinition.BIGDECIMAL_NULL_VALUE);
        defValMap.put(Integer.valueOf(91), HdfsAvroDataTypeDefinition.DATE_NULL_VALUE);
        defValMap.put(Integer.valueOf(92), HdfsAvroDataTypeDefinition.TIME_NULL_VALUE);
        defValMap.put(Integer.valueOf(93), HdfsAvroDataTypeDefinition.TIMESTAMP_NULL_VALUE);
        defValMap.put(Integer.valueOf(8), HdfsAvroDataTypeDefinition.DOUBLE_NULL_VALUE);
        defValMap.put(Integer.valueOf(16), HdfsAvroDataTypeDefinition.BOOLEAN_NULL_VALUE);
        defValMap.put(Integer.valueOf(-2), HdfsAvroDataTypeDefinition.BYTE_NULL_VALUE);
        defValMap.put(Integer.valueOf(6), HdfsAvroDataTypeDefinition.FLOAT_NULL_VALUE);
        defValMap.put(Integer.valueOf(12), "");
        defValMap.put(Integer.valueOf(ConnectorDataTypeDefinition.TYPE_PERIOD), "");
        defValMap.put(Integer.valueOf(1111), "");
        defValMap.put(Integer.valueOf(ConnectorDataTypeDefinition.TYPE_ARRAY_TD), "");
        return defValMap;

    }

    @Override
    public Map<Integer, Object> initializeFalseDefaultValue() throws ConnectorException {
        Map<Integer, Object> defVal = new HashMap();
        defVal.put(Integer.valueOf(4), HdfsAvroDataTypeDefinition.INTEGER_FALSE_VALUE);
        defVal.put(Integer.valueOf(-5), HdfsAvroDataTypeDefinition.LONG_FALSE_VALUE);
        defVal.put(Integer.valueOf(5), HdfsAvroDataTypeDefinition.SHORT_FALSE_VALUE);
        defVal.put(Integer.valueOf(6), HdfsAvroDataTypeDefinition.FLOAT_FALSE_VALUE);
        defVal.put(Integer.valueOf(8), HdfsAvroDataTypeDefinition.DOUBLE_FALSE_VALUE);
        defVal.put(Integer.valueOf(7), HdfsAvroDataTypeDefinition.DOUBLE_FALSE_VALUE);
        defVal.put(Integer.valueOf(2), HdfsAvroDataTypeDefinition.BIGDECIMAL_FALSE_VALUE);
        defVal.put(Integer.valueOf(3), HdfsAvroDataTypeDefinition.BIGDECIMAL_FALSE_VALUE);
        defVal.put(Integer.valueOf(-2), HdfsAvroDataTypeDefinition.BYTE_FALSE_VALUE);
        return defVal;

    }

    @Override
    public Map<Integer, Object> initializeTrueDefaultValue() throws ConnectorException {
        Map<Integer, Object> defVal = new HashMap();
        defVal.put(Integer.valueOf(4), HdfsAvroDataTypeDefinition.INTEGER_TRUE_VALUE);
        defVal.put(Integer.valueOf(-5), HdfsAvroDataTypeDefinition.LONG_TRUE_VALUE);
        defVal.put(Integer.valueOf(5), HdfsAvroDataTypeDefinition.SHORT_TRUE_VALUE);
        defVal.put(Integer.valueOf(6), HdfsAvroDataTypeDefinition.FLOAT_TRUE_VALUE);
        defVal.put(Integer.valueOf(8), HdfsAvroDataTypeDefinition.DOUBLE_TRUE_VALUE);
        defVal.put(Integer.valueOf(7), HdfsAvroDataTypeDefinition.DOUBLE_TRUE_VALUE);
        defVal.put(Integer.valueOf(2), HdfsAvroDataTypeDefinition.BIGDECIMAL_TRUE_VALUE);
        defVal.put(Integer.valueOf(3), HdfsAvroDataTypeDefinition.BIGDECIMAL_TRUE_VALUE);
        defVal.put(Integer.valueOf(-2), HdfsAvroDataTypeDefinition.BYTE_TRUE_VALUE);
        return defVal;
    }
}
