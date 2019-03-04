package com.teradata.connector.sample.plugin.converter;

import com.teradata.connector.common.ConnectorRecord;
import com.teradata.connector.common.ConnectorRecordSchema;
import com.teradata.connector.common.converter.ConnectorConverter;
import com.teradata.connector.common.converter.ConnectorDataTypeConverter;
import com.teradata.connector.common.converter.ConnectorDataTypeConverter.BigDecimalToBigDecimal;
import com.teradata.connector.common.converter.ConnectorDataTypeConverter.BigDecimalToBigDecimalWithScale;
import com.teradata.connector.common.converter.ConnectorDataTypeConverter.BigDecimalToDouble;
import com.teradata.connector.common.converter.ConnectorDataTypeConverter.BigDecimalToInteger;
import com.teradata.connector.common.converter.ConnectorDataTypeConverter.BigDecimalToLong;
import com.teradata.connector.common.converter.ConnectorDataTypeConverter.BigDecimalToShort;
import com.teradata.connector.common.converter.ConnectorDataTypeConverter.BigDecimalToString;
import com.teradata.connector.common.converter.ConnectorDataTypeConverter.BigDecimalToTinyInt;
import com.teradata.connector.common.converter.ConnectorDataTypeConverter.BinaryToBinary;
import com.teradata.connector.common.converter.ConnectorDataTypeConverter.BinaryToString;
import com.teradata.connector.common.converter.ConnectorDataTypeConverter.BlobToBinary;
import com.teradata.connector.common.converter.ConnectorDataTypeConverter.BlobToString;
import com.teradata.connector.common.converter.ConnectorDataTypeConverter.BooleanToBigDecimal;
import com.teradata.connector.common.converter.ConnectorDataTypeConverter.BooleanToBigDecimalWithScale;
import com.teradata.connector.common.converter.ConnectorDataTypeConverter.BooleanToDouble;
import com.teradata.connector.common.converter.ConnectorDataTypeConverter.BooleanToInteger;
import com.teradata.connector.common.converter.ConnectorDataTypeConverter.BooleanToLong;
import com.teradata.connector.common.converter.ConnectorDataTypeConverter.BooleanToShort;
import com.teradata.connector.common.converter.ConnectorDataTypeConverter.BooleanToString;
import com.teradata.connector.common.converter.ConnectorDataTypeConverter.BooleanToTinyInt;
import com.teradata.connector.common.converter.ConnectorDataTypeConverter.ClobToString;
import com.teradata.connector.common.converter.ConnectorDataTypeConverter.DateToDate;
import com.teradata.connector.common.converter.ConnectorDataTypeConverter.DateToString;
import com.teradata.connector.common.converter.ConnectorDataTypeConverter.DateToTimestamp;
import com.teradata.connector.common.converter.ConnectorDataTypeConverter.DoubleToBigDecimal;
import com.teradata.connector.common.converter.ConnectorDataTypeConverter.DoubleToBigDecimalWithScale;
import com.teradata.connector.common.converter.ConnectorDataTypeConverter.DoubleToDouble;
import com.teradata.connector.common.converter.ConnectorDataTypeConverter.DoubleToInteger;
import com.teradata.connector.common.converter.ConnectorDataTypeConverter.DoubleToLong;
import com.teradata.connector.common.converter.ConnectorDataTypeConverter.DoubleToShort;
import com.teradata.connector.common.converter.ConnectorDataTypeConverter.DoubleToString;
import com.teradata.connector.common.converter.ConnectorDataTypeConverter.DoubleToTinyInt;
import com.teradata.connector.common.converter.ConnectorDataTypeConverter.FloatToBigDecimal;
import com.teradata.connector.common.converter.ConnectorDataTypeConverter.FloatToBigDecimalWithScale;
import com.teradata.connector.common.converter.ConnectorDataTypeConverter.FloatToDouble;
import com.teradata.connector.common.converter.ConnectorDataTypeConverter.FloatToInteger;
import com.teradata.connector.common.converter.ConnectorDataTypeConverter.FloatToLong;
import com.teradata.connector.common.converter.ConnectorDataTypeConverter.FloatToShort;
import com.teradata.connector.common.converter.ConnectorDataTypeConverter.FloatToString;
import com.teradata.connector.common.converter.ConnectorDataTypeConverter.FloatToTinyInt;
import com.teradata.connector.common.converter.ConnectorDataTypeConverter.IntegerToBigDecimal;
import com.teradata.connector.common.converter.ConnectorDataTypeConverter.IntegerToBigDecimalWithScale;
import com.teradata.connector.common.converter.ConnectorDataTypeConverter.IntegerToBoolean;
import com.teradata.connector.common.converter.ConnectorDataTypeConverter.IntegerToDouble;
import com.teradata.connector.common.converter.ConnectorDataTypeConverter.IntegerToInteger;
import com.teradata.connector.common.converter.ConnectorDataTypeConverter.IntegerToLong;
import com.teradata.connector.common.converter.ConnectorDataTypeConverter.IntegerToShort;
import com.teradata.connector.common.converter.ConnectorDataTypeConverter.IntegerToString;
import com.teradata.connector.common.converter.ConnectorDataTypeConverter.IntegerToTinyInt;
import com.teradata.connector.common.converter.ConnectorDataTypeConverter.IntervalToString;
import com.teradata.connector.common.converter.ConnectorDataTypeConverter.LongToBigDecimal;
import com.teradata.connector.common.converter.ConnectorDataTypeConverter.LongToBigDecimalWithScale;
import com.teradata.connector.common.converter.ConnectorDataTypeConverter.LongToBoolean;
import com.teradata.connector.common.converter.ConnectorDataTypeConverter.LongToDouble;
import com.teradata.connector.common.converter.ConnectorDataTypeConverter.LongToInteger;
import com.teradata.connector.common.converter.ConnectorDataTypeConverter.LongToLong;
import com.teradata.connector.common.converter.ConnectorDataTypeConverter.LongToShort;
import com.teradata.connector.common.converter.ConnectorDataTypeConverter.LongToString;
import com.teradata.connector.common.converter.ConnectorDataTypeConverter.LongToTinyInt;
import com.teradata.connector.common.converter.ConnectorDataTypeConverter.ObjectToString;
import com.teradata.connector.common.converter.ConnectorDataTypeConverter.PeriodToString;
import com.teradata.connector.common.converter.ConnectorDataTypeConverter.ShortToBigDecimal;
import com.teradata.connector.common.converter.ConnectorDataTypeConverter.ShortToBigDecimalWithScale;
import com.teradata.connector.common.converter.ConnectorDataTypeConverter.ShortToBoolean;
import com.teradata.connector.common.converter.ConnectorDataTypeConverter.ShortToDouble;
import com.teradata.connector.common.converter.ConnectorDataTypeConverter.ShortToInteger;
import com.teradata.connector.common.converter.ConnectorDataTypeConverter.ShortToLong;
import com.teradata.connector.common.converter.ConnectorDataTypeConverter.ShortToShort;
import com.teradata.connector.common.converter.ConnectorDataTypeConverter.ShortToString;
import com.teradata.connector.common.converter.ConnectorDataTypeConverter.ShortToTinyInt;
import com.teradata.connector.common.converter.ConnectorDataTypeConverter.StringToBigDecimal;
import com.teradata.connector.common.converter.ConnectorDataTypeConverter.StringToBigDecimalWithScale;
import com.teradata.connector.common.converter.ConnectorDataTypeConverter.StringToBinary;
import com.teradata.connector.common.converter.ConnectorDataTypeConverter.StringToBoolean;
import com.teradata.connector.common.converter.ConnectorDataTypeConverter.StringToDate;
import com.teradata.connector.common.converter.ConnectorDataTypeConverter.StringToDouble;
import com.teradata.connector.common.converter.ConnectorDataTypeConverter.StringToInteger;
import com.teradata.connector.common.converter.ConnectorDataTypeConverter.StringToInterval;
import com.teradata.connector.common.converter.ConnectorDataTypeConverter.StringToLong;
import com.teradata.connector.common.converter.ConnectorDataTypeConverter.StringToPeriod;
import com.teradata.connector.common.converter.ConnectorDataTypeConverter.StringToShort;
import com.teradata.connector.common.converter.ConnectorDataTypeConverter.StringToString;
import com.teradata.connector.common.converter.ConnectorDataTypeConverter.StringToTime;
import com.teradata.connector.common.converter.ConnectorDataTypeConverter.StringToTimestamp;
import com.teradata.connector.common.converter.ConnectorDataTypeConverter.StringToTinyInt;
import com.teradata.connector.common.converter.ConnectorDataTypeConverter.TimeToString;
import com.teradata.connector.common.converter.ConnectorDataTypeConverter.TimeToTime;
import com.teradata.connector.common.converter.ConnectorDataTypeConverter.TimestampToDate;
import com.teradata.connector.common.converter.ConnectorDataTypeConverter.TimestampToString;
import com.teradata.connector.common.converter.ConnectorDataTypeConverter.TimestampToTime;
import com.teradata.connector.common.converter.ConnectorDataTypeConverter.TimestampToTimestamp;
import com.teradata.connector.common.converter.ConnectorDataTypeConverter.TinyIntToBigDecimal;
import com.teradata.connector.common.converter.ConnectorDataTypeConverter.TinyIntToBigDecimalWithScale;
import com.teradata.connector.common.converter.ConnectorDataTypeConverter.TinyIntToBoolean;
import com.teradata.connector.common.converter.ConnectorDataTypeConverter.TinyIntToDouble;
import com.teradata.connector.common.converter.ConnectorDataTypeConverter.TinyIntToInteger;
import com.teradata.connector.common.converter.ConnectorDataTypeConverter.TinyIntToLong;
import com.teradata.connector.common.converter.ConnectorDataTypeConverter.TinyIntToShort;
import com.teradata.connector.common.converter.ConnectorDataTypeConverter.TinyIntToString;
import com.teradata.connector.common.converter.ConnectorDataTypeConverter.TinyIntToTinyInt;
import com.teradata.connector.common.converter.ConnectorDataTypeDefinition;
import com.teradata.connector.common.exception.ConnectorException;
import com.teradata.connector.common.exception.ConnectorException.ErrorCode;
import com.teradata.connector.common.utils.ConnectorConfiguration;
import com.teradata.connector.common.utils.ConnectorSchemaUtils;
import com.teradata.connector.sample.plugin.utils.CommonDBConfiguration;
import com.teradata.connector.sample.plugin.utils.CommonDBSchemaUtils;
import com.teradata.connector.teradata.schema.TeradataColumnDesc;
import com.teradata.connector.teradata.schema.TeradataTableDesc;
import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.util.Map;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.JobContext;

public class CommonDBConverter extends ConnectorConverter {
    protected static final int NUMBER_PRECISION_MAX = 38;
    protected int columnCount = 0;
    protected Configuration configuration;
    protected ConnectorDataTypeConverter[] dataTypeConverters = null;
    protected ConnectorRecordSchema sourceRecordSchema = null;
    protected ConnectorRecord targetRecord = null;
    protected ConnectorRecordSchema targetRecordSchema = null;
    protected TeradataTableDesc targetTableDesc = null;

    @Override
    public ConnectorDataTypeConverter[] lookupConverter(Configuration configuraion, ConnectorRecordSchema sourceRecordSchema) throws ConnectorException {
        TeradataColumnDesc[] teradataColumnDescs = this.targetTableDesc.getColumns();
        this.dataTypeConverters = new ConnectorDataTypeConverter[this.columnCount];
        int i = 0;
        while (i < this.columnCount) {
            int sourceType;
            if (sourceRecordSchema == null) {
                sourceType = 12;
            } else {
                sourceType = sourceRecordSchema.getFieldType(i);
            }
            try {
                int targetType = teradataColumnDescs[i].getType();
                TeradataColumnDesc targetColumnDesc = this.targetTableDesc.getColumn(i);
                if (sourceType == ConnectorDataTypeDefinition.TYPE_UDF) {
                    this.dataTypeConverters[i] = ConnectorSchemaUtils.lookupUDF(sourceRecordSchema, i);
                    if (this.dataTypeConverters[i] == null) {
                        throw new ConnectorException((int) ErrorCode.CONVERTER_CONSTRUCTOR_NOT_MATCHING);
                    }
                } else {
                    this.dataTypeConverters[i] = lookupDataTypeConverter(sourceType, targetType, i, targetColumnDesc);
                    this.dataTypeConverters[i].setNullable(targetColumnDesc.isNullable());
                }
                i++;
            } catch (IllegalArgumentException e) {
                throw new ConnectorException(e.getMessage(), e);
            }
        }
        this.targetRecord = new ConnectorRecord(this.columnCount);
        return this.dataTypeConverters;
    }

    @Override
    public void lookupConverter(ConnectorRecordSchema sourceRecordSchema) throws ConnectorException {
        this.dataTypeConverters = lookupConverter(this.configuration, sourceRecordSchema);
    }

    @Override
    public ConnectorRecord convert(ConnectorRecord sourceRecord) throws ConnectorException {
        int i = 0;
        while (i < this.columnCount) {
            try {
                this.targetRecord.set(i, this.dataTypeConverters[i].convert(sourceRecord.get(i)));
                i++;
            } catch (ConnectorException e) {
                throw new ConnectorException(e.getMessage(), e);
            }
        }
        return this.targetRecord;
    }

    @Override
    public void initialize(JobContext context) throws ConnectorException {
        this.configuration = context.getConfiguration();
        this.sourceRecordSchema = ConnectorSchemaUtils.recordSchemaFromString(ConnectorConfiguration.getInputConverterRecordSchema(this.configuration));
        this.targetRecordSchema = ConnectorSchemaUtils.recordSchemaFromString(ConnectorConfiguration.getOutputConverterRecordSchema(this.configuration));
        if (this.targetRecordSchema == null || this.targetRecordSchema.getLength() == 0) {
            throw new ConnectorException((int) ErrorCode.TARGET_RECORD_SCHEMA_IS_NULL);
        } else if (this.sourceRecordSchema == null || this.sourceRecordSchema.getLength() == 0 || this.sourceRecordSchema.getLength() == this.targetRecordSchema.getLength()) {
            this.targetTableDesc = CommonDBSchemaUtils.tableDescFromText(CommonDBConfiguration.getOutputTableDesc(this.configuration));
            this.columnCount = this.targetRecordSchema.getLength();
        } else {
            throw new ConnectorException((int) ErrorCode.COLUMN_LENGTH_OF_SOURCE_RECORD_SCHEMA_MISMATH_LENGTH_OF_TARGET_RECORD_SCHEMA);
        }
    }

    /* Access modifiers changed, original: protected */
    public ConnectorDataTypeConverter lookupDataTypeConverter(int sourceType, int targetType, int index, TeradataColumnDesc targetColumnDesc) throws ConnectorException {
        ConnectorDataTypeConverter converter = new ObjectToString();
        switch (sourceType) {
            case ConnectorDataTypeDefinition.TYPE_TINYINT /*-6*/:
                switch (targetType) {
                    case ConnectorDataTypeDefinition.TYPE_TINYINT /*-6*/:
                        converter = new TinyIntToTinyInt();
                        break;
                    case ConnectorDataTypeDefinition.TYPE_BIGINT /*-5*/:
                        converter = new TinyIntToLong();
                        break;
                    case ConnectorDataTypeDefinition.TYPE_LONGVARCHAR /*-1*/:
                    case 1:
                    case 12:
                    case ConnectorDataTypeDefinition.TYPE_CLOB /*2005*/:
                        converter = new TinyIntToString();
                        break;
                    case 2:
                        converter = new TinyIntToBigDecimal();
                        break;
                    case ConnectorDataTypeDefinition.TYPE_DECIMAL /*3*/:
                        converter = new TinyIntToBigDecimalWithScale();
                        ((TinyIntToBigDecimalWithScale) converter).setScale(targetColumnDesc.getScale());
                        break;
                    case 4:
                        converter = new TinyIntToInteger();
                        break;
                    case 5:
                        converter = new TinyIntToShort();
                        break;
                    case 6:
                    case ConnectorDataTypeDefinition.TYPE_REAL /*7*/:
                    case ConnectorDataTypeDefinition.TYPE_DOUBLE /*8*/:
                        converter = new TinyIntToDouble();
                        break;
                    case ConnectorDataTypeDefinition.TYPE_BOOLEAN /*16*/:
                        converter = new TinyIntToBoolean();
                        break;
                    default:
                        throw new ConnectorException((int) ErrorCode.FIELD_DATATYPE_CONVERSION_UNSUPPORTED);
                }
                converter.setDefaultValue(CommonDBDataTypeDefinition.TINYINT_NULL_VALUE);
                return converter;
            case ConnectorDataTypeDefinition.TYPE_BIGINT /*-5*/:
                switch (targetType) {
                    case ConnectorDataTypeDefinition.TYPE_TINYINT /*-6*/:
                        converter = new LongToTinyInt();
                        break;
                    case ConnectorDataTypeDefinition.TYPE_BIGINT /*-5*/:
                        converter = new LongToLong();
                        break;
                    case ConnectorDataTypeDefinition.TYPE_LONGVARCHAR /*-1*/:
                    case 1:
                    case 12:
                    case ConnectorDataTypeDefinition.TYPE_CLOB /*2005*/:
                        converter = new LongToString();
                        break;
                    case 2:
                        converter = new LongToBigDecimal();
                        break;
                    case ConnectorDataTypeDefinition.TYPE_DECIMAL /*3*/:
                        converter = new LongToBigDecimalWithScale();
                        ((LongToBigDecimalWithScale) converter).setScale(targetColumnDesc.getScale());
                        break;
                    case 4:
                        converter = new LongToInteger();
                        break;
                    case 5:
                        converter = new LongToShort();
                        break;
                    case 6:
                    case ConnectorDataTypeDefinition.TYPE_REAL /*7*/:
                    case ConnectorDataTypeDefinition.TYPE_DOUBLE /*8*/:
                        converter = new LongToDouble();
                        break;
                    case ConnectorDataTypeDefinition.TYPE_BOOLEAN /*16*/:
                        converter = new LongToBoolean();
                        break;
                    default:
                        throw new ConnectorException((int) ErrorCode.FIELD_DATATYPE_CONVERSION_UNSUPPORTED);
                }
                converter.setDefaultValue(CommonDBDataTypeDefinition.BIGINT_NULL_VALUE);
                return converter;
            case ConnectorDataTypeDefinition.TYPE_VARBINARY /*-3*/:
            case ConnectorDataTypeDefinition.TYPE_BINARY /*-2*/:
                switch (targetType) {
                    case ConnectorDataTypeDefinition.TYPE_VARBINARY /*-3*/:
                    case ConnectorDataTypeDefinition.TYPE_BINARY /*-2*/:
                    case ConnectorDataTypeDefinition.TYPE_BLOB /*2004*/:
                        converter = new BinaryToBinary();
                        break;
                    case ConnectorDataTypeDefinition.TYPE_LONGVARCHAR /*-1*/:
                    case 1:
                    case 12:
                    case ConnectorDataTypeDefinition.TYPE_CLOB /*2005*/:
                        converter = new BinaryToString();
                        break;
                    default:
                        throw new ConnectorException((int) ErrorCode.FIELD_DATATYPE_CONVERSION_UNSUPPORTED);
                }
                converter.setDefaultValue(CommonDBDataTypeDefinition.BINARY_NULL_VALUE);
                return converter;
            case ConnectorDataTypeDefinition.TYPE_LONGVARCHAR /*-1*/:
            case 1:
            case 12:
                switch (targetType) {
                    case ConnectorDataTypeDefinition.TYPE_TINYINT /*-6*/:
                        return new StringToTinyInt();
                    case ConnectorDataTypeDefinition.TYPE_BIGINT /*-5*/:
                        return new StringToLong();
                    case ConnectorDataTypeDefinition.TYPE_VARBINARY /*-3*/:
                    case ConnectorDataTypeDefinition.TYPE_BINARY /*-2*/:
                        return new StringToBinary();
                    case ConnectorDataTypeDefinition.TYPE_LONGVARCHAR /*-1*/:
                    case 1:
                    case 12:
                    case ConnectorDataTypeDefinition.TYPE_CLOB /*2005*/:
                        return new StringToString();
                    case 2:
                        return new StringToBigDecimal();
                    case ConnectorDataTypeDefinition.TYPE_DECIMAL /*3*/:
                        converter = new StringToBigDecimalWithScale();
                        ((StringToBigDecimalWithScale) converter).setScale(targetColumnDesc.getScale());
                        return converter;
                    case 4:
                        return new StringToInteger();
                    case 5:
                        return new StringToShort();
                    case 6:
                    case ConnectorDataTypeDefinition.TYPE_REAL /*7*/:
                    case ConnectorDataTypeDefinition.TYPE_DOUBLE /*8*/:
                        return new StringToDouble();
                    case ConnectorDataTypeDefinition.TYPE_BOOLEAN /*16*/:
                        return new StringToBoolean();
                    case ConnectorDataTypeDefinition.TYPE_DATE /*91*/:
                        return new StringToDate();
                    case ConnectorDataTypeDefinition.TYPE_TIME /*92*/:
                        return new StringToTime();
                    case ConnectorDataTypeDefinition.TYPE_TIMESTAMP /*93*/:
                        return new StringToTimestamp();
                    case 1111:
                        return new StringToInterval();
                    case ConnectorDataTypeDefinition.TYPE_PERIOD /*2002*/:
                        return new StringToPeriod();
                    default:
                        throw new ConnectorException((int) ErrorCode.FIELD_DATATYPE_CONVERSION_UNSUPPORTED);
                }
            case 2:
            case ConnectorDataTypeDefinition.TYPE_DECIMAL /*3*/:
                switch (targetType) {
                    case ConnectorDataTypeDefinition.TYPE_TINYINT /*-6*/:
                        converter = new BigDecimalToTinyInt();
                        break;
                    case ConnectorDataTypeDefinition.TYPE_BIGINT /*-5*/:
                        converter = new BigDecimalToLong();
                        break;
                    case ConnectorDataTypeDefinition.TYPE_LONGVARCHAR /*-1*/:
                    case 1:
                    case 12:
                    case ConnectorDataTypeDefinition.TYPE_CLOB /*2005*/:
                        converter = new BigDecimalToString();
                        break;
                    case 2:
                        converter = new BigDecimalToBigDecimal();
                        break;
                    case ConnectorDataTypeDefinition.TYPE_DECIMAL /*3*/:
                        converter = new BigDecimalToBigDecimalWithScale();
                        ((BigDecimalToBigDecimalWithScale) converter).setScale(targetColumnDesc.getScale());
                        break;
                    case 4:
                        converter = new BigDecimalToInteger();
                        break;
                    case 5:
                        converter = new BigDecimalToShort();
                        break;
                    case 6:
                    case ConnectorDataTypeDefinition.TYPE_REAL /*7*/:
                    case ConnectorDataTypeDefinition.TYPE_DOUBLE /*8*/:
                        converter = new BigDecimalToDouble();
                        break;
                    default:
                        throw new ConnectorException((int) ErrorCode.FIELD_DATATYPE_CONVERSION_UNSUPPORTED);
                }
                converter.setDefaultValue(CommonDBDataTypeDefinition.BIGDECIMAL_NULL_VALUE);
                return converter;
            case 4:
                switch (targetType) {
                    case ConnectorDataTypeDefinition.TYPE_TINYINT /*-6*/:
                        converter = new IntegerToTinyInt();
                        converter.setDefaultValue(CommonDBDataTypeDefinition.INTEGER_NULL_VALUE);
                        break;
                    case ConnectorDataTypeDefinition.TYPE_BIGINT /*-5*/:
                        converter = new IntegerToLong();
                        converter.setDefaultValue(CommonDBDataTypeDefinition.INTEGER_NULL_VALUE);
                        break;
                    case ConnectorDataTypeDefinition.TYPE_LONGVARCHAR /*-1*/:
                    case 1:
                    case 12:
                    case ConnectorDataTypeDefinition.TYPE_CLOB /*2005*/:
                        converter = new IntegerToString();
                        break;
                    case 2:
                        converter = new IntegerToBigDecimal();
                        break;
                    case ConnectorDataTypeDefinition.TYPE_DECIMAL /*3*/:
                        converter = new IntegerToBigDecimalWithScale();
                        ((IntegerToBigDecimalWithScale) converter).setScale(targetColumnDesc.getScale());
                        break;
                    case 4:
                        converter = new IntegerToInteger();
                        break;
                    case 5:
                        converter = new IntegerToShort();
                        converter.setDefaultValue(CommonDBDataTypeDefinition.INTEGER_NULL_VALUE);
                        break;
                    case 6:
                    case ConnectorDataTypeDefinition.TYPE_REAL /*7*/:
                    case ConnectorDataTypeDefinition.TYPE_DOUBLE /*8*/:
                        converter = new IntegerToDouble();
                        converter.setDefaultValue(CommonDBDataTypeDefinition.INTEGER_NULL_VALUE);
                        break;
                    case ConnectorDataTypeDefinition.TYPE_BOOLEAN /*16*/:
                        converter = new IntegerToBoolean();
                        break;
                    default:
                        throw new ConnectorException((int) ErrorCode.FIELD_DATATYPE_CONVERSION_UNSUPPORTED);
                }
                converter.setDefaultValue(CommonDBDataTypeDefinition.INTEGER_NULL_VALUE);
                return converter;
            case 5:
                switch (targetType) {
                    case ConnectorDataTypeDefinition.TYPE_TINYINT /*-6*/:
                        converter = new ShortToTinyInt();
                        break;
                    case ConnectorDataTypeDefinition.TYPE_BIGINT /*-5*/:
                        converter = new ShortToLong();
                        break;
                    case ConnectorDataTypeDefinition.TYPE_LONGVARCHAR /*-1*/:
                    case 1:
                    case 12:
                    case ConnectorDataTypeDefinition.TYPE_CLOB /*2005*/:
                        converter = new ShortToString();
                        break;
                    case 2:
                        converter = new ShortToBigDecimal();
                        break;
                    case ConnectorDataTypeDefinition.TYPE_DECIMAL /*3*/:
                        converter = new ShortToBigDecimalWithScale();
                        ((ShortToBigDecimalWithScale) converter).setScale(targetColumnDesc.getScale());
                        break;
                    case 4:
                        converter = new ShortToInteger();
                        break;
                    case 5:
                        converter = new ShortToShort();
                        break;
                    case 6:
                    case ConnectorDataTypeDefinition.TYPE_REAL /*7*/:
                    case ConnectorDataTypeDefinition.TYPE_DOUBLE /*8*/:
                        converter = new ShortToDouble();
                        break;
                    case ConnectorDataTypeDefinition.TYPE_BOOLEAN /*16*/:
                        converter = new ShortToBoolean();
                        break;
                    default:
                        throw new ConnectorException((int) ErrorCode.FIELD_DATATYPE_CONVERSION_UNSUPPORTED);
                }
                converter.setDefaultValue(CommonDBDataTypeDefinition.SMALLINT_NULL_VALUE);
                return converter;
            case 6:
                switch (targetType) {
                    case ConnectorDataTypeDefinition.TYPE_TINYINT /*-6*/:
                        converter = new FloatToTinyInt();
                        break;
                    case ConnectorDataTypeDefinition.TYPE_BIGINT /*-5*/:
                        converter = new FloatToLong();
                        break;
                    case ConnectorDataTypeDefinition.TYPE_LONGVARCHAR /*-1*/:
                    case 1:
                    case 12:
                    case ConnectorDataTypeDefinition.TYPE_CLOB /*2005*/:
                        converter = new FloatToString();
                        break;
                    case 2:
                        converter = new FloatToBigDecimal();
                        break;
                    case ConnectorDataTypeDefinition.TYPE_DECIMAL /*3*/:
                        converter = new FloatToBigDecimalWithScale();
                        ((FloatToBigDecimalWithScale) converter).setScale(targetColumnDesc.getScale());
                        ((FloatToBigDecimalWithScale) converter).setPrecision(targetColumnDesc.getPrecision());
                        break;
                    case 4:
                        converter = new FloatToInteger();
                        break;
                    case 5:
                        converter = new FloatToShort();
                        break;
                    case 6:
                    case ConnectorDataTypeDefinition.TYPE_REAL /*7*/:
                    case ConnectorDataTypeDefinition.TYPE_DOUBLE /*8*/:
                        converter = new FloatToDouble();
                        break;
                    default:
                        throw new ConnectorException((int) ErrorCode.FIELD_DATATYPE_CONVERSION_UNSUPPORTED);
                }
                converter.setDefaultValue(CommonDBDataTypeDefinition.FLOAT_NULL_VALUE);
                return converter;
            case ConnectorDataTypeDefinition.TYPE_REAL /*7*/:
            case ConnectorDataTypeDefinition.TYPE_DOUBLE /*8*/:
                switch (targetType) {
                    case ConnectorDataTypeDefinition.TYPE_TINYINT /*-6*/:
                        converter = new DoubleToTinyInt();
                        break;
                    case ConnectorDataTypeDefinition.TYPE_BIGINT /*-5*/:
                        converter = new DoubleToLong();
                        break;
                    case ConnectorDataTypeDefinition.TYPE_LONGVARCHAR /*-1*/:
                    case 1:
                    case 12:
                    case ConnectorDataTypeDefinition.TYPE_CLOB /*2005*/:
                        converter = new DoubleToString();
                        break;
                    case 2:
                        converter = new DoubleToBigDecimal();
                        break;
                    case ConnectorDataTypeDefinition.TYPE_DECIMAL /*3*/:
                        converter = new DoubleToBigDecimalWithScale();
                        ((DoubleToBigDecimalWithScale) converter).setScale(targetColumnDesc.getScale());
                        break;
                    case 4:
                        converter = new DoubleToInteger();
                        break;
                    case 5:
                        converter = new DoubleToShort();
                        break;
                    case 6:
                    case ConnectorDataTypeDefinition.TYPE_REAL /*7*/:
                    case ConnectorDataTypeDefinition.TYPE_DOUBLE /*8*/:
                        converter = new DoubleToDouble();
                        break;
                    default:
                        throw new ConnectorException((int) ErrorCode.FIELD_DATATYPE_CONVERSION_UNSUPPORTED);
                }
                converter.setDefaultValue(CommonDBDataTypeDefinition.DOUBLE_NULL_VALUE);
                return converter;
            case ConnectorDataTypeDefinition.TYPE_BOOLEAN /*16*/:
                switch (targetType) {
                    case ConnectorDataTypeDefinition.TYPE_TINYINT /*-6*/:
                        converter = new BooleanToTinyInt();
                        ((BooleanToTinyInt) converter).setFalseDefaultValue(CommonDBDataTypeDefinition.INTEGER_FALSE_VALUE);
                        ((BooleanToTinyInt) converter).setDefaultValue(CommonDBDataTypeDefinition.INTEGER_TRUE_VALUE);
                        return converter;
                    case ConnectorDataTypeDefinition.TYPE_BIGINT /*-5*/:
                        converter = new BooleanToLong();
                        ((BooleanToLong) converter).setFalseDefaultValue(CommonDBDataTypeDefinition.LONG_FALSE_VALUE);
                        ((BooleanToLong) converter).setDefaultValue(CommonDBDataTypeDefinition.LONG_TRUE_VALUE);
                        return converter;
                    case ConnectorDataTypeDefinition.TYPE_LONGVARCHAR /*-1*/:
                    case 1:
                    case 12:
                    case ConnectorDataTypeDefinition.TYPE_CLOB /*2005*/:
                        return new BooleanToString();
                    case 2:
                        converter = new BooleanToBigDecimal();
                        ((BooleanToBigDecimal) converter).setFalseDefaultValue(CommonDBDataTypeDefinition.BIGDECIMAL_FALSE_VALUE);
                        ((BooleanToBigDecimal) converter).setDefaultValue(CommonDBDataTypeDefinition.BIGDECIMAL_TRUE_VALUE);
                        return converter;
                    case ConnectorDataTypeDefinition.TYPE_DECIMAL /*3*/:
                        converter = new BooleanToBigDecimalWithScale();
                        ((BooleanToBigDecimalWithScale) converter).setFalseDefaultValue(CommonDBDataTypeDefinition.BIGDECIMAL_FALSE_VALUE);
                        ((BooleanToBigDecimalWithScale) converter).setDefaultValue(CommonDBDataTypeDefinition.BIGDECIMAL_TRUE_VALUE);
                        ((BooleanToBigDecimalWithScale) converter).setScale(targetColumnDesc.getScale());
                        ((BooleanToBigDecimalWithScale) converter).setPrecision(targetColumnDesc.getPrecision());
                        return converter;
                    case 4:
                        converter = new BooleanToInteger();
                        ((BooleanToInteger) converter).setFalseDefaultValue(CommonDBDataTypeDefinition.INTEGER_FALSE_VALUE);
                        ((BooleanToInteger) converter).setDefaultValue(CommonDBDataTypeDefinition.INTEGER_TRUE_VALUE);
                        return converter;
                    case 5:
                        converter = new BooleanToShort();
                        ((BooleanToShort) converter).setFalseDefaultValue(CommonDBDataTypeDefinition.INTEGER_FALSE_VALUE);
                        ((BooleanToShort) converter).setDefaultValue(CommonDBDataTypeDefinition.INTEGER_TRUE_VALUE);
                        return converter;
                    case 6:
                    case ConnectorDataTypeDefinition.TYPE_REAL /*7*/:
                    case ConnectorDataTypeDefinition.TYPE_DOUBLE /*8*/:
                        converter = new BooleanToDouble();
                        ((BooleanToDouble) converter).setFalseDefaultValue(CommonDBDataTypeDefinition.DOUBLE_FALSE_VALUE);
                        ((BooleanToDouble) converter).setDefaultValue(CommonDBDataTypeDefinition.DOUBLE_TRUE_VALUE);
                        return converter;
                    default:
                        throw new ConnectorException((int) ErrorCode.FIELD_DATATYPE_CONVERSION_UNSUPPORTED);
                }
            case ConnectorDataTypeDefinition.TYPE_DATE /*91*/:
                switch (targetType) {
                    case ConnectorDataTypeDefinition.TYPE_LONGVARCHAR /*-1*/:
                    case 1:
                    case 12:
                    case ConnectorDataTypeDefinition.TYPE_CLOB /*2005*/:
                        return new DateToString();
                    case ConnectorDataTypeDefinition.TYPE_DATE /*91*/:
                        return new DateToDate();
                    case ConnectorDataTypeDefinition.TYPE_TIMESTAMP /*93*/:
                        return new DateToTimestamp();
                    default:
                        throw new ConnectorException((int) ErrorCode.FIELD_DATATYPE_CONVERSION_UNSUPPORTED);
                }
            case ConnectorDataTypeDefinition.TYPE_TIME /*92*/:
                switch (targetType) {
                    case ConnectorDataTypeDefinition.TYPE_LONGVARCHAR /*-1*/:
                    case 1:
                    case 12:
                    case ConnectorDataTypeDefinition.TYPE_CLOB /*2005*/:
                        return new TimeToString();
                    case ConnectorDataTypeDefinition.TYPE_TIME /*92*/:
                        return new TimeToTime();
                    default:
                        throw new ConnectorException((int) ErrorCode.FIELD_DATATYPE_CONVERSION_UNSUPPORTED);
                }
            case ConnectorDataTypeDefinition.TYPE_TIMESTAMP /*93*/:
                switch (targetType) {
                    case ConnectorDataTypeDefinition.TYPE_LONGVARCHAR /*-1*/:
                    case 1:
                    case 12:
                    case ConnectorDataTypeDefinition.TYPE_CLOB /*2005*/:
                        return new TimestampToString();
                    case ConnectorDataTypeDefinition.TYPE_DATE /*91*/:
                        return new TimestampToDate();
                    case ConnectorDataTypeDefinition.TYPE_TIME /*92*/:
                        return new TimestampToTime();
                    case ConnectorDataTypeDefinition.TYPE_TIMESTAMP /*93*/:
                        return new TimestampToTimestamp();
                    default:
                        throw new ConnectorException((int) ErrorCode.FIELD_DATATYPE_CONVERSION_UNSUPPORTED);
                }
            case 1111:
                switch (targetType) {
                    case ConnectorDataTypeDefinition.TYPE_LONGVARCHAR /*-1*/:
                    case 1:
                    case 12:
                    case 1111:
                    case ConnectorDataTypeDefinition.TYPE_CLOB /*2005*/:
                        converter = new IntervalToString();
                        converter.setDefaultValue("");
                        return converter;
                    default:
                        throw new ConnectorException((int) ErrorCode.FIELD_DATATYPE_CONVERSION_UNSUPPORTED);
                }
            case ConnectorDataTypeDefinition.TYPE_UDF /*1883*/:
                try {
                    String[] parameters = this.sourceRecordSchema.getParameters(index);
                    int length = this.sourceRecordSchema.getParameters(index).length;
                    Object[] objects = new Object[length];
                    for (Constructor<?> constructor : Class.forName(this.sourceRecordSchema.getDataTypeConverter(index)).getConstructors()) {
                        Class<?>[] parameterClasses = constructor.getParameterTypes();
                        if (parameterClasses.length == length) {
                            for (int i = 0; i < length; i++) {
                                objects[i] = parameterClasses[i].cast(parameters[i]);
                            }
                            return (ConnectorDataTypeConverter) constructor.newInstance(objects);
                        }
                    }
                    return converter;
                } catch (InstantiationException e) {
                    throw new ConnectorException(e.getMessage(), e);
                } catch (IllegalAccessException e2) {
                    throw new ConnectorException(e2.getMessage(), e2);
                } catch (ClassNotFoundException e22) {
                    throw new ConnectorException(e22.getMessage(), e22);
                } catch (IllegalArgumentException e222) {
                    throw new ConnectorException(e222.getMessage(), e222);
                } catch (InvocationTargetException e2222) {
                    throw new ConnectorException(e2222.getMessage(), e2222);
                }
            case ConnectorDataTypeDefinition.TYPE_PERIOD /*2002*/:
                switch (targetType) {
                    case ConnectorDataTypeDefinition.TYPE_LONGVARCHAR /*-1*/:
                    case 1:
                    case 12:
                    case ConnectorDataTypeDefinition.TYPE_PERIOD /*2002*/:
                    case ConnectorDataTypeDefinition.TYPE_CLOB /*2005*/:
                        converter = new PeriodToString();
                        converter.setDefaultValue("");
                        return converter;
                    default:
                        throw new ConnectorException((int) ErrorCode.FIELD_DATATYPE_CONVERSION_UNSUPPORTED);
                }
            case ConnectorDataTypeDefinition.TYPE_BLOB /*2004*/:
                switch (targetType) {
                    case ConnectorDataTypeDefinition.TYPE_BINARY /*-2*/:
                        converter = new BlobToBinary();
                        break;
                    case ConnectorDataTypeDefinition.TYPE_LONGVARCHAR /*-1*/:
                    case 1:
                    case 12:
                    case ConnectorDataTypeDefinition.TYPE_BLOB /*2004*/:
                    case ConnectorDataTypeDefinition.TYPE_CLOB /*2005*/:
                        converter = new BlobToString();
                        break;
                    default:
                        throw new ConnectorException((int) ErrorCode.FIELD_DATATYPE_CONVERSION_UNSUPPORTED);
                }
                converter.setDefaultValue(CommonDBDataTypeDefinition.BINARY_NULL_VALUE);
                return converter;
            case ConnectorDataTypeDefinition.TYPE_CLOB /*2005*/:
                switch (targetType) {
                    case ConnectorDataTypeDefinition.TYPE_LONGVARCHAR /*-1*/:
                    case 1:
                    case 12:
                    case ConnectorDataTypeDefinition.TYPE_CLOB /*2005*/:
                        return new ClobToString();
                    default:
                        throw new ConnectorException((int) ErrorCode.FIELD_DATATYPE_CONVERSION_UNSUPPORTED);
                }
            default:
                return converter;
        }
    }

    @Override
    public Map<Integer, Boolean> initializeNullable() throws ConnectorException {
        return null;
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