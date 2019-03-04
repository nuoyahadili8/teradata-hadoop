package com.teradata.connector.common.converter;

import org.apache.hadoop.mapreduce.*;
import com.teradata.connector.common.exception.*;

import java.util.*;

import org.apache.hadoop.conf.*;
import com.teradata.connector.common.*;
import com.teradata.connector.common.utils.*;

public abstract class ConnectorConverter {
    public abstract void initialize(final JobContext p0) throws ConnectorException;

    public abstract Map<Integer, Boolean> initializeNullable() throws ConnectorException;

    public abstract Map<Integer, Object> initializeDefaultValue() throws ConnectorException;

    public abstract Map<Integer, Object> initializeFalseDefaultValue() throws ConnectorException;

    public abstract Map<Integer, Object> initializeTrueDefaultValue() throws ConnectorException;

    public abstract ConnectorRecord convert(final ConnectorRecord p0) throws ConnectorException;

    public int[] initializeScale() throws ConnectorException {
        return null;
    }

    public int[] initializePrecision() throws ConnectorException {
        return null;
    }

    public int[] initializeLength() throws ConnectorException {
        return null;
    }

    public ConnectorDataTypeConverter[] lookupConverter(final Configuration configuration, final ConnectorRecordSchema sourceRecordSchema) throws ConnectorException {
        final int[] scales = this.initializeScale();
        final int[] precisions = this.initializePrecision();
        final int[] lengths = this.initializeLength();
        final Map<Integer, Object> defaultVal = this.initializeDefaultValue();
        final Map<Integer, Boolean> nullable = this.initializeNullable();
        final Map<Integer, Object> falseDefVal = this.initializeFalseDefaultValue();
        final Map<Integer, Object> trueDefVal = this.initializeTrueDefaultValue();
        ConnectorRecordSchema targetRecordSchema = null;
        targetRecordSchema = ConnectorSchemaUtils.recordSchemaFromString(ConnectorConfiguration.getOutputConverterRecordSchema(configuration));
        final int columnCount = targetRecordSchema.getLength();
        final ConnectorDataTypeConverter[] dataTypeConverters = new ConnectorDataTypeConverter[columnCount];
        for (int i = 0; i < columnCount; ++i) {
            try {
                int sourceType;
                if (sourceRecordSchema == null) {
                    sourceType = 12;
                } else {
                    sourceType = sourceRecordSchema.getFieldType(i);
                }
                final int targetType = targetRecordSchema.getFieldType(i);
                if (sourceType == 1883) {
                    dataTypeConverters[i] = ConnectorSchemaUtils.lookupUDF(sourceRecordSchema, i);
                    if (dataTypeConverters[i] == null) {
                        throw new ConnectorException(11001);
                    }
                } else {
                    dataTypeConverters[i] = this.lookupDataTypeConverter(sourceType, targetType, (scales == null) ? 6 : scales[i], (precisions == null) ? 6 : precisions[i], (lengths == null) ? Integer.MAX_VALUE : lengths[i], defaultVal, falseDefVal, trueDefVal, configuration);
                    final boolean nul = nullable.get(i);
                    dataTypeConverters[i].setNullable(nul);
                }
            } catch (IllegalArgumentException e) {
                throw new ConnectorException(e.getMessage(), e);
            }
        }
        return dataTypeConverters;
    }

    public ConnectorDataTypeConverter lookupDataTypeConverter(final int sourceType, final int targetType, final int targetScale, final int targetPrecision, final int targetLength, final Map<Integer, Object> defaultVal, final Map<Integer, Object> falseDefVal, final Map<Integer, Object> trueDefVal, final Configuration configuration) throws ConnectorException {
        if (targetType == 1882) {
            final ConnectorDataTypeConverter converter = new ConnectorDataTypeConverter.Others();
            return converter;
        }
        ConnectorDataTypeConverter converter = new ConnectorDataTypeConverter.ObjectToString();
        final String inputDateFormat = ConnectorConfiguration.getInputDateFormat(configuration);
        final String outputDateFormat = ConnectorConfiguration.getOutputDateFormat(configuration);
        final String inputTimezoneId = ConnectorConfiguration.getInputTimezoneId(configuration);
        final String outputTimezoneId = ConnectorConfiguration.getOutputTimezoneId(configuration);
        final String inputTimeFormat = ConnectorConfiguration.getInputTimeFormat(configuration);
        final String outputTimeFormat = ConnectorConfiguration.getOutputTimeFormat(configuration);
        final String inputTimestampFormat = ConnectorConfiguration.getInputTimestampFormat(configuration);
        final String outputTimestampFormat = ConnectorConfiguration.getOutputTimestampFormat(configuration);
        Label_5092:
        {
            switch (sourceType) {
                case 4: {
                    switch (targetType) {
                        case 4: {
                            converter = new ConnectorDataTypeConverter.IntegerToInteger();
                            break Label_5092;
                        }
                        case -5: {
                            converter = new ConnectorDataTypeConverter.IntegerToLong();
                            break Label_5092;
                        }
                        case 5: {
                            converter = new ConnectorDataTypeConverter.IntegerToShort();
                            break Label_5092;
                        }
                        case -6: {
                            converter = new ConnectorDataTypeConverter.IntegerToTinyInt();
                            break Label_5092;
                        }
                        case 6: {
                            converter = new ConnectorDataTypeConverter.IntegerToFloat();
                            break Label_5092;
                        }
                        case 7:
                        case 8: {
                            converter = new ConnectorDataTypeConverter.IntegerToDouble();
                            break Label_5092;
                        }
                        case 2: {
                            converter = new ConnectorDataTypeConverter.IntegerToBigDecimal();
                            break Label_5092;
                        }
                        case 3: {
                            converter = new ConnectorDataTypeConverter.IntegerToBigDecimalWithScale();
                            ((ConnectorDataTypeConverter.IntegerToBigDecimalWithScale) converter).setScale(targetScale);
                            break Label_5092;
                        }
                        case 93: {
                            converter = new ConnectorDataTypeConverter.IntegerToTimestampTZ(outputTimezoneId);
                            break Label_5092;
                        }
                        case 92: {
                            converter = new ConnectorDataTypeConverter.IntegerToTimeTZ(outputTimezoneId);
                            break Label_5092;
                        }
                        case 91: {
                            converter = new ConnectorDataTypeConverter.IntegerToDateTZ(outputTimezoneId);
                            break Label_5092;
                        }
                        case 1885:
                        case 1886: {
                            converter = new ConnectorDataTypeConverter.IntegerToCalendar(outputTimezoneId);
                            break Label_5092;
                        }
                        case 16: {
                            converter = new ConnectorDataTypeConverter.IntegerToBoolean();
                            break Label_5092;
                        }
                        case -1:
                        case 1:
                        case 12:
                        case 2005: {
                            converter = new ConnectorDataTypeConverter.IntegerToString();
                            break Label_5092;
                        }
                        default: {
                            throw new ConnectorException(14007, new Object[]{sourceType + " to " + targetType});
                        }
                    }
                }
                case -5: {
                    switch (targetType) {
                        case 4: {
                            converter = new ConnectorDataTypeConverter.LongToInteger();
                            break Label_5092;
                        }
                        case -5: {
                            converter = new ConnectorDataTypeConverter.LongToLong();
                            break Label_5092;
                        }
                        case 5: {
                            converter = new ConnectorDataTypeConverter.LongToShort();
                            break Label_5092;
                        }
                        case -6: {
                            converter = new ConnectorDataTypeConverter.LongToTinyInt();
                            break Label_5092;
                        }
                        case 6: {
                            converter = new ConnectorDataTypeConverter.LongToFloat();
                            break Label_5092;
                        }
                        case 7:
                        case 8: {
                            converter = new ConnectorDataTypeConverter.LongToDouble();
                            break Label_5092;
                        }
                        case 2: {
                            converter = new ConnectorDataTypeConverter.LongToBigDecimal();
                            break Label_5092;
                        }
                        case 3: {
                            converter = new ConnectorDataTypeConverter.LongToBigDecimalWithScale();
                            ((ConnectorDataTypeConverter.LongToBigDecimalWithScale) converter).setScale(targetScale);
                            break Label_5092;
                        }
                        case 16: {
                            converter = new ConnectorDataTypeConverter.LongToBoolean();
                            break Label_5092;
                        }
                        case -1:
                        case 1:
                        case 12:
                        case 2005: {
                            converter = new ConnectorDataTypeConverter.LongToString();
                            break Label_5092;
                        }
                        case 93: {
                            converter = new ConnectorDataTypeConverter.LongToTimestampTZ(outputTimezoneId);
                            break Label_5092;
                        }
                        case 92: {
                            converter = new ConnectorDataTypeConverter.LongToTimeTZ(outputTimezoneId);
                            break Label_5092;
                        }
                        case 91: {
                            converter = new ConnectorDataTypeConverter.LongToDateTZ(outputTimezoneId);
                            break Label_5092;
                        }
                        case 1885:
                        case 1886: {
                            converter = new ConnectorDataTypeConverter.LongToCalendar(outputTimezoneId);
                            break Label_5092;
                        }
                        default: {
                            throw new ConnectorException(14007, new Object[]{sourceType + " to " + targetType});
                        }
                    }
                }
                case 5: {
                    switch (targetType) {
                        case 4: {
                            converter = new ConnectorDataTypeConverter.ShortToInteger();
                            break Label_5092;
                        }
                        case -5: {
                            converter = new ConnectorDataTypeConverter.ShortToLong();
                            break Label_5092;
                        }
                        case 5: {
                            converter = new ConnectorDataTypeConverter.ShortToShort();
                            break Label_5092;
                        }
                        case -6: {
                            converter = new ConnectorDataTypeConverter.ShortToTinyInt();
                            break Label_5092;
                        }
                        case 6: {
                            converter = new ConnectorDataTypeConverter.ShortToFloat();
                            break Label_5092;
                        }
                        case 7:
                        case 8: {
                            converter = new ConnectorDataTypeConverter.ShortToDouble();
                            break Label_5092;
                        }
                        case 2: {
                            converter = new ConnectorDataTypeConverter.ShortToBigDecimal();
                            break Label_5092;
                        }
                        case 3: {
                            converter = new ConnectorDataTypeConverter.ShortToBigDecimalWithScale();
                            ((ConnectorDataTypeConverter.ShortToBigDecimalWithScale) converter).setScale(targetScale);
                            break Label_5092;
                        }
                        case 16: {
                            converter = new ConnectorDataTypeConverter.ShortToBoolean();
                            break Label_5092;
                        }
                        case -1:
                        case 1:
                        case 12:
                        case 2005: {
                            converter = new ConnectorDataTypeConverter.ShortToString();
                            break Label_5092;
                        }
                        default: {
                            throw new ConnectorException(14007, new Object[]{sourceType + " to " + targetType});
                        }
                    }
                }
                case -6: {
                    switch (targetType) {
                        case 4: {
                            converter = new ConnectorDataTypeConverter.TinyIntToInteger();
                            break Label_5092;
                        }
                        case -5: {
                            converter = new ConnectorDataTypeConverter.TinyIntToLong();
                            break Label_5092;
                        }
                        case 5: {
                            converter = new ConnectorDataTypeConverter.TinyIntToShort();
                            break Label_5092;
                        }
                        case -6: {
                            converter = new ConnectorDataTypeConverter.TinyIntToTinyInt();
                            break Label_5092;
                        }
                        case 6: {
                            converter = new ConnectorDataTypeConverter.TinyIntToFloat();
                            break Label_5092;
                        }
                        case 7:
                        case 8: {
                            converter = new ConnectorDataTypeConverter.TinyIntToDouble();
                            break Label_5092;
                        }
                        case 2: {
                            converter = new ConnectorDataTypeConverter.TinyIntToBigDecimal();
                            break Label_5092;
                        }
                        case 3: {
                            converter = new ConnectorDataTypeConverter.TinyIntToBigDecimalWithScale();
                            ((ConnectorDataTypeConverter.TinyIntToBigDecimalWithScale) converter).setScale(targetScale);
                            break Label_5092;
                        }
                        case 16: {
                            converter = new ConnectorDataTypeConverter.TinyIntToBoolean();
                            break Label_5092;
                        }
                        case -1:
                        case 1:
                        case 12:
                        case 2005: {
                            converter = new ConnectorDataTypeConverter.TinyIntToString();
                            break Label_5092;
                        }
                        default: {
                            throw new ConnectorException(14007, new Object[]{sourceType + " to " + targetType});
                        }
                    }
                }
                case 16: {
                    switch (targetType) {
                        case 4: {
                            converter = new ConnectorDataTypeConverter.BooleanToInteger();
                            if (falseDefVal != null) {
                                ((ConnectorDataTypeConverter.BooleanToInteger) converter).setFalseDefaultValue(falseDefVal.get(targetType));
                                break Label_5092;
                            }
                            break Label_5092;
                        }
                        case -5: {
                            converter = new ConnectorDataTypeConverter.BooleanToLong();
                            if (falseDefVal != null) {
                                ((ConnectorDataTypeConverter.BooleanToLong) converter).setFalseDefaultValue(falseDefVal.get(targetType));
                                break Label_5092;
                            }
                            break Label_5092;
                        }
                        case 5: {
                            converter = new ConnectorDataTypeConverter.BooleanToShort();
                            if (falseDefVal != null) {
                                ((ConnectorDataTypeConverter.BooleanToShort) converter).setFalseDefaultValue(falseDefVal.get(targetType));
                                break Label_5092;
                            }
                            break Label_5092;
                        }
                        case -6: {
                            converter = new ConnectorDataTypeConverter.BooleanToTinyInt();
                            if (falseDefVal != null) {
                                ((ConnectorDataTypeConverter.BooleanToTinyInt) converter).setFalseDefaultValue(falseDefVal.get(targetType));
                                break Label_5092;
                            }
                            break Label_5092;
                        }
                        case 6: {
                            converter = new ConnectorDataTypeConverter.BooleanToFloat();
                            if (falseDefVal != null) {
                                ((ConnectorDataTypeConverter.BooleanToFloat) converter).setFalseDefaultValue(falseDefVal.get(targetType));
                                break Label_5092;
                            }
                            break Label_5092;
                        }
                        case 7:
                        case 8: {
                            converter = new ConnectorDataTypeConverter.BooleanToDouble();
                            if (falseDefVal != null) {
                                ((ConnectorDataTypeConverter.BooleanToDouble) converter).setFalseDefaultValue(falseDefVal.get(targetType));
                                break Label_5092;
                            }
                            break Label_5092;
                        }
                        case 2: {
                            converter = new ConnectorDataTypeConverter.BooleanToBigDecimal();
                            if (falseDefVal != null) {
                                ((ConnectorDataTypeConverter.BooleanToBigDecimal) converter).setFalseDefaultValue(falseDefVal.get(targetType));
                                break Label_5092;
                            }
                            break Label_5092;
                        }
                        case 3: {
                            converter = new ConnectorDataTypeConverter.BooleanToBigDecimalWithScale();
                            if (falseDefVal != null) {
                                ((ConnectorDataTypeConverter.BooleanToBigDecimalWithScale) converter).setFalseDefaultValue(falseDefVal.get(targetType));
                            }
                            ((ConnectorDataTypeConverter.BooleanToBigDecimalWithScale) converter).setScale(targetScale);
                            ((ConnectorDataTypeConverter.BooleanToBigDecimalWithScale) converter).setPrecision(targetPrecision);
                            break Label_5092;
                        }
                        case -1:
                        case 1:
                        case 12:
                        case 2005: {
                            converter = new ConnectorDataTypeConverter.BooleanToString();
                            break Label_5092;
                        }
                        default: {
                            throw new ConnectorException(14007, new Object[]{sourceType + " to " + targetType});
                        }
                    }
                }
                case 6: {
                    switch (targetType) {
                        case 4: {
                            converter = new ConnectorDataTypeConverter.FloatToInteger();
                            break Label_5092;
                        }
                        case -5: {
                            converter = new ConnectorDataTypeConverter.FloatToLong();
                            break Label_5092;
                        }
                        case 5: {
                            converter = new ConnectorDataTypeConverter.FloatToShort();
                            break Label_5092;
                        }
                        case -6: {
                            converter = new ConnectorDataTypeConverter.FloatToTinyInt();
                            break Label_5092;
                        }
                        case 6: {
                            converter = new ConnectorDataTypeConverter.FloatToFloat();
                            break Label_5092;
                        }
                        case 7:
                        case 8: {
                            converter = new ConnectorDataTypeConverter.FloatToDouble();
                            break Label_5092;
                        }
                        case 2: {
                            converter = new ConnectorDataTypeConverter.FloatToBigDecimal();
                            break Label_5092;
                        }
                        case 3: {
                            converter = new ConnectorDataTypeConverter.FloatToBigDecimalWithScale();
                            ((ConnectorDataTypeConverter.FloatToBigDecimalWithScale) converter).setScale(targetScale);
                            ((ConnectorDataTypeConverter.FloatToBigDecimalWithScale) converter).setPrecision(targetPrecision);
                            break Label_5092;
                        }
                        case -1:
                        case 1:
                        case 12:
                        case 2005: {
                            converter = new ConnectorDataTypeConverter.FloatToString();
                            break Label_5092;
                        }
                        default: {
                            throw new ConnectorException(14007, new Object[]{sourceType + " to " + targetType});
                        }
                    }
                }
                case 7:
                case 8: {
                    switch (targetType) {
                        case 4: {
                            converter = new ConnectorDataTypeConverter.DoubleToInteger();
                            break Label_5092;
                        }
                        case -5: {
                            converter = new ConnectorDataTypeConverter.DoubleToLong();
                            break Label_5092;
                        }
                        case 5: {
                            converter = new ConnectorDataTypeConverter.DoubleToShort();
                            break Label_5092;
                        }
                        case -6: {
                            converter = new ConnectorDataTypeConverter.DoubleToTinyInt();
                            break Label_5092;
                        }
                        case 6: {
                            converter = new ConnectorDataTypeConverter.DoubleToFloat();
                            break Label_5092;
                        }
                        case 7:
                        case 8: {
                            converter = new ConnectorDataTypeConverter.DoubleToDouble();
                            break Label_5092;
                        }
                        case 2: {
                            converter = new ConnectorDataTypeConverter.DoubleToBigDecimal();
                            break Label_5092;
                        }
                        case 3: {
                            converter = new ConnectorDataTypeConverter.DoubleToBigDecimalWithScale();
                            ((ConnectorDataTypeConverter.DoubleToBigDecimalWithScale) converter).setScale(targetScale);
                            break Label_5092;
                        }
                        case -1:
                        case 1:
                        case 12:
                        case 2005: {
                            converter = new ConnectorDataTypeConverter.DoubleToString();
                            break Label_5092;
                        }
                        default: {
                            throw new ConnectorException(14007, new Object[]{sourceType + " to " + targetType});
                        }
                    }
                }
                case 2:
                case 3: {
                    switch (targetType) {
                        case 4: {
                            converter = new ConnectorDataTypeConverter.BigDecimalToInteger();
                            break Label_5092;
                        }
                        case -5: {
                            converter = new ConnectorDataTypeConverter.BigDecimalToLong();
                            break Label_5092;
                        }
                        case 5: {
                            converter = new ConnectorDataTypeConverter.BigDecimalToShort();
                            break Label_5092;
                        }
                        case -6: {
                            converter = new ConnectorDataTypeConverter.BigDecimalToTinyInt();
                            break Label_5092;
                        }
                        case 6: {
                            converter = new ConnectorDataTypeConverter.BigDecimalToFloat();
                            break Label_5092;
                        }
                        case 7:
                        case 8: {
                            converter = new ConnectorDataTypeConverter.BigDecimalToDouble();
                            break Label_5092;
                        }
                        case 2: {
                            converter = new ConnectorDataTypeConverter.BigDecimalToBigDecimal();
                            break Label_5092;
                        }
                        case 3: {
                            converter = new ConnectorDataTypeConverter.BigDecimalToBigDecimalWithScale();
                            ((ConnectorDataTypeConverter.BigDecimalToBigDecimalWithScale) converter).setScale(targetScale);
                            break Label_5092;
                        }
                        case 16: {
                            converter = new ConnectorDataTypeConverter.BigDecimalToBoolean();
                            break Label_5092;
                        }
                        case -1:
                        case 1:
                        case 12:
                        case 2005: {
                            converter = new ConnectorDataTypeConverter.BigDecimalToString();
                            break Label_5092;
                        }
                        default: {
                            throw new ConnectorException(14007, new Object[]{sourceType + " to " + targetType});
                        }
                    }
                }
                case -1:
                case 1:
                case 12: {
                    switch (targetType) {
                        case 4: {
                            converter = new ConnectorDataTypeConverter.StringToInteger();
                            break Label_5092;
                        }
                        case -5: {
                            converter = new ConnectorDataTypeConverter.StringToLong();
                            break Label_5092;
                        }
                        case 5: {
                            converter = new ConnectorDataTypeConverter.StringToShort();
                            break Label_5092;
                        }
                        case -6: {
                            converter = new ConnectorDataTypeConverter.StringToTinyInt();
                            break Label_5092;
                        }
                        case 6: {
                            converter = new ConnectorDataTypeConverter.StringToFloat();
                            break Label_5092;
                        }
                        case 7:
                        case 8: {
                            converter = new ConnectorDataTypeConverter.StringToDouble();
                            break Label_5092;
                        }
                        case 2: {
                            converter = new ConnectorDataTypeConverter.StringToBigDecimalUnbounded();
                            ((ConnectorDataTypeConverter.StringToBigDecimalUnbounded) converter).setScale(targetScale);
                            ((ConnectorDataTypeConverter.StringToBigDecimalUnbounded) converter).setPrecision(targetPrecision);
                            ((ConnectorDataTypeConverter.StringToBigDecimalUnbounded) converter).setLength(targetLength);
                            break Label_5092;
                        }
                        case 3: {
                            converter = new ConnectorDataTypeConverter.StringToBigDecimalWithScale();
                            ((ConnectorDataTypeConverter.StringToBigDecimalWithScale) converter).setScale(targetScale);
                            break Label_5092;
                        }
                        case -1:
                        case 1:
                        case 12:
                        case 2005: {
                            converter = new ConnectorDataTypeConverter.StringToString(ConnectorConfiguration.getStringTruncate(configuration), targetLength);
                            break Label_5092;
                        }
                        case 16: {
                            converter = new ConnectorDataTypeConverter.StringToBoolean();
                            break Label_5092;
                        }
                        case 91: {
                            converter = new ConnectorDataTypeConverter.StringFMTTZToDateTZ(inputDateFormat, inputTimezoneId, outputTimezoneId).setupBackupDateFormat(configuration);
                            break Label_5092;
                        }
                        case 92: {
                            converter = new ConnectorDataTypeConverter.StringFMTTZToTimeTZ(inputTimeFormat, inputTimezoneId, outputTimezoneId).setupBackupDateFormat(configuration);
                            break Label_5092;
                        }
                        case 93: {
                            converter = new ConnectorDataTypeConverter.StringFMTTZToTimestampTZ(inputTimestampFormat, inputTimezoneId, outputTimezoneId).setupBackupDateFormat(configuration);
                            break Label_5092;
                        }
                        case 2002: {
                            converter = new ConnectorDataTypeConverter.StringToPeriod();
                            break Label_5092;
                        }
                        case 1111: {
                            converter = new ConnectorDataTypeConverter.StringToInterval();
                            break Label_5092;
                        }
                        case -3:
                        case -2: {
                            converter = new ConnectorDataTypeConverter.StringToBytes();
                            break Label_5092;
                        }
                        case 2004: {
                            converter = new ConnectorDataTypeConverter.StringToBinary();
                            break Label_5092;
                        }
                        case 1886: {
                            converter = new ConnectorDataTypeConverter.StringFMTTZToCalendarTimestamp(inputTimestampFormat, inputTimezoneId, outputTimezoneId).setupBackupDateFormat(configuration, "tdch.input.timestamp.format");
                            break Label_5092;
                        }
                        case 1885: {
                            converter = new ConnectorDataTypeConverter.StringFMTTZToCalendarTime(inputTimeFormat, inputTimezoneId, outputTimezoneId).setupBackupDateFormat(configuration, "tdch.input.time.format");
                            break Label_5092;
                        }
                        default: {
                            throw new ConnectorException(14007, new Object[]{sourceType + " to " + targetType});
                        }
                    }
                }
                case 91: {
                    switch (targetType) {
                        case 4: {
                            converter = new ConnectorDataTypeConverter.DateTZToInteger(inputTimezoneId);
                            break Label_5092;
                        }
                        case -5: {
                            converter = new ConnectorDataTypeConverter.DateTZToLong(inputTimezoneId);
                            break Label_5092;
                        }
                        case 91: {
                            converter = new ConnectorDataTypeConverter.DateToDate();
                            break Label_5092;
                        }
                        case 93: {
                            converter = new ConnectorDataTypeConverter.DateTZToTimestampTZ(inputTimezoneId, outputTimezoneId);
                            break Label_5092;
                        }
                        case -1:
                        case 1:
                        case 12:
                        case 2005: {
                            converter = new ConnectorDataTypeConverter.DateToStringFMT(outputDateFormat);
                            break Label_5092;
                        }
                        case 1885:
                        case 1886: {
                            converter = new ConnectorDataTypeConverter.DateToCalendar(outputTimezoneId);
                            break Label_5092;
                        }
                        default: {
                            throw new ConnectorException(14007, new Object[]{sourceType + " to " + targetType});
                        }
                    }
                }
                case 92: {
                    switch (targetType) {
                        case 93: {
                            converter = new ConnectorDataTypeConverter.TimeToTimestamp();
                            break Label_5092;
                        }
                        case 92: {
                            converter = new ConnectorDataTypeConverter.TimeTZToTimeTZ(inputTimezoneId, outputTimezoneId);
                            break Label_5092;
                        }
                        case 4: {
                            converter = new ConnectorDataTypeConverter.TimeTZToInteger(inputTimezoneId);
                            break Label_5092;
                        }
                        case -5: {
                            converter = new ConnectorDataTypeConverter.TimeTZToLong(inputTimezoneId);
                            break Label_5092;
                        }
                        case -1:
                        case 1:
                        case 12:
                        case 2005: {
                            converter = new ConnectorDataTypeConverter.TimeTZToStringFMTTZ(outputTimeFormat, inputTimezoneId, outputTimezoneId);
                            break Label_5092;
                        }
                        case 1885:
                        case 1886: {
                            converter = new ConnectorDataTypeConverter.TimeTZToCalendar(inputTimezoneId, outputTimezoneId);
                            break Label_5092;
                        }
                        default: {
                            throw new ConnectorException(14007, new Object[]{sourceType + " to " + targetType});
                        }
                    }
                }
                case 93: {
                    switch (targetType) {
                        case 4: {
                            converter = new ConnectorDataTypeConverter.TimestampTZToInteger(inputTimezoneId);
                            break Label_5092;
                        }
                        case -5: {
                            converter = new ConnectorDataTypeConverter.TimestampTZToLong(inputTimezoneId);
                            break Label_5092;
                        }
                        case 91: {
                            converter = new ConnectorDataTypeConverter.TimestampTZToDateTZ(inputTimezoneId, outputTimezoneId);
                            break Label_5092;
                        }
                        case 92: {
                            converter = new ConnectorDataTypeConverter.TimestampTZToTimeTZ(inputTimezoneId, outputTimezoneId);
                            break Label_5092;
                        }
                        case 93: {
                            converter = new ConnectorDataTypeConverter.TimestampTZToTimestampTZ(inputTimezoneId, outputTimezoneId);
                            break Label_5092;
                        }
                        case -1:
                        case 1:
                        case 12:
                        case 2005: {
                            converter = new ConnectorDataTypeConverter.TimestampTZToStringFMTTZ(inputTimezoneId, outputTimezoneId, outputTimestampFormat);
                            break Label_5092;
                        }
                        case 1885:
                        case 1886: {
                            converter = new ConnectorDataTypeConverter.TimestampTZToCalendar(inputTimezoneId, outputTimezoneId);
                            break Label_5092;
                        }
                        default: {
                            throw new ConnectorException(14007, new Object[]{sourceType + " to " + targetType});
                        }
                    }
                }
                case 2005: {
                    switch (targetType) {
                        case -1:
                        case 1:
                        case 12:
                        case 2005: {
                            converter = new ConnectorDataTypeConverter.ClobToString();
                            break Label_5092;
                        }
                        default: {
                            throw new ConnectorException(14007, new Object[]{sourceType + " to " + targetType});
                        }
                    }
                }
                case 2004: {
                    switch (targetType) {
                        case -2: {
                            converter = new ConnectorDataTypeConverter.BlobToBinary();
                            break Label_5092;
                        }
                        case -1:
                        case 1:
                        case 12:
                        case 2004:
                        case 2005: {
                            converter = new ConnectorDataTypeConverter.BlobToString();
                            break Label_5092;
                        }
                        default: {
                            throw new ConnectorException(14007, new Object[]{sourceType + " to " + targetType});
                        }
                    }
                }
                case -3:
                case -2: {
                    switch (targetType) {
                        case -3:
                        case -2:
                        case 2004: {
                            converter = new ConnectorDataTypeConverter.BinaryToBinary();
                            break Label_5092;
                        }
                        case -1:
                        case 1:
                        case 12:
                        case 2005: {
                            converter = new ConnectorDataTypeConverter.BytesToString();
                            break Label_5092;
                        }
                        default: {
                            throw new ConnectorException(14007, new Object[]{sourceType + " to " + targetType});
                        }
                    }
                }
                case 2002: {
                    switch (targetType) {
                        case -1:
                        case 1:
                        case 12:
                        case 2002:
                        case 2005: {
                            converter = new ConnectorDataTypeConverter.PeriodToString();
                            break Label_5092;
                        }
                        default: {
                            throw new ConnectorException(14007, new Object[]{sourceType + " to " + targetType});
                        }
                    }
                }
                case 1111: {
                    switch (targetType) {
                        case -1:
                        case 1:
                        case 12:
                        case 1111:
                        case 2005: {
                            converter = new ConnectorDataTypeConverter.IntervalToString();
                            break Label_5092;
                        }
                        default: {
                            throw new ConnectorException(14007, new Object[]{sourceType + " to " + targetType});
                        }
                    }
                }
                case 1885:
                case 1886: {
                    switch (targetType) {
                        case 1885:
                        case 1886: {
                            converter = new ConnectorDataTypeConverter.CalendarToCalendar(outputTimezoneId);
                            break Label_5092;
                        }
                        case 92: {
                            converter = new ConnectorDataTypeConverter.CalendarToTimeTZ(outputTimezoneId);
                            break Label_5092;
                        }
                        case 93: {
                            converter = new ConnectorDataTypeConverter.CalendarToTimestampTZ(outputTimezoneId);
                            break Label_5092;
                        }
                        case 91: {
                            converter = new ConnectorDataTypeConverter.CalendarToDateTZ(outputTimezoneId);
                            break Label_5092;
                        }
                        case 4: {
                            converter = new ConnectorDataTypeConverter.CalendarToInteger();
                            break Label_5092;
                        }
                        case -5: {
                            converter = new ConnectorDataTypeConverter.CalendarToLong();
                            break Label_5092;
                        }
                        case 1:
                        case 12:
                        case 2005: {
                            if (sourceType == 1886) {
                                converter = new ConnectorDataTypeConverter.CalendarToStringFMTTZ(outputTimezoneId, outputTimestampFormat);
                                break Label_5092;
                            }
                            converter = new ConnectorDataTypeConverter.CalendarToStringFMTTZ(outputTimezoneId, outputTimeFormat);
                            break Label_5092;
                        }
                        default: {
                            throw new ConnectorException(14007, new Object[]{sourceType + " to " + targetType});
                        }
                    }
                }
                case 1882: {
                    final Map<Long, ConnectorDataTypeConverter> map = ConnectorSchemaUtils.constructConvertMap(this, configuration, targetScale, targetPrecision, targetLength);
                    converter = new ConnectorDataTypeConverter.ObjectToAnyType(map, targetType);
                    break;
                }
            }
        }
        if (sourceType != 16) {
            if (defaultVal != null) {
                converter.setDefaultValue(defaultVal.get(targetType));
            }
        } else if (trueDefVal != null) {
            converter.setDefaultValue(trueDefVal.get(targetType));
        }
        return converter;
    }

    public abstract void lookupConverter(final ConnectorRecordSchema p0) throws ConnectorException;
}
