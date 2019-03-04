package com.teradata.connector.teradata.converter;

import com.teradata.connector.common.ConnectorRecord;
import com.teradata.connector.common.ConnectorRecordSchema;
import com.teradata.connector.common.converter.ConnectorConverter;
import com.teradata.connector.common.converter.ConnectorDataTypeConverter;
import com.teradata.connector.common.converter.ConnectorDataTypeConverter.StringToString;
import com.teradata.connector.common.converter.ConnectorDataTypeDefinition;
import com.teradata.connector.common.exception.ConnectorException;
import com.teradata.connector.common.exception.ConnectorException.ErrorCode;
import com.teradata.connector.common.utils.ConnectorConfiguration;
import com.teradata.connector.common.utils.ConnectorSchemaUtils;
import com.teradata.connector.common.utils.TDUnsupportedCharacterReplacer;
import com.teradata.connector.teradata.schema.TeradataColumnDesc;
import com.teradata.connector.teradata.schema.TeradataTableDesc;
import com.teradata.connector.teradata.utils.LogContainer;
import com.teradata.connector.teradata.utils.TeradataPlugInConfiguration;
import com.teradata.connector.teradata.utils.TeradataSchemaUtils;

import java.util.HashMap;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.JobContext;


public class TeradataConverter extends ConnectorConverter {
    protected static final int NUMBER_PRECISION_MAX = 38;
    protected TeradataTableDesc targetTableDesc;
    protected ConnectorRecordSchema targetRecordSchema;
    protected ConnectorRecordSchema sourceRecordSchema;
    protected int columnCount;
    protected ConnectorDataTypeConverter[] dataTypeConverters;
    protected Configuration configuration;
    protected ConnectorRecord targetRecord;

    public TeradataConverter() {
        this.targetTableDesc = null;
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
    public Map<Integer, Boolean> initializeNullable() throws ConnectorException {
        final Map<Integer, Boolean> nullableMap = new HashMap<Integer, Boolean>();
        for (int i = 0; i < this.columnCount; ++i) {
            final TeradataColumnDesc targetColumnDesc = this.targetTableDesc.getColumn(i);
            nullableMap.put(i, targetColumnDesc.isNullable());
        }
        return nullableMap;
    }

    @Override
    public Map<Integer, Object> initializeDefaultValue() throws ConnectorException {
        final Map<Integer, Object> defValMap = new HashMap<Integer, Object>();
        defValMap.put(4, TeradataDataTypeDefinition.INTEGER_NULL_VALUE);
        defValMap.put(-5, TeradataDataTypeDefinition.BIGINT_NULL_VALUE);
        defValMap.put(5, TeradataDataTypeDefinition.SMALLINT_NULL_VALUE);
        defValMap.put(-6, TeradataDataTypeDefinition.TINYINT_NULL_VALUE);
        defValMap.put(3, TeradataDataTypeDefinition.BIGDECIMAL_NULL_VALUE);
        defValMap.put(91, TeradataDataTypeDefinition.DATE_NULL_VALUE);
        defValMap.put(92, TeradataDataTypeDefinition.TIME_NULL_VALUE);
        defValMap.put(93, TeradataDataTypeDefinition.TIMESTAMP_NULL_VALUE);
        defValMap.put(8, TeradataDataTypeDefinition.DOUBLE_NULL_VALUE);
        defValMap.put(16, TeradataDataTypeDefinition.BOOLEAN_NULL_VALUE);
        defValMap.put(-2, TeradataDataTypeDefinition.BYTE_NULL_VALUE);
        defValMap.put(6, TeradataDataTypeDefinition.FLOAT_NULL_VALUE);
        defValMap.put(12, "");
        defValMap.put(2002, "");
        defValMap.put(1111, "");
        defValMap.put(2003, "");
        defValMap.put(2005, "");
        defValMap.put(1, "");
        defValMap.put(-1, "");
        defValMap.put(7, TeradataDataTypeDefinition.DOUBLE_NULL_VALUE);
        defValMap.put(2004, TeradataDataTypeDefinition.BYTE_NULL_VALUE);
        defValMap.put(2, TeradataDataTypeDefinition.BIGDECIMAL_NULL_VALUE);
        defValMap.put(-3, TeradataDataTypeDefinition.BYTE_NULL_VALUE);
        defValMap.put(2002, "");
        defValMap.put(1885, TeradataDataTypeDefinition.CALENDAR_NULL_VALUE);
        defValMap.put(1886, TeradataDataTypeDefinition.CALENDAR_NULL_VALUE);
        return defValMap;
    }

    @Override
    public Map<Integer, Object> initializeFalseDefaultValue() throws ConnectorException {
        final Map<Integer, Object> defVal = new HashMap<Integer, Object>();
        defVal.put(4, TeradataDataTypeDefinition.INTEGER_FALSE_VALUE);
        defVal.put(-5, TeradataDataTypeDefinition.LONG_FALSE_VALUE);
        defVal.put(5, TeradataDataTypeDefinition.SHORT_FALSE_VALUE);
        defVal.put(6, TeradataDataTypeDefinition.FLOAT_FALSE_VALUE);
        defVal.put(8, TeradataDataTypeDefinition.DOUBLE_FALSE_VALUE);
        defVal.put(7, TeradataDataTypeDefinition.DOUBLE_FALSE_VALUE);
        defVal.put(2, TeradataDataTypeDefinition.BIGDECIMAL_FALSE_VALUE);
        defVal.put(3, TeradataDataTypeDefinition.BIGDECIMAL_FALSE_VALUE);
        defVal.put(-2, TeradataDataTypeDefinition.BYTE_FALSE_VALUE);
        defVal.put(-6, TeradataDataTypeDefinition.TINYINT_FALSE_VALUE);
        return defVal;
    }

    @Override
    public Map<Integer, Object> initializeTrueDefaultValue() throws ConnectorException {
        final Map<Integer, Object> defVal = new HashMap<Integer, Object>();
        defVal.put(4, TeradataDataTypeDefinition.INTEGER_TRUE_VALUE);
        defVal.put(-5, TeradataDataTypeDefinition.LONG_TRUE_VALUE);
        defVal.put(5, TeradataDataTypeDefinition.SHORT_TRUE_VALUE);
        defVal.put(6, TeradataDataTypeDefinition.FLOAT_TRUE_VALUE);
        defVal.put(8, TeradataDataTypeDefinition.DOUBLE_TRUE_VALUE);
        defVal.put(7, TeradataDataTypeDefinition.DOUBLE_TRUE_VALUE);
        defVal.put(2, TeradataDataTypeDefinition.BIGDECIMAL_TRUE_VALUE);
        defVal.put(3, TeradataDataTypeDefinition.BIGDECIMAL_TRUE_VALUE);
        defVal.put(-2, TeradataDataTypeDefinition.BYTE_TRUE_VALUE);
        defVal.put(-6, TeradataDataTypeDefinition.TINYINT_TRUE_VALUE);
        return defVal;
    }

    @Override
    public int[] initializeScale() throws ConnectorException {
        final TeradataColumnDesc[] columns = this.targetTableDesc.getColumns();
        final int[] scaleArray = new int[columns.length];
        for (int i = 0; i < columns.length; ++i) {
            scaleArray[i] = columns[i].getScale();
        }
        return scaleArray;
    }

    @Override
    public int[] initializePrecision() throws ConnectorException {
        final TeradataColumnDesc[] columns = this.targetTableDesc.getColumns();
        final int[] precisionArray = new int[columns.length];
        for (int i = 0; i < columns.length; ++i) {
            precisionArray[i] = columns[i].getPrecision();
        }
        return precisionArray;
    }

    @Override
    public int[] initializeLength() throws ConnectorException {
        final TeradataColumnDesc[] columns = this.targetTableDesc.getColumns();
        final int[] lengthArray = new int[columns.length];
        for (int i = 0; i < columns.length; ++i) {
            lengthArray[i] = (int) columns[i].getLength();
        }
        return lengthArray;
    }

    @Override
    public ConnectorRecord convert(final ConnectorRecord sourceRecord) throws ConnectorException {
        final boolean replaceUnSupportedUnicodeChars = TeradataPlugInConfiguration.getReplaceUnSupportedUnicodeChars(this.configuration);
        for (int i = 0; i < this.columnCount; ++i) {
            Object sr = sourceRecord.get(i);
            if (replaceUnSupportedUnicodeChars && this.dataTypeConverters[i] instanceof ConnectorDataTypeConverter.StringToString) {
                sr = TDUnsupportedCharacterReplacer.evaluate(sr.toString(), '?');
            }
            String str = "";
            try {
                if (ConnectorConfiguration.getEnableHdfsLoggingFlag(this.configuration)) {
                    try {
                        this.targetRecord.set(i, this.dataTypeConverters[i].convert(sr));
                    } catch (Exception e) {
                        final String logDelimiter = ConnectorConfiguration.getLogDelimiter(this.configuration);
                        this.targetRecord.set(0, "skip");
                        for (int j = 0; j < this.columnCount; ++j) {
                            str = str + sourceRecord.get(j) + logDelimiter;
                        }
                        str = str + e + logDelimiter + "error column number : " + (i + 1) + logDelimiter + "error Data : " + sourceRecord.get(i);
                        LogContainer.getInstance().addLogToList(str, this.configuration);
                        i = this.columnCount;
                    }
                } else {
                    this.targetRecord.set(i, this.dataTypeConverters[i].convert(sr));
                }
            } catch (ConnectorException e2) {
                throw new ConnectorException(e2.getMessage(), e2);
            }
        }
        return this.targetRecord;
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
        this.targetTableDesc = TeradataSchemaUtils.tableDescFromText(TeradataPlugInConfiguration.getOutputTableDesc(this.configuration));
        this.columnCount = this.targetRecordSchema.getLength();
        this.targetRecord = new ConnectorRecord(this.columnCount);
        ConnectorConfiguration.setOutputConverterRecordSchema(this.configuration, ConnectorSchemaUtils.recordSchemaToString(this.targetRecordSchema));
    }
}
