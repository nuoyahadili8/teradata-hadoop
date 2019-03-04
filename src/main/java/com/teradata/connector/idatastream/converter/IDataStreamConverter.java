package com.teradata.connector.idatastream.converter;

import com.teradata.connector.common.ConnectorRecord;
import com.teradata.connector.common.ConnectorRecordSchema;
import com.teradata.connector.common.converter.ConnectorConverter;
import com.teradata.connector.common.converter.ConnectorDataTypeConverter;
import com.teradata.connector.common.exception.ConnectorException;
import com.teradata.connector.common.utils.ConnectorConfiguration;
import com.teradata.connector.common.utils.ConnectorSchemaUtils;
import com.teradata.connector.idatastream.schema.IDataStreamColumnDesc;
import com.teradata.connector.idatastream.utils.IDataStreamPlugInConfiguration;
import com.teradata.connector.idatastream.utils.IDataStreamUtils;
import com.teradata.connector.teradata.converter.TeradataDataTypeDefinition;
import java.util.HashMap;
import java.util.Map;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.JobContext;


/**
 * @author Administrator
 */
public class IDataStreamConverter extends ConnectorConverter {
    protected int columnCount= 0;
    protected Configuration configuration;
    protected ConnectorRecord targetRecord = null;
    protected ConnectorDataTypeConverter[] dataTypeConverters = null;
    protected IDataStreamColumnDesc[] columns = null;

    @Override
    public void initialize(final JobContext context) throws ConnectorException {
        this.configuration = context.getConfiguration();
        final ConnectorRecordSchema sourceRecordSchema = ConnectorSchemaUtils.recordSchemaFromString(ConnectorConfiguration.getInputConverterRecordSchema(this.configuration));
        final ConnectorRecordSchema targetRecordSchema = ConnectorSchemaUtils.recordSchemaFromString(ConnectorConfiguration.getOutputConverterRecordSchema(this.configuration));
        if (targetRecordSchema == null || targetRecordSchema.getLength() == 0) {
            throw new ConnectorException(14018);
        }
        if (sourceRecordSchema != null && sourceRecordSchema.getLength() != 0 && sourceRecordSchema.getLength() != targetRecordSchema.getLength()) {
            throw new ConnectorException(14017);
        }
        this.columnCount = targetRecordSchema.getLength();
        this.targetRecord = new ConnectorRecord(this.columnCount);
        final String[] outputFieldTypes = IDataStreamPlugInConfiguration.getOutputFieldTypesArray(this.configuration);
        final int outputFieldCount = outputFieldTypes.length;
        this.columns = new IDataStreamColumnDesc[outputFieldCount];
        for (int i = 0; i < outputFieldCount; ++i) {
            this.columns[i] = IDataStreamUtils.getIDataStreamColumnDescFromString(outputFieldTypes[i]);
        }
    }

    @Override
    public int[] initializeScale() throws ConnectorException {
        final int[] scaleArray = new int[this.columns.length];
        for (int i = 0; i < this.columns.length; ++i) {
            scaleArray[i] = this.columns[i].getScale();
        }
        return scaleArray;
    }

    @Override
    public int[] initializePrecision() throws ConnectorException {
        final int[] precisionArray = new int[this.columns.length];
        for (int i = 0; i < this.columns.length; ++i) {
            precisionArray[i] = this.columns[i].getPrecision();
        }
        return precisionArray;
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
        return null;
    }

    @Override
    public Map<Integer, Object> initializeFalseDefaultValue() throws ConnectorException {
        final Map<Integer, Object> defVal = new HashMap<>();
        defVal.put(4, TeradataDataTypeDefinition.INTEGER_FALSE_VALUE);
        defVal.put(-5, TeradataDataTypeDefinition.LONG_FALSE_VALUE);
        defVal.put(5, TeradataDataTypeDefinition.SHORT_FALSE_VALUE);
        defVal.put(6, TeradataDataTypeDefinition.FLOAT_FALSE_VALUE);
        defVal.put(8, TeradataDataTypeDefinition.DOUBLE_FALSE_VALUE);
        defVal.put(7, TeradataDataTypeDefinition.DOUBLE_FALSE_VALUE);
        defVal.put(2, TeradataDataTypeDefinition.BIGDECIMAL_FALSE_VALUE);
        defVal.put(3, TeradataDataTypeDefinition.BIGDECIMAL_FALSE_VALUE);
        defVal.put(-2, TeradataDataTypeDefinition.BYTE_FALSE_VALUE);
        return defVal;
    }

    @Override
    public Map<Integer, Object> initializeTrueDefaultValue() throws ConnectorException {
        final Map<Integer, Object> defVal = new HashMap<>();
        defVal.put(4, TeradataDataTypeDefinition.INTEGER_TRUE_VALUE);
        defVal.put(-5, TeradataDataTypeDefinition.LONG_TRUE_VALUE);
        defVal.put(5, TeradataDataTypeDefinition.SHORT_TRUE_VALUE);
        defVal.put(6, TeradataDataTypeDefinition.FLOAT_TRUE_VALUE);
        defVal.put(8, TeradataDataTypeDefinition.DOUBLE_TRUE_VALUE);
        defVal.put(7, TeradataDataTypeDefinition.DOUBLE_TRUE_VALUE);
        defVal.put(2, TeradataDataTypeDefinition.BIGDECIMAL_TRUE_VALUE);
        defVal.put(3, TeradataDataTypeDefinition.BIGDECIMAL_TRUE_VALUE);
        defVal.put(-2, TeradataDataTypeDefinition.BYTE_TRUE_VALUE);
        return defVal;
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
    public void lookupConverter(final ConnectorRecordSchema sourceRecordSchema) throws ConnectorException {
        this.dataTypeConverters = super.lookupConverter(this.configuration, sourceRecordSchema);
    }
}
