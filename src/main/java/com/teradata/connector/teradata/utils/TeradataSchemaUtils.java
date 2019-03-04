package com.teradata.connector.teradata.utils;

import com.teradata.connector.common.ConnectorPlugin;
import com.teradata.connector.common.ConnectorRecordSchema;
import com.teradata.connector.common.converter.ConnectorDataTypeConverter;
import com.teradata.connector.common.converter.ConnectorDataTypeDefinition;
import com.teradata.connector.common.exception.ConnectorException;
import com.teradata.connector.common.exception.ConnectorException.ErrorCode;
import com.teradata.connector.common.utils.ConnectorConfiguration;
import com.teradata.connector.common.utils.ConnectorSchemaParser;
import com.teradata.connector.common.utils.ConnectorSchemaUtils;
import com.teradata.connector.teradata.converter.TeradataDataType;
import com.teradata.connector.teradata.db.TeradataConnection;
import com.teradata.connector.teradata.schema.TeradataColumnDesc;
import com.teradata.connector.teradata.schema.TeradataTableDesc;
import java.io.StringWriter;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import junit.framework.Assert;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.codehaus.jackson.map.ObjectMapper;


public class TeradataSchemaUtils {
    private static Log logger;
    protected static final String PATTERN_MAP_TYPE = "\\s*(map\\s*[<].*[>])";
    protected static final String PATTERN_ARRAY_TYPE = "\\s*(array\\s*[<].*[>])";
    protected static final String PATTERN_STRUCT_TYPE = "\\s*(struct\\s*[<].*[>])";
    protected static final String PATTERN_UNION_TYPE = "\\s*(union\\s*[<].*[>])";
    public static final String LIST_COLUMNS = "columns";
    public static final String LIST_COLUMN_TYPES = "columns.types";
    private static final char DOT_CHAR = '.';
    private static final char COMMA_CHAR = ',';
    private static final String REPLACE_DOUBLE_QUOTE_SQL = "\"\"";
    private static final String REPLACE_SINGLE_QUOTE_SQL = "''";

    public static String quoteFieldNameForSql(final String fieldName) {
        return ConnectorSchemaUtils.quoteFieldName(fieldName, "\"\"");
    }

    public static String quoteFieldValueForSql(final String fieldValue) {
        return ConnectorSchemaUtils.quoteFieldValue(fieldValue, "''");
    }

    public static String quoteFieldNamesForSql(final String fieldNames) {
        if (fieldNames == null || fieldNames.isEmpty()) {
            return "";
        }
        final String[] fieldNamesArray = ConnectorSchemaUtils.convertFieldNamesToArray(fieldNames);
        final StringBuilder builder = new StringBuilder();
        for (final String fieldName : fieldNamesArray) {
            builder.append(quoteFieldNameForSql(fieldName)).append(',');
        }
        if (builder.length() > 0) {
            builder.setLength(builder.length() - 1);
        }
        return builder.toString();
    }

    public static String unquoteFieldValueForSql(final String fieldValue) {
        return ConnectorSchemaUtils.unquoteFieldValue(fieldValue, "''");
    }

    public static void setupTeradataSourceTableSchema(final Configuration configuration, final TeradataConnection connection) throws ConnectorException {
        final String inputQuery = TeradataPlugInConfiguration.getInputQuery(configuration);
        final String inputDatabase = TeradataPlugInConfiguration.getInputDatabase(configuration);
        final String objectName;
        String inputTableName = objectName = TeradataPlugInConfiguration.getInputTable(configuration);
        inputTableName = TeradataConnection.getQuotedEscapedName(inputDatabase, inputTableName);
        final String[] inputTableFieldNames = TeradataPlugInConfiguration.getInputFieldNamesArray(configuration);
        TeradataColumnDesc[] fieldDescs = null;
        Label_0130:
        {
            if (objectName != null && !objectName.isEmpty()) {
                try {
                    fieldDescs = connection.getColumnDescsForTable(inputTableName, inputTableFieldNames);
                    break Label_0130;
                } catch (SQLException e) {
                    throw new ConnectorException(e.getMessage(), e);
                }
            }
            try {
                final String charset = TeradataConnection.getURLParamValue(TeradataPlugInConfiguration.getInputJdbcUrl(configuration), "CHARSET");
                fieldDescs = connection.getColumnDescsForSQLWithCharSet(inputQuery, charset);
            } catch (SQLException e) {
                throw new ConnectorException(e.getMessage(), e);
            }
        }
        final TeradataTableDesc sourceTableDesc = new TeradataTableDesc();
        sourceTableDesc.setColumns(fieldDescs);
        TeradataPlugInConfiguration.setInputTableDesc(configuration, tableDescToJson(sourceTableDesc));
        final int sourceFieldLen = inputTableFieldNames.length;
        if (sourceFieldLen == 0) {
            TeradataPlugInConfiguration.setInputFieldNamesArray(configuration, sourceTableDesc.getColumnNames());
        }
    }

    public static boolean isPrimaryIndexInFieldNames(final String[] indices, final String[] fieldNames) {
        if (indices.length > 0) {
            final String[] unquotedFieldNames = ConnectorSchemaUtils.unquoteFieldNamesArray(fieldNames);
            final List<String> indicesList = new ArrayList<String>();
            final List<String> unquotedFieldsList = new ArrayList<String>();
            for (int i = 0; i < indices.length; ++i) {
                indicesList.add(indices[i].toLowerCase());
            }
            for (int i = 0; unquotedFieldNames != null && i < fieldNames.length; ++i) {
                unquotedFieldsList.add(unquotedFieldNames[i].toLowerCase());
            }
            return unquotedFieldsList.containsAll(indicesList);
        }
        return true;
    }

    public static void setupTeradataTargetTableSchema(final Configuration configuration, final TeradataConnection connection) throws ConnectorException {
        final String outputDatabase = TeradataPlugInConfiguration.getOutputDatabase(configuration);
        String outputTableName = TeradataPlugInConfiguration.getOutputTable(configuration);
        int outputTableFieldCount = TeradataPlugInConfiguration.getOutputFieldCount(configuration);
        final String[] outputTableFieldNames = TeradataPlugInConfiguration.getOutputFieldNamesArray(configuration);
        final String objectName = outputTableName;
        outputTableName = TeradataConnection.getQuotedEscapedName(outputDatabase, outputTableName);
        TeradataColumnDesc[] fieldDescs;
        try {
            fieldDescs = connection.getColumnDescsForTable(outputTableName, outputTableFieldNames);
            if (outputTableFieldCount > 0 && outputTableFieldCount > fieldDescs.length) {
                throw new ConnectorException(13010);
            }
            outputTableFieldCount = fieldDescs.length;
            if (outputTableFieldNames != null && outputTableFieldNames.length > 0) {
                final String[] indices = connection.getPrimaryIndex(outputTableName);
                if (!isPrimaryIndexInFieldNames(indices, outputTableFieldNames)) {
                    throw new ConnectorException(13015);
                }
            }
            if (TeradataPlugInConfiguration.getOutputTeradataTruncate(configuration)) {
                connection.truncateTable(outputTableName);
            }
        } catch (SQLException e) {
            throw new ConnectorException(e.getMessage(), e);
        }
        final TeradataTableDesc targetTableDesc = new TeradataTableDesc();
        targetTableDesc.setDatabaseName(outputDatabase);
        targetTableDesc.setName(objectName);
        targetTableDesc.setColumns(fieldDescs);
        TeradataPlugInConfiguration.setOutputTableDesc(configuration, tableDescToJson(targetTableDesc));
        final String[] fieldNames = new String[outputTableFieldCount];
        for (int i = 0; i < outputTableFieldCount; ++i) {
            final TeradataColumnDesc fieldDesc = fieldDescs[i];
            fieldNames[i] = fieldDesc.getName();
        }
        TeradataPlugInConfiguration.setOutputFieldCount(configuration, outputTableFieldCount);
        TeradataPlugInConfiguration.setOutputFieldNamesArray(configuration, fieldNames);
    }

    public static void configureSourceConnectorRecordSchema(final Configuration configuration) throws ConnectorException {
        final ConnectorRecordSchema userRecordSchema = ConnectorSchemaUtils.recordSchemaFromString(ConnectorConfiguration.getInputConverterRecordSchema(configuration));
        final String[] sourceFieldNameArray = TeradataPlugInConfiguration.getInputFieldNamesArray(configuration);
        final boolean isUDMapper = !ConnectorConfiguration.getJobMapper(configuration).isEmpty();
        final boolean isUDReducer = !ConnectorConfiguration.getJobReducer(configuration).isEmpty();
        final boolean needReducerPhase = ConnectorConfiguration.getNumReducers(configuration) != 0;
        if (!isUDMapper && !isUDReducer && !needReducerPhase) {
            if (userRecordSchema != null && sourceFieldNameArray.length != userRecordSchema.getLength()) {
                throw new ConnectorException(14013);
            }
            final TeradataTableDesc sourceTableDesc = tableDescFromText(TeradataPlugInConfiguration.getInputTableDesc(configuration));
            final TeradataColumnDesc[] columnDescs = sourceTableDesc.getColumns();
            final ConnectorRecordSchema sourceRecordSchema = new ConnectorRecordSchema(sourceFieldNameArray.length);
            int index = 0;
            boolean findField = false;
            for (final String fieldName : sourceFieldNameArray) {
                findField = false;
                for (final TeradataColumnDesc column : columnDescs) {
                    if (fieldName.equalsIgnoreCase(column.getName())) {
                        sourceRecordSchema.setFieldType(index++, transformTeradataTimeType(tranformTeradataDataType(column.getType()), column.getTypeName()));
                        findField = true;
                        break;
                    }
                }
                if (!findField) {
                    throw new ConnectorException(14005);
                }
            }
            if (userRecordSchema != null) {
                final int columnCount = sourceRecordSchema.getLength();
                if (userRecordSchema.getLength() != columnCount) {
                    throw new ConnectorException(14013);
                }
                for (int i = 0; i < columnCount; ++i) {
                    if (userRecordSchema.getFieldType(i) != 1883 && userRecordSchema.getFieldType(i) != sourceRecordSchema.getFieldType(i)) {
                        if (userRecordSchema.getFieldType(i) != 6 || sourceRecordSchema.getFieldType(i) != 8) {
                            throw new ConnectorException(14015);
                        }
                        userRecordSchema.setFieldType(i, 8);
                    }
                }
            } else {
                ConnectorConfiguration.setInputConverterRecordSchema(configuration, ConnectorSchemaUtils.recordSchemaToString(ConnectorSchemaUtils.formalizeConnectorRecordSchema(sourceRecordSchema)));
            }
        }
    }

    public static void configureTargetConnectorRecordSchema(final Configuration configuration) throws ConnectorException {
        final ConnectorRecordSchema userRecordSchema = ConnectorSchemaUtils.recordSchemaFromString(ConnectorConfiguration.getOutputConverterRecordSchema(configuration));
        final String[] targetFieldNameArray = TeradataPlugInConfiguration.getOutputFieldNamesArray(configuration);
        if (userRecordSchema != null && targetFieldNameArray.length != userRecordSchema.getLength()) {
            throw new ConnectorException(14014);
        }
        final TeradataTableDesc tagetTableDesc = tableDescFromText(TeradataPlugInConfiguration.getOutputTableDesc(configuration));
        final TeradataColumnDesc[] columnDescs = tagetTableDesc.getColumns();
        final ConnectorRecordSchema targetRecordSchema = new ConnectorRecordSchema(targetFieldNameArray.length);
        int index = 0;
        boolean findField = false;
        for (final String fieldName : targetFieldNameArray) {
            findField = false;
            for (final TeradataColumnDesc column : columnDescs) {
                if (fieldName.equalsIgnoreCase(column.getName())) {
                    targetRecordSchema.setFieldType(index++, transformTeradataTimeType(column.getType(), column.getTypeName()));
                    findField = true;
                    break;
                }
            }
            if (!findField) {
                throw new ConnectorException(14005);
            }
        }
        if (userRecordSchema != null) {
            for (int columnCount = targetRecordSchema.getLength(), i = 0; i < columnCount; ++i) {
                if (userRecordSchema.getFieldType(i) != targetRecordSchema.getFieldType(i)) {
                    throw new ConnectorException(14016);
                }
            }
        } else {
            ConnectorConfiguration.setOutputConverterRecordSchema(configuration, ConnectorSchemaUtils.recordSchemaToString(ConnectorSchemaUtils.formalizeConnectorRecordSchema(targetRecordSchema)));
        }
    }

    public static int transformTeradataTimeType(final int dataType, final String typeName) {
        if (dataType == 92) {
            if (ConnectorSchemaUtils.isTimeWithTimeZoneType(typeName)) {
                return 1885;
            }
        } else if (dataType == 93) {
            if (ConnectorSchemaUtils.isTimestampWithTimeZoneType(typeName)) {
                return 1886;
            }
        } else if (dataType == 91 && ConnectorSchemaUtils.isDateType(typeName)) {
            return 91;
        }
        return dataType;
    }

    public static int tranformTeradataDataType(final int dataType) {
        if (dataType == 3 || dataType == 2) {
            return 3;
        }
        if (dataType == 6) {
            return 8;
        }
        if (dataType == 5 || dataType == 4 || dataType == -6) {
            return 4;
        }
        return dataType;
    }

    public static String getStageTableName(final int maxLength, final String tableName, final String bkupTableName) {
        final int tableNameLen = (tableName == null) ? 0 : tableName.length();
        final int bkupTableNameLen = (bkupTableName == null) ? 0 : bkupTableName.length();
        final SimpleDateFormat sdf = new SimpleDateFormat("hhmmssSSS");
        final String identifier = sdf.format(new Date());
        final int identifierLen = identifier.length();
        String stageTableName;
        if (tableNameLen != 0 && tableNameLen + 1 + identifierLen <= maxLength) {
            stageTableName = tableName + "_" + identifier;
        } else if (bkupTableNameLen != 0 && bkupTableNameLen + 1 + identifierLen <= maxLength) {
            stageTableName = bkupTableName + "_" + identifier;
        } else {
            stageTableName = "TDCH_STAGE_" + identifier;
            stageTableName = stageTableName.substring(0, maxLength);
        }
        return stageTableName;
    }

    public static int[] lookupMappingFromTableDescText(final String tableDescText, final String[] fieldNames) throws ConnectorException {
        if (tableDescText == null || tableDescText.length() <= 0) {
            throw new ConnectorException(14012);
        }
        if (fieldNames == null || fieldNames.length <= 0) {
            throw new ConnectorException(14004);
        }
        final TeradataTableDesc schemaDesc = tableDescFromText(tableDescText);
        final Map<String, Integer> schemaColumnNames = schemaDesc.getColumnNameLowerCaseMap();
        final ConnectorSchemaParser parser = new ConnectorSchemaParser();
        parser.setTrimSpaces(true);
        final int fieldCount = fieldNames.length;
        parser.setDelimChar('.');
        final int[] mappings = new int[fieldCount];
        for (int i = 0; i < fieldCount; ++i) {
            final String fieldName = fieldNames[i].toLowerCase();
            final Integer columnPosition = schemaColumnNames.get(fieldName);
            if (columnPosition == null) {
                TeradataSchemaUtils.logger.error((Object) ("Column \"" + fieldName + "\" is not in schema " + tableDescText));
                throw new ConnectorException(14005);
            }
            mappings[i] = columnPosition;
        }
        return mappings;
    }

    public static int[] lookupFieldsSQLDataScales(final String tableDescText, final String[] fieldNamesArray) throws ConnectorException {
        final TeradataTableDesc tableDesc = tableDescFromText(tableDescText);
        final TeradataColumnDesc[] columnDescs = tableDesc.getColumns();
        int[] mappings = null;
        int[] fieldDataScales = null;
        int realFieldCount = 0;
        try {
            mappings = lookupMappingFromTableDescText(tableDescText, fieldNamesArray);
            realFieldCount = mappings.length;
            fieldDataScales = (int[]) ((realFieldCount > 0) ? new int[realFieldCount] : null);
            for (int i = 0; i < realFieldCount; ++i) {
                fieldDataScales[i] = columnDescs[mappings[i]].getScale();
            }
        } catch (ConnectorException e) {
            throw e;
        }
        return fieldDataScales;
    }

    public static int[] lookupTypesFromTableDescText(final String tableDescText, final String[] fieldNamesArray) throws ConnectorException {
        if (tableDescText == null || tableDescText.length() <= 0) {
            throw new ConnectorException(14012);
        }
        final TeradataTableDesc tableDesc = tableDescFromText(tableDescText);
        final TeradataColumnDesc[] columnDescs = tableDesc.getColumns();
        int[] mappings = null;
        int[] fieldDataTypes = null;
        int realFieldCount = 0;
        try {
            mappings = lookupMappingFromTableDescText(tableDescText, fieldNamesArray);
            realFieldCount = mappings.length;
            fieldDataTypes = (int[]) ((realFieldCount > 0) ? new int[realFieldCount] : null);
            for (int i = 0; i < realFieldCount; ++i) {
                fieldDataTypes[i] = columnDescs[mappings[i]].getType();
            }
        } catch (ConnectorException e) {
            throw e;
        }
        return fieldDataTypes;
    }

    public static HashMap<String, Object> lookupTeradataDataTypes(final String tableDescText, final String[] fieldNamesArray) throws ConnectorException {
        final TeradataDataType[] fieldDataTypes = new TeradataDataType[fieldNamesArray.length];
        final HashMap<String, Object> retHashMap = new HashMap<String, Object>();
        boolean SQLXMLEXISTS = false;
        final TeradataTableDesc tableDesc = tableDescFromText(tableDescText);
        final TeradataColumnDesc[] columnDescs = tableDesc.getColumns();
        int index = 0;
        boolean findField = false;
        for (final String fieldName : fieldNamesArray) {
            findField = false;
            for (final TeradataColumnDesc column : columnDescs) {
                if (fieldName.equalsIgnoreCase(column.getName())) {
                    if (column.getType() == 2009) {
                        fieldDataTypes[index++] = TeradataDataType.TeradataDataTypeImpl.SQLXML;
                        SQLXMLEXISTS = true;
                    } else {
                        fieldDataTypes[index++] = TeradataDataType.TeradataDataTypeImpl.OTHER;
                    }
                    findField = true;
                    break;
                }
            }
            if (!findField) {
                throw new ConnectorException(14005);
            }
        }
        retHashMap.put("SQLXMLEXISTS", SQLXMLEXISTS);
        retHashMap.put("TeradataDataType", fieldDataTypes);
        return retHashMap;
    }

    public static String[] lookupTypeNamesFromTableDescText(final String tableDescText, final String[] fieldNamesArray) throws ConnectorException {
        if (tableDescText == null || tableDescText.length() <= 0) {
            throw new ConnectorException(14012);
        }
        final TeradataTableDesc tableDesc = tableDescFromText(tableDescText);
        final TeradataColumnDesc[] columnDescs = tableDesc.getColumns();
        int[] mappings = null;
        String[] fieldDataTypeNames = null;
        int realFieldCount = 0;
        try {
            mappings = lookupMappingFromTableDescText(tableDescText, fieldNamesArray);
            realFieldCount = mappings.length;
            Assert.assertTrue(realFieldCount > 0);
            fieldDataTypeNames = new String[realFieldCount];
            for (int i = 0; i < realFieldCount; ++i) {
                fieldDataTypeNames[i] = columnDescs[mappings[i]].getTypeName();
            }
        } catch (ConnectorException e) {
            throw e;
        }
        return fieldDataTypeNames;
    }

    public static String tableDescToJson(TeradataTableDesc tableDesc) {
        StringBuilder sb = new StringBuilder();
        sb.append("{\"database\":\"").append(tableDesc.getDatabaseName()).append("\"");
        sb.append(",\"table\":\"").append(tableDesc.getName()).append("\"");
        sb.append(",\"columns\":[");
        TeradataColumnDesc[] columns = tableDesc.getColumns();
        int columnCount = columns.length;
        for (int i = 0; i < columnCount; i++) {
            int i2;
            try {
                ObjectMapper om = new ObjectMapper();
                StringWriter writer = new StringWriter();
                om.writeValue(writer, columns[i].getName());
                sb.append("{\"name\":").append(writer.toString());
            } catch (Exception e) {
                sb.append("{\"name\":\"").append(columns[i].getName()).append("\"");
            }
            sb.append(",\"type\":").append(columns[i].getType());
            sb.append(",\"typename\":\"").append(columns[i].getTypeName()).append("\"");
            StringBuilder append = sb.append(",\"nullable\":");
            if (columns[i].isNullable()) {
                i2 = 1;
            } else {
                i2 = 0;
            }
            append.append(i2);
            sb.append(",\"format\":\"").append(columns[i].getFormat()).append("\"");
            sb.append(",\"length\":").append(columns[i].getLength());
            sb.append(",\"chartype\":").append(columns[i].getCharType());
            append = sb.append(",\"casesensitive\":");
            if (columns[i].isCaseSensitive()) {
                i2 = 1;
            } else {
                i2 = 0;
            }
            append.append(i2);
            sb.append(",\"scale\":").append(columns[i].getScale());
            sb.append(",\"precision\":").append(columns[i].getPrecision());
            sb.append("},");
        }
        sb.setCharAt(sb.length() - 1, ']');
        sb.append("}}");
        return sb.toString();
    }


    public static TeradataTableDesc tableDescFromText(String tableDescText) {
        TeradataTableDesc tableDesc = new TeradataTableDesc();
        try {
            Map<String, Object> tableMap = (Map) new ObjectMapper().readValue(tableDescText, Map.class);
            tableDesc.setDatabaseName((String) tableMap.get("database"));
            tableDesc.setName((String) tableMap.get("table"));
            ArrayList<Map<String, Object>> columnList = (ArrayList) tableMap.get("columns");
            for (int i = 0; i < columnList.size(); i++) {
                boolean z;
                TeradataColumnDesc columnDesc = new TeradataColumnDesc();
                Map<String, Object> columnMap = (Map) columnList.get(i);
                columnDesc.setName((String) columnMap.get(ConnectorPlugin.TAG_NAME));
                columnDesc.setType(((Integer) columnMap.get("type")).intValue());
                columnDesc.setTypeName((String) columnMap.get("typename"));
                columnDesc.setFormat((String) columnMap.get("format"));
                if (((Integer) columnMap.get("nullable")).intValue() > 0) {
                    z = true;
                } else {
                    z = false;
                }
                columnDesc.setNullable(z);
                Object lenObject = columnMap.get("length");
                columnDesc.setCharType(((Integer) columnMap.get("chartype")).intValue());
                if (((Integer) columnMap.get("casesensitive")).intValue() > 0) {
                    z = true;
                } else {
                    z = false;
                }
                columnDesc.setCaseSensitive(z);
                if (lenObject instanceof Integer) {
                    columnDesc.setLength((long) ((Integer) lenObject).intValue());
                } else if (lenObject instanceof Long) {
                    columnDesc.setLength(((Long) lenObject).longValue());
                }
                columnDesc.setScale(((Integer) columnMap.get("scale")).intValue());
                columnDesc.setPrecision(((Integer) columnMap.get("precision")).intValue());
                tableDesc.addColumn(columnDesc);
            }
        } catch (Exception e) {
        }
        return tableDesc;
    }


    public static TeradataColumnDesc[] addTaskIDColumn(final TeradataColumnDesc[] inputColumns, final String colName) {
        final TeradataColumnDesc[] outputColumns = new TeradataColumnDesc[inputColumns.length + 1];
        for (int i = 0; i < inputColumns.length; ++i) {
            outputColumns[i] = new TeradataColumnDesc(inputColumns[i]);
        }
        (outputColumns[inputColumns.length] = new TeradataColumnDesc()).setName(colName);
        outputColumns[inputColumns.length].setType(12);
        outputColumns[inputColumns.length].setLength(40L);
        return outputColumns;
    }

    public static int[] getColumnMapping(final List<String> ColNames, final String[] mappingNames) {
        final Map<String, Integer> map = new HashMap<String, Integer>();
        final String[] ColNamesArray = new String[ColNames.size()];
        ColNames.toArray(ColNamesArray);
        for (int i = 0; i < ColNamesArray.length; ++i) {
            map.put(ColNamesArray[i].toLowerCase(), i);
        }
        final int[] mapping = new int[mappingNames.length];
        for (int j = 0; j < mappingNames.length; ++j) {
            mapping[j] = map.get(mappingNames[j].toLowerCase());
        }
        return mapping;
    }

    public static String getFieldNamesFromSchema(final String schema) throws ConnectorException {
        final TeradataTableDesc tableDesc = columnSchemaToTableDesc(schema);
        final StringBuilder sb = new StringBuilder();
        final TeradataColumnDesc[] columns2;
        final TeradataColumnDesc[] columns = columns2 = tableDesc.getColumns();
        for (final TeradataColumnDesc column : columns2) {
            sb.append(ConnectorSchemaUtils.quoteFieldName(column.getName())).append(',');
        }
        if (sb.length() > 0) {
            sb.setLength(sb.length() - 1);
        }
        return sb.toString();
    }

    private static int lookupTeradataDataTypeByTypeName(String typeName) {
        typeName = typeName.toUpperCase();
        if (typeName.equals("INT") || typeName.equals("INTEGER")) {
            return 4;
        }
        if (typeName.equals("BIGINT") || typeName.equals("LONG")) {
            return -5;
        }
        if (typeName.equals("SMALLINT")) {
            return 5;
        }
        if (typeName.equals("STRING") || typeName.equals("VARCHAR")) {
            return 12;
        }
        if (typeName.equals("FLOAT")) {
            return 6;
        }
        if (typeName.equals("DOUBLE") || typeName.equals("DOUBLE PRECISION")) {
            return 8;
        }
        if (typeName.equals("DECIMAL")) {
            return 3;
        }
        if (typeName.equals("BOOLEAN")) {
            return 16;
        }
        if (typeName.equals("DATE")) {
            return 91;
        }
        if (typeName.equals("TIME")) {
            return 92;
        }
        if (typeName.equals("TIMESTAMP")) {
            return 93;
        }
        if (typeName.startsWith("PERIOD")) {
            return 2002;
        }
        if (typeName.startsWith("INTERVAL")) {
            return 1111;
        }
        if (typeName.equals("CHAR")) {
            return 1;
        }
        if (typeName.equals("TINYINT")) {
            return -6;
        }
        if (typeName.equals("LONGVARCHAR")) {
            return -1;
        }
        if (typeName.equals("CLOB")) {
            return 2005;
        }
        if (typeName.equals("BLOB")) {
            return 2004;
        }
        if (typeName.equals("BINARY") || typeName.equals("VARBINARY") || typeName.equals("BYTES")) {
            return -2;
        }
        if (typeName.equals("REAL")) {
            return 8;
        }
        if (typeName.equals("NUMERIC")) {
            return 3;
        }
        if (typeName.equals("OTHER")) {
            return 1882;
        }
        return 1883;
    }

    public static TeradataTableDesc columnSchemaToTableDesc(final String tableColumnSchema) throws ConnectorException {
        final TeradataTableDesc tableDesc = new TeradataTableDesc();
        final List<String> columns = ConnectorSchemaUtils.parseColumns(tableColumnSchema);
        final List<String> columnNames = ConnectorSchemaUtils.parseColumnNames(columns);
        final List<String> columnTypes = ConnectorSchemaUtils.parseColumnTypes(columns);
        for (int i = 0; i < columnNames.size(); ++i) {
            final TeradataColumnDesc columnDesc = new TeradataColumnDesc();
            final String columnName = ConnectorSchemaUtils.unquoteFieldName(columnNames.get(i).trim());
            columnDesc.setName(columnName);
            columnDesc.setType(lookupTeradataDataTypeByTypeName(columnTypes.get(i).trim()));
            columnDesc.setTypeName(columnTypes.get(i).trim());
            columnDesc.setNullable(true);
            tableDesc.addColumn(columnDesc);
        }
        return tableDesc;
    }

    public static Timestamp getTimestampFromCalendar(final Calendar cal) {
        long millisec = cal.getTimeInMillis();
        millisec = ConnectorDataTypeConverter.convertMillisecFromDefaultToTargetTZ(millisec, cal.getTimeZone());
        return new Timestamp(millisec);
    }

    static {
        TeradataSchemaUtils.logger = LogFactory.getLog((Class) TeradataSchemaUtils.class);
    }
}
