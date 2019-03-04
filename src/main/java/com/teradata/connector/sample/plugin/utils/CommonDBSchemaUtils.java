package com.teradata.connector.sample.plugin.utils;

import com.teradata.connector.common.ConnectorPlugin;
import com.teradata.connector.common.exception.ConnectorException;
import com.teradata.connector.common.exception.ConnectorException.ErrorCode;
import com.teradata.connector.common.utils.ConnectorSchemaParser;
import com.teradata.connector.teradata.schema.TeradataColumnDesc;
import com.teradata.connector.teradata.schema.TeradataTableDesc;
import java.io.StringWriter;
import java.io.Writer;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import org.codehaus.jackson.map.ObjectMapper;


public class CommonDBSchemaUtils {
    private static final char COMMA_CHAR = ',';
    private static final char DOT_CHAR = '.';
    private static final char DOUBLE_QUOTE = '\"';
    private static final char ESCAPE_CHAR = '\\';
    public static final String LIST_COLUMNS = "columns";
    public static final String LIST_COLUMN_TYPES = "columns.types";
    protected static final String PATTERN_ARRAY_TYPE = "\\s*(array\\s*[<].*[>])";
    protected static final String PATTERN_MAP_TYPE = "\\s*(map\\s*[<].*[>])";
    protected static final String PATTERN_STRUCT_TYPE = "\\s*(struct\\s*[<].*[>])";
    protected static final String PATTERN_UNION_TYPE = "\\s*(union\\s*[<].*[>])";
    private static final String REPLACE_DOUBLE_QUOTE = "\\\"";
    private static final String REPLACE_DOUBLE_QUOTE_SQL = "\"\"";
    private static final char SINGLE_QUOTE = '\'';
    protected static final String SQL_SELECT_FROM_SOURCE_WHERE = "SELECT %s FROM %s %s";

    public static String quoteFieldName(String fieldName) {
        return quoteFieldName(fieldName, REPLACE_DOUBLE_QUOTE);
    }

    private static String quoteFieldName(String fieldName, String replaceQuoteString) {
        if (fieldName == null || fieldName.isEmpty()) {
            return String.format("%c%c", new Object[]{Character.valueOf(DOUBLE_QUOTE), Character.valueOf(DOUBLE_QUOTE)});
        }
        ConnectorSchemaParser parser = new ConnectorSchemaParser();
        parser.setDelimChar(DOT_CHAR);
        parser.setEscapeChar(ESCAPE_CHAR);
        List<String> tokens = parser.tokenize(fieldName);
        StringBuilder builder = new StringBuilder();
        for (String token : tokens) {
            String token2 = null;
            if (token2.length() > 1) {
                char begin = token2.charAt(0);
                char end = token2.charAt(token2.length() - 1);
                if ((begin == SINGLE_QUOTE || begin == DOUBLE_QUOTE) && begin == end) {
                    token2 = token2.substring(1, token2.length() - 1);
                }
            }
            builder.append(DOUBLE_QUOTE).append(token2.replace(String.valueOf(DOUBLE_QUOTE), replaceQuoteString)).append(DOUBLE_QUOTE).append(DOT_CHAR);
        }
        if (builder.length() > 0) {
            builder.setLength(builder.length() - 1);
        }
        return builder.toString();
    }

    public static String concatFieldNamesArray(String[] fieldNamesArray) {
        if (fieldNamesArray == null || fieldNamesArray.length == 0) {
            return "";
        }
        StringBuilder builder = new StringBuilder();
        for (String fieldName : fieldNamesArray) {
            builder.append(fieldName).append(COMMA_CHAR);
        }
        if (builder.length() > 0) {
            builder.setLength(builder.length() - 1);
        }
        return builder.toString();
    }

    public static String[] quoteFieldNamesArray(String[] fieldNamesArray) {
        if (fieldNamesArray == null) {
            return new String[0];
        }
        String[] newFieldNamesArray = new String[fieldNamesArray.length];
        for (int i = 0; i < newFieldNamesArray.length; i++) {
            newFieldNamesArray[i] = quoteFieldName(fieldNamesArray[i].trim());
        }
        return newFieldNamesArray;
    }

    public static int[] lookupMappingFromTableDescText(String tableDescText, String[] fieldNames) throws ConnectorException {
        if (tableDescText == null || tableDescText.length() <= 0) {
            throw new ConnectorException((int) ErrorCode.SCHEMA_DEFINITION_INVALID);
        } else if (fieldNames == null || fieldNames.length <= 0) {
            throw new ConnectorException((int) ErrorCode.FIELD_NAME_MISSING);
        } else {
            Map<String, Integer> schemaColumnNames = tableDescFromText(tableDescText).getColumnNameLowerCaseMap();
            ConnectorSchemaParser parser = new ConnectorSchemaParser();
            parser.setTrimSpaces(true);
            int fieldCount = fieldNames.length;
            parser.setDelimChar(DOT_CHAR);
            int[] mappings = new int[fieldCount];
            for (int i = 0; i < fieldCount; i++) {
                Integer columnPosition = (Integer) schemaColumnNames.get(fieldNames[i].toLowerCase());
                if (columnPosition == null) {
                    throw new ConnectorException((int) ErrorCode.FIELD_NAME_NOT_IN_SCHEMA);
                }
                mappings[i] = columnPosition.intValue();
            }
            return mappings;
        }
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

    public static int[] lookupTypesFromTableDescText(String tableDescText, String[] fieldNamesArray) throws ConnectorException {
        if (tableDescText == null || tableDescText.length() <= 0) {
            throw new ConnectorException((int) ErrorCode.SCHEMA_DEFINITION_INVALID);
        }
        TeradataColumnDesc[] columnDescs = tableDescFromText(tableDescText).getColumns();
        try {
            int[] mappings = lookupMappingFromTableDescText(tableDescText, fieldNamesArray);
            int realFieldCount = mappings.length;
            int[] fieldDataTypes = realFieldCount > 0 ? new int[realFieldCount] : null;
            for (int i = 0; i < realFieldCount; i++) {
                fieldDataTypes[i] = columnDescs[mappings[i]].getType();
            }
            return fieldDataTypes;
        } catch (ConnectorException e) {
            throw e;
        }
    }

    public static int[] lookupFieldsSQLDataScales(String tableDescText, String[] fieldNamesArray) throws ConnectorException {
        TeradataColumnDesc[] columnDescs = tableDescFromText(tableDescText).getColumns();
        try {
            int[] mappings = lookupMappingFromTableDescText(tableDescText, fieldNamesArray);
            int realFieldCount = mappings.length;
            int[] fieldDataScales = realFieldCount > 0 ? new int[realFieldCount] : null;
            for (int i = 0; i < realFieldCount; i++) {
                fieldDataScales[i] = columnDescs[mappings[i]].getScale();
            }
            return fieldDataScales;
        } catch (ConnectorException e) {
            throw e;
        }
    }

    public static String[] unquoteFieldNamesArray(String[] fieldNamesArray) {
        if (fieldNamesArray == null) {
            return new String[0];
        }
        String[] newFieldNamesArray = new String[fieldNamesArray.length];
        for (int i = 0; i < newFieldNamesArray.length; i++) {
            newFieldNamesArray[i] = unquoteFieldName(fieldNamesArray[i].trim());
        }
        return newFieldNamesArray;
    }

    public static String unquoteFieldName(String fieldName) {
        if (fieldName == null || fieldName.isEmpty()) {
            return "";
        }
        ConnectorSchemaParser parser = new ConnectorSchemaParser();
        parser.setDelimChar(DOT_CHAR);
        parser.setEscapeChar(ESCAPE_CHAR);
        List<String> tokens = parser.tokenize(fieldName);
        StringBuilder builder = new StringBuilder();
        for (String token : tokens) {
            String token2 = null;
            if (token2.length() > 1) {
                char begin = token2.charAt(0);
                char end = token2.charAt(token2.length() - 1);
                if ((begin == SINGLE_QUOTE || begin == DOUBLE_QUOTE) && begin == end) {
                    token2 = token2.substring(1, token2.length() - 1);
                }
            }
            builder.append(token2.replace(REPLACE_DOUBLE_QUOTE, String.valueOf(DOUBLE_QUOTE))).append(DOT_CHAR);
        }
        if (builder.length() > 0) {
            builder.setLength(builder.length() - 1);
        }
        return builder.toString();
    }

    public static String quoteFieldNameForSql(String fieldName) {
        return quoteFieldName(fieldName, REPLACE_DOUBLE_QUOTE_SQL);
    }

    public static int tranformTeradataDataType(int dataType) {
        if (dataType == 3 || dataType == 2) {
            return 3;
        }
        if (dataType == 8 || dataType == 6 || dataType == 8) {
            return 8;
        }
        if (dataType == 5 || dataType == 4 || dataType == -6) {
            return 4;
        }
        return dataType;
    }

    public static String getSelectSQL(String tableName, String[] columns) {
        StringBuilder colExpBuilder = new StringBuilder();
        if (columns == null || columns.length == 0) {
            colExpBuilder.append('*');
        } else {
            for (int i = 0; i < columns.length; i++) {
                if (i > 0) {
                    colExpBuilder.append(", ");
                }
                colExpBuilder.append(columns[i]);
            }
        }
        return String.format(SQL_SELECT_FROM_SOURCE_WHERE, new Object[]{colExpBuilder.toString(), tableName, ""});
    }

    protected static TeradataColumnDesc[] getColumnDescs(ResultSetMetaData metadata) throws SQLException {
        int columnCount = metadata.getColumnCount();
        TeradataColumnDesc[] columns = new TeradataColumnDesc[columnCount];
        for (int i = 1; i <= columnCount; i++) {
            TeradataColumnDesc column = new TeradataColumnDesc();
            column.setName(metadata.getColumnName(i));
            column.setType(metadata.getColumnType(i));
            column.setTypeName(metadata.getColumnTypeName(i));
            column.setClassName(metadata.getColumnClassName(i));
            column.setNullable(metadata.isNullable(i) > 0);
            column.setLength((long) metadata.getColumnDisplaySize(i));
            column.setCaseSensitive(metadata.isCaseSensitive(i));
            column.setPrecision(metadata.getPrecision(i));
            column.setScale(metadata.getScale(i));
            columns[i - 1] = column;
        }
        return columns;
    }

    public static TeradataColumnDesc[] getColumnDescsForSQL(String sql, Connection connection) throws SQLException {
        if (connection == null) {
            return null;
        }
        PreparedStatement stmt = connection.prepareStatement(sql);
        TeradataColumnDesc[] desc = getColumnDescs(stmt.getMetaData());
        stmt.close();
        return desc;
    }

    public static TeradataColumnDesc[] getColumnDesc(String tableName, String[] fieldNames, Connection connection) throws SQLException {
        return getColumnDescsForSQL(getSelectSQL(tableName, fieldNames), connection);
    }
}
