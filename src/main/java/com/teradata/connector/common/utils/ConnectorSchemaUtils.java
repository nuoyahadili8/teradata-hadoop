package com.teradata.connector.common.utils;

import com.teradata.connector.common.*;
import org.apache.commons.codec.binary.*;
import com.teradata.connector.common.exception.*;
import java.io.*;

import org.apache.commons.codec.binary.Base64;
import org.apache.hadoop.hive.serde2.io.*;
import java.lang.reflect.*;
import java.util.Date;
import java.util.regex.*;

import org.apache.hadoop.hive.serde2.io.ByteWritable;
import org.apache.hadoop.hive.serde2.io.DoubleWritable;
import org.apache.hadoop.hive.serde2.io.ShortWritable;
import org.codehaus.jackson.map.*;
import org.codehaus.jackson.node.*;
import org.apache.hadoop.io.*;
import java.math.*;
import java.util.*;
import com.teradata.jdbc.*;
import java.sql.*;
import com.teradata.connector.common.converter.*;
import org.apache.hadoop.conf.*;

public class ConnectorSchemaUtils
{
    public static final String PATTERN_MAP_TYPE = "\\s*(map\\s*[<].*[>])";
    public static final String PATTERN_ARRAY_TYPE = "\\s*(array\\s*[<].*[>])";
    public static final String PATTERN_STRUCT_TYPE = "\\s*(struct\\s*[<].*[>])";
    public static final String PATTERN_UNION_TYPE = "\\s*(union\\s*[<].*[>])";
    private static final String PATTERN_CALENDAR_TIME_TYPE = "\\s*time\\s*with\\s*time\\s*zone\\s*";
    private static final String PATTERN_CALENDAR_TIMESTAMP_TYPE = "\\s*timestamp\\s*with\\s*time\\s*zone\\s*";
    protected static final String PATTERN_DECIMAL_TYPE = "\\s*DECIMAL\\s*\\(\\s*(\\d+)\\s*(,\\s*(\\d+)\\s*)?\\)";
    protected static final String PATTERN_VARCHAR_TYPE = "\\s*VARCHAR\\s*\\(\\s*(\\d+)\\s*\\)";
    protected static final String PATTERN_CHAR_TYPE = "\\s*CHAR\\s*\\(\\s*(\\d+)\\s*\\)";
    private static final String PATTERN_DATE_TYPE = "\\s*date";
    private static final String PATTERN_TIME_TYPE = "\\s*time";
    public static final String LIST_COLUMNS = "columns";
    public static final String LIST_COLUMN_TYPES = "columns.types";
    private static final char ESCAPE_CHAR = '\\';
    private static final char SPACE_CHAR = ' ';
    private static final char DOT_CHAR = '.';
    private static final char COMMA_CHAR = ',';
    private static final char SINGLE_QUOTE = '\'';
    private static final char DOUBLE_QUOTE = '\"';
    private static final String REPLACE_DOUBLE_QUOTE = "\\\"";
    private static final String REPLACE_SINGLE_QUOTE = "\\'";

    public static List<String> parseColumns(final String schema) {
        final ConnectorSchemaParser parser = new ConnectorSchemaParser();
        parser.setEscapeChar('\\');
        return parser.tokenizeKeepEscape(schema);
    }

    public static List<String> parseFields(final String fields) {
        final ConnectorSchemaParser parser = new ConnectorSchemaParser();
        parser.setEscapeChar('\\');
        return parser.tokenize(fields);
    }

    public static List<String> parseColumnNames(final List<String> fields) {
        final ConnectorSchemaParser parser = new ConnectorSchemaParser();
        parser.setDelimChar(' ');
        parser.setEscapeChar('\\');
        parser.setIgnoreContinousDelim(true);
        final List<String> result = new ArrayList<String>();
        for (final String field : fields) {
            final List<String> tokens = parser.tokenize(field.trim(), 2, false);
            if (tokens.size() >= 1) {
                result.add(tokens.get(0).trim().replaceAll("\"", ""));
            }
            else {
                result.add("");
            }
        }
        return result;
    }

    public static List<String> parseColumnTypes(final List<String> fields) {
        final ConnectorSchemaParser parser = new ConnectorSchemaParser();
        parser.setDelimChar(' ');
        parser.setEscapeChar('\\');
        parser.setIgnoreContinousDelim(true);
        final List<String> result = new ArrayList<String>();
        for (final String field : fields) {
            final List<String> tokens = parser.tokenize(field.trim(), 2, false);
            if (tokens.size() == 2) {
                result.add(tokens.get(1).trim());
            }
            else {
                result.add("");
            }
        }
        return result;
    }

    public static String[] convertFieldNamesToArray(final String fieldNames) {
        final List<String> fields = parseFields(fieldNames);
        return fields.toArray(new String[0]);
    }

    public static String concatFieldNamesArray(final String[] fieldNamesArray) {
        if (fieldNamesArray == null || fieldNamesArray.length == 0) {
            return "";
        }
        final StringBuilder builder = new StringBuilder();
        for (final String fieldName : fieldNamesArray) {
            builder.append(fieldName).append(',');
        }
        if (builder.length() > 0) {
            builder.setLength(builder.length() - 1);
        }
        return builder.toString();
    }

    public static String quoteFieldName(final String fieldName) {
        return quoteFieldName(fieldName, "\\\"");
    }

    public static String quoteFieldName(final String fieldName, final String replaceQuoteString) {
        if (fieldName == null || fieldName.isEmpty()) {
            return String.format("%c%c", '\"', '\"');
        }
        final ConnectorSchemaParser parser = new ConnectorSchemaParser();
        parser.setDelimChar('.');
        parser.setEscapeChar('\\');
        final List<String> tokens = parser.tokenize(fieldName);
        final StringBuilder builder = new StringBuilder();
        for (String token : tokens) {
            if (token.length() > 1) {
                final char begin = token.charAt(0);
                final char end = token.charAt(token.length() - 1);
                if ((begin == '\'' || begin == '\"') && begin == end) {
                    token = token.substring(1, token.length() - 1);
                }
            }
            builder.append('\"').append(token.replace(String.valueOf('\"'), replaceQuoteString)).append('\"').append('.');
        }
        if (builder.length() > 0) {
            builder.setLength(builder.length() - 1);
        }
        return builder.toString();
    }

    public static String quoteFieldNames(final String fieldNames) {
        if (fieldNames == null || fieldNames.isEmpty()) {
            return "";
        }
        final String[] fieldNamesArray = convertFieldNamesToArray(fieldNames);
        final String[] quotedFieldNamesArray = quoteFieldNamesArray(fieldNamesArray);
        final StringBuilder builder = new StringBuilder();
        for (final String fieldName : quotedFieldNamesArray) {
            builder.append(fieldName).append(',');
        }
        if (builder.length() > 0) {
            builder.setLength(builder.length() - 1);
        }
        return builder.toString();
    }

    public static String[] quoteFieldNamesArray(final String[] fieldNamesArray) {
        if (fieldNamesArray == null) {
            return new String[0];
        }
        final String[] newFieldNamesArray = new String[fieldNamesArray.length];
        for (int i = 0; i < newFieldNamesArray.length; ++i) {
            final String fieldName = fieldNamesArray[i].trim();
            newFieldNamesArray[i] = quoteFieldName(fieldName);
        }
        return newFieldNamesArray;
    }

    public static String quoteFieldValue(final String fieldValue) {
        return quoteFieldValue(fieldValue, "\\'");
    }

    public static String quoteFieldValue(String fieldValue, final String replaceQuoteString) {
        if (fieldValue == null || fieldValue.isEmpty()) {
            return String.format("%c%c", '\'', '\'');
        }
        if (fieldValue.length() > 1) {
            final char begin = fieldValue.charAt(0);
            final char end = fieldValue.charAt(fieldValue.length() - 1);
            if ((begin == '\'' || begin == '\"') && begin == end) {
                fieldValue = fieldValue.substring(1, fieldValue.length() - 1);
            }
        }
        return '\'' + fieldValue.replace(String.valueOf('\''), replaceQuoteString) + '\'';
    }

    public static String unquoteFieldValue(final String fieldValue) {
        return unquoteFieldValue(fieldValue, "\\'");
    }

    public static String unquoteFieldValue(String fieldValue, final String replaceQuoteString) {
        if (fieldValue == null || fieldValue.isEmpty()) {
            return "";
        }
        if (fieldValue.length() > 1) {
            final char begin = fieldValue.charAt(0);
            final char end = fieldValue.charAt(fieldValue.length() - 1);
            if ((begin == '\'' || begin == '\"') && begin == end) {
                fieldValue = fieldValue.substring(1, fieldValue.length() - 1);
            }
        }
        return fieldValue.replace(replaceQuoteString, String.valueOf('\''));
    }

    public static String unquoteFieldName(final String fieldName) {
        if (fieldName == null || fieldName.isEmpty()) {
            return "";
        }
        final ConnectorSchemaParser parser = new ConnectorSchemaParser();
        parser.setDelimChar('.');
        parser.setEscapeChar('\\');
        final List<String> tokens = parser.tokenize(fieldName);
        final StringBuilder builder = new StringBuilder();
        for (String token : tokens) {
            if (token.length() > 1) {
                final char begin = token.charAt(0);
                final char end = token.charAt(token.length() - 1);
                if ((begin == '\'' || begin == '\"') && begin == end) {
                    token = token.substring(1, token.length() - 1);
                }
            }
            token = token.replace("\\\"", String.valueOf('\"'));
            builder.append(token).append('.');
        }
        if (builder.length() > 0) {
            builder.setLength(builder.length() - 1);
        }
        return builder.toString();
    }

    public static String[] unquoteFieldNamesArray(final String[] fieldNamesArray) {
        if (fieldNamesArray == null) {
            return new String[0];
        }
        final String[] newFieldNamesArray = new String[fieldNamesArray.length];
        for (int i = 0; i < newFieldNamesArray.length; ++i) {
            final String fieldName = fieldNamesArray[i].trim();
            newFieldNamesArray[i] = unquoteFieldName(fieldName);
        }
        return newFieldNamesArray;
    }

    public static ConnectorRecordSchema recordSchemaFromString(final String recordSchema) throws ConnectorException {
        if (recordSchema == null || recordSchema.isEmpty()) {
            return null;
        }
        ByteArrayInputStream input;
        try {
            input = new ByteArrayInputStream(Base64.decodeBase64(recordSchema));
        }
        catch (Exception e1) {
            e1.printStackTrace();
            throw new ConnectorException(e1.getMessage(), e1);
        }
        final DataInputStream in = new DataInputStream(input);
        final ConnectorRecordSchema result = new ConnectorRecordSchema();
        try {
            result.readFields(in);
        }
        catch (IOException e2) {
            throw new ConnectorException(e2.getMessage(), e2);
        }
        return result;
    }

    public static String recordSchemaToString(final ConnectorRecordSchema recordSchema) throws ConnectorException {
        if (recordSchema == null || recordSchema.getLength() == 0) {
            return "";
        }
        final ByteArrayOutputStream output = new ByteArrayOutputStream();
        final DataOutputStream out = new DataOutputStream(output);
        try {
            recordSchema.write(out);
            return Base64.encodeBase64String(output.toByteArray());
        }
        catch (IOException e) {
            throw new ConnectorException(e.getMessage(), e);
        }
    }

    public static String getHivePathString(final Object object) {
        if (object instanceof Timestamp) {
            return new TimestampWritable((Timestamp)object).toString();
        }
        if (object instanceof Double) {
            return new DoubleWritable((double)object).toString();
        }
        if (object instanceof Float) {
            return new FloatWritable((float)object).toString();
        }
        if (object instanceof Integer) {
            return new IntWritable((int)object).toString();
        }
        if (object instanceof Boolean) {
            return new BooleanWritable((boolean)object).toString();
        }
        if (object instanceof Short) {
            return new ShortWritable((short)object).toString();
        }
        if (object instanceof Byte) {
            return new ByteWritable((byte)object).toString();
        }
        if (object instanceof Long) {
            return new LongWritable((long)object).toString();
        }
        return object.toString();
    }

    public static int[] getHivePartitionMapping(final String[] schemaFieldNames, final String sourcePartitionSchema) throws ConnectorException {
        final List<String> fields = parseColumns(sourcePartitionSchema);
        final List<String> fieldNames = parseColumnNames(fields);
        final Map<String, Integer> columnMap = new HashMap<String, Integer>();
        for (int i = 0; i < schemaFieldNames.length; ++i) {
            columnMap.put(schemaFieldNames[i].toLowerCase(), i);
        }
        final int[] mapping = new int[fieldNames.size()];
        for (int j = 0; j < fieldNames.size(); ++j) {
            final Integer position = columnMap.get(fieldNames.get(j).trim().toLowerCase());
            if (position == null) {
                throw new ConnectorException(14005);
            }
            mapping[j] = position;
        }
        return mapping;
    }

    public static ConnectorDataTypeConverter lookupUDF(final ConnectorRecordSchema sourceRecordSchema, final int i) throws ConnectorException {
        try {
            final Class<?> baseClass = ConnectorDataTypeConverter.class;
            String converterClassString = sourceRecordSchema.getDataTypeConverter(i);
            if (!converterClassString.contains(".")) {
                converterClassString = baseClass.getName() + "$" + sourceRecordSchema.getDataTypeConverter(i);
            }
            Class<?> converterClass;
            try {
                converterClass = Class.forName(converterClassString);
            }
            catch (ClassNotFoundException e8) {
                converterClass = Class.forName(sourceRecordSchema.getDataTypeConverter(i));
            }
            final Constructor<?>[] constructors2;
            final Constructor<?>[] constructors = constructors2 = converterClass.getConstructors();
            for (final Constructor<?> constructor : constructors2) {
                final Class<?>[] parameterTypes = constructor.getParameterTypes();
                final int parameterLength = sourceRecordSchema.getParameters(i).length;
                if (parameterTypes.length == parameterLength) {
                    int j;
                    for (j = 0; j < parameterLength && parameterTypes[j].isAssignableFrom(String.class); ++j) {}
                    if (j == parameterLength && baseClass.isAssignableFrom(converterClass)) {
                        return (ConnectorDataTypeConverter)constructor.newInstance((Object[])sourceRecordSchema.getParameters(i));
                    }
                }
            }
            return null;
        }
        catch (SecurityException e) {
            throw new ConnectorException(e.getMessage(), e);
        }
        catch (ConnectorException e2) {
            throw new ConnectorException(e2.getMessage(), e2);
        }
        catch (ClassNotFoundException e3) {
            throw new ConnectorException(e3.getMessage(), e3);
        }
        catch (IllegalArgumentException e4) {
            throw new ConnectorException(e4.getMessage(), e4);
        }
        catch (InstantiationException e5) {
            throw new ConnectorException(e5.getMessage(), e5);
        }
        catch (IllegalAccessException e6) {
            throw new ConnectorException(e6.getMessage(), e6);
        }
        catch (InvocationTargetException e7) {
            throw new ConnectorException(e7.getMessage(), e7);
        }
    }

    public static int convertNullDataTypeByName(String typeName) throws ConnectorException {
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
        if (typeName.equals("STRING")) {
            return 12;
        }
        if (Pattern.matches("\\s*VARCHAR\\s*\\(\\s*(\\d+)\\s*\\)", typeName)) {
            return 12;
        }
        if (typeName.equals("DATE")) {
            return 91;
        }
        if (Pattern.matches("\\s*CHAR\\s*\\(\\s*(\\d+)\\s*\\)", typeName)) {
            return 1;
        }
        if (typeName.equals("DOUBLE") || typeName.equals("DOUBLE PRECISION")) {
            return 8;
        }
        if (typeName.equals("BOOLEAN")) {
            return 16;
        }
        if (typeName.equals("TIMESTAMP")) {
            return 93;
        }
        if (typeName.equals("MAP") || Pattern.matches("\\s*(map\\s*[<].*[>])", typeName.toLowerCase())) {
            return -1000;
        }
        if (typeName.equals("ARRAY") || Pattern.matches("\\s*(array\\s*[<].*[>])", typeName.toLowerCase())) {
            return -1001;
        }
        if (typeName.equals("STRUCT") || Pattern.matches("\\s*(struct\\s*[<].*[>])", typeName.toLowerCase())) {
            return -1002;
        }
        if (typeName.equals("TINYINT")) {
            return -6;
        }
        if (typeName.equals("FLOAT")) {
            return 6;
        }
        if (typeName.equals("BINARY")) {
            return -2;
        }
        if (typeName.equals("DECIMAL") || Pattern.matches("\\s*DECIMAL\\s*\\(\\s*(\\d+)\\s*(,\\s*(\\d+)\\s*)?\\)", typeName)) {
            return 3;
        }
        throw new ConnectorException(14006, new Object[] { typeName });
    }

    public static int lookupDataTypeAndValidate(String typeName) throws ConnectorException {
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
        if (Pattern.matches("\\s*time\\s*with\\s*time\\s*zone\\s*", typeName.toLowerCase())) {
            return 1885;
        }
        if (Pattern.matches("\\s*timestamp\\s*with\\s*time\\s*zone\\s*", typeName.toLowerCase())) {
            return 1886;
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
        if (typeName.equals("MAP") || Pattern.matches("\\s*(map\\s*[<].*[>])", typeName.toLowerCase())) {
            return 12;
        }
        if (typeName.equals("ARRAY") || Pattern.matches("\\s*(array\\s*[<].*[>])", typeName.toLowerCase())) {
            return 12;
        }
        if (typeName.equals("STRUCT") || typeName.equals("RECORD") || Pattern.matches("\\s*(struct\\s*[<].*[>])", typeName.toLowerCase())) {
            return 12;
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
        if (typeName.equals("ENUM")) {
            return 12;
        }
        if (typeName.equals("NULL")) {
            return 12;
        }
        if (typeName.equals("UNION")) {
            return 1882;
        }
        if (typeName.equals("FIXED")) {
            return 12;
        }
        if (typeName.equals("NUMERIC")) {
            return 3;
        }
        if (typeName.equals("OTHER")) {
            return 1882;
        }
        return 1883;
    }

    public static String[] getUdfParameters(final String udf) {
        final int start = udf.indexOf("(");
        final int end = udf.indexOf(")");
        if (start == -1 || end == -1) {
            return new String[] { "" };
        }
        if (end - start == 1) {
            return new String[0];
        }
        final String parameters = udf.substring(start + 1, end);
        return parameters.split(",");
    }

    public static String[] fieldNamesFromJson(final String fieldNamesJson) {
        if (fieldNamesJson == null || fieldNamesJson.trim().length() == 0) {
            return new String[0];
        }
        String[] fieldNames = new String[0];
        try {
            final ObjectMapper objectMapper = new ObjectMapper();
            final Map<String, Object> tableMap = (Map<String, Object>)objectMapper.readValue(fieldNamesJson, (Class)Map.class);
            final String database = (String) tableMap.get("database");
            final String table = (String) tableMap.get("table");
            final ArrayList<Map<String, Object>> columnList = (ArrayList<Map<String, Object>>) tableMap.get("columns");
            fieldNames = new String[columnList.size()];
            for (int i = 0; i < columnList.size(); ++i) {
                final Map<String, Object> columnMap = columnList.get(i);
                fieldNames[i] = (String) columnMap.get("name");
            }
        }
        catch (Exception ex) {}
        return fieldNames;
    }

    public static String fieldNamesToJson(final String[] fieldNames) {
        final ObjectMapper objectMapper = new ObjectMapper();
        final ObjectNode table = objectMapper.createObjectNode();
        table.put("database", "");
        table.put("table", "");
        final ArrayNode columns = table.putArray("columns");
        for (int i = 0; i < fieldNames.length; ++i) {
            final ObjectNode column = columns.addObject();
            column.put("name", fieldNames[i]);
        }
        return table.toString();
    }

    public static boolean isConnectorDefinitionDataType(final int type) {
        return type == 4 || type == -5 || type == 5 || type == -6 || type == 6 || type == 7 || type == 8 || type == 16 || type == 2 || type == 3 || type == 2005 || type == 2004 || type == 1 || type == 12 || type == -1 || type == -2 || type == -3 || type == 91 || type == 92 || type == 93 || type == 2003 || type == 2002 || type == 1111 || type == 1882 || type == 1885 || type == 1886 || type == 1883;
    }

    public static ConnectorRecordSchema formalizeConnectorRecordSchema(final ConnectorRecordSchema r) throws ConnectorException {
        for (int i = 0; i < r.getLength(); ++i) {
            if (!isConnectorDefinitionDataType(r.getFieldType(i))) {
                r.setFieldType(i, 12);
            }
        }
        return r;
    }

    public static int getWritableObjectType(final Object object) throws ConnectorException {
        if (object instanceof IntWritable) {
            return 4;
        }
        if (object instanceof LongWritable) {
            return -5;
        }
        if (object instanceof ShortWritable) {
            return 5;
        }
        if (object instanceof ByteWritable) {
            return -6;
        }
        if (object instanceof FloatWritable) {
            return 6;
        }
        if (object instanceof DoubleWritable) {
            return 7;
        }
        if (object instanceof BooleanWritable) {
            return 16;
        }
        if (object instanceof ConnectorDataWritable.BigDecimalWritable) {
            return 2;
        }
        if (object instanceof ConnectorDataWritable.ClobWritable) {
            return 2005;
        }
        if (object instanceof ConnectorDataWritable.BlobWritable) {
            return 2004;
        }
        if (object instanceof Text) {
            return 12;
        }
        if (object instanceof BytesWritable) {
            return -2;
        }
        if (object instanceof ConnectorDataWritable.DateWritable) {
            return 91;
        }
        if (object instanceof ConnectorDataWritable.TimeWritable) {
            return 92;
        }
        if (object instanceof TimestampWritable) {
            return 93;
        }
        if (object instanceof ConnectorDataWritable.PeriodWritable) {
            return 2002;
        }
        if (object instanceof ConnectorDataWritable.CalendarWritable) {
            return 1886;
        }
        throw new ConnectorException(15028, new Object[] { object.getClass() });
    }

    public static int getGenericObjectType(final Object object) {
        if (object == null) {
            return 1884;
        }
        if (object instanceof Integer) {
            return 4;
        }
        if (object instanceof Long) {
            return -5;
        }
        if (object instanceof Short) {
            return 5;
        }
        if (object instanceof Byte) {
            return -6;
        }
        if (object instanceof Float) {
            return 6;
        }
        if (object instanceof Double) {
            return 7;
        }
        if (object instanceof Boolean) {
            return 16;
        }
        if (object instanceof BigDecimal) {
            return 2;
        }
        if (object instanceof Clob) {
            return 2005;
        }
        if (object instanceof Blob) {
            return 2004;
        }
        if (object instanceof String) {
            return 12;
        }
        if (object instanceof byte[]) {
            return -2;
        }
        if (object instanceof Date) {
            return 91;
        }
        if (object instanceof Time) {
            return 92;
        }
        if (object instanceof Timestamp) {
            return 93;
        }
        if (object instanceof Calendar) {
            return 1886;
        }
        if (object instanceof ResultStruct) {
            final ResultStruct rs = (ResultStruct)object;
            try {
                if (rs.getSQLTypeName().startsWith("PERIOD")) {
                    return 2002;
                }
            }
            catch (SQLException e) {
                throw new RuntimeException(e.getMessage(), e);
            }
        }
        return 12;
    }

    public static Map<Long, ConnectorDataTypeConverter> constructConvertMap(final ConnectorConverter connectorConverter, final Configuration configuration, final int targetScale, final int targetPrecision, final int targetLength) throws ConnectorException {
        final int[] connectorType = { 4, -5, 5, -6, 6, 7, 8, 16, 2, 3, 2005, 2004, 1, 12, -1, -2, -3, 91, 92, 93, 2003, 2002, 1111, 1885, 1886 };
        final Map<Integer, Object> defaultVal = connectorConverter.initializeDefaultValue();
        final Map<Integer, Object> falseDefVal = connectorConverter.initializeFalseDefaultValue();
        final Map<Integer, Object> trueDefVal = connectorConverter.initializeTrueDefaultValue();
        final Map<Long, ConnectorDataTypeConverter> map = new HashMap<Long, ConnectorDataTypeConverter>();
        for (int i = 0; i < connectorType.length; ++i) {
            final int sourceType = connectorType[i];
            for (int j = 0; j < connectorType.length; ++j) {
                final int targetType = connectorType[j];
                final long mapKey = genConnectorMapKey(sourceType, targetType);
                if (map.get(mapKey) != null) {
                    throw new ConnectorException(15027);
                }
                try {
                    final ConnectorDataTypeConverter c = connectorConverter.lookupDataTypeConverter(sourceType, targetType, targetScale, targetPrecision, targetLength, defaultVal, falseDefVal, trueDefVal, configuration);
                    map.put(mapKey, c);
                }
                catch (ConnectorException e) {}
            }
        }
        return map;
    }

    public static long genConnectorMapKey(int sourceType, int targetType) {
        if (sourceType < 0) {
            sourceType = -1 * sourceType * 100000;
        }
        if (targetType < 0) {
            targetType = -1 * targetType * 10000;
        }
        return sourceType * 1000000000 + targetType;
    }

    public static boolean isTimeWithTimeZoneType(final String typeName) {
        return Pattern.matches("\\s*time\\s*with\\s*time\\s*zone\\s*", typeName.toLowerCase());
    }

    public static boolean isTimestampWithTimeZoneType(final String typeName) {
        return Pattern.matches("\\s*timestamp\\s*with\\s*time\\s*zone\\s*", typeName.toLowerCase());
    }

    public static boolean isDateType(final String typeName) {
        return Pattern.matches("\\s*date", typeName.toLowerCase());
    }

    public static boolean isTimeType(final String typeName) {
        return Pattern.matches("\\s*time", typeName.toLowerCase());
    }
}
