package com.teradata.connector.hive.utils;

import com.teradata.connector.common.ConnectorPlugin;
import com.teradata.connector.common.ConnectorRecordSchema;
import com.teradata.connector.common.converter.ConnectorDataTypeConverter;
import com.teradata.connector.common.converter.ConnectorDataTypeConverter.ObjectToJsonString;
import com.teradata.connector.common.converter.ConnectorDataTypeDefinition;
import com.teradata.connector.common.exception.ConnectorException;
import com.teradata.connector.common.exception.ConnectorException.ErrorCode;
import com.teradata.connector.common.utils.ConnectorConfiguration;
import com.teradata.connector.common.utils.ConnectorConfiguration.direction;
import com.teradata.connector.common.utils.ConnectorSchemaUtils;
import com.teradata.connector.hive.converter.HiveDataType;
import com.teradata.connector.hive.converter.HiveDataType.HiveDataTypeLazyImpl;
import com.teradata.connector.hive.converter.HiveDataType.HiveDataTypeWritableImpl;
import com.teradata.connector.hive.converter.HiveDataTypeConverter;
import com.teradata.connector.hive.converter.HiveDataTypeConverter.BigDecimalToHiveDecimalWithConstructor;
import com.teradata.connector.hive.converter.HiveDataTypeConverter.BigDecimalToHiveDecimalWithCreateMethod;
import com.teradata.connector.hive.converter.HiveDataTypeConverter.JsonStringToArray;
import com.teradata.connector.hive.converter.HiveDataTypeConverter.JsonStringToMap;
import com.teradata.connector.hive.converter.HiveDataTypeConverter.JsonStringToStruct;
import com.teradata.connector.hive.converter.HiveDataTypeConverter.StringToHiveChar;
import com.teradata.connector.hive.converter.HiveDataTypeConverter.StringToHiveVarchar;
import com.teradata.connector.hive.converter.HiveDataTypeDefinition;
import com.teradata.connector.teradata.schema.TeradataColumnDesc;
import com.teradata.connector.teradata.schema.TeradataTableDesc;

import java.lang.reflect.Constructor;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.commons.logging.Log;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.common.type.HiveDecimal;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.metastore.HiveMetaStoreClient;
import org.apache.hadoop.hive.metastore.api.FieldSchema;
import org.apache.hadoop.hive.metastore.api.Partition;
import org.apache.hadoop.hive.metastore.api.UnknownDBException;
import org.apache.hadoop.hive.ql.metadata.Hive;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.metadata.Table;
import org.apache.hadoop.hive.ql.metadata.formatting.MetaDataFormatUtils;
import org.apache.hadoop.hive.serde2.SerDeException;
import org.apache.hadoop.hive.serde2.columnar.ColumnarSerDe;
import org.apache.hadoop.hive.serde2.columnar.ColumnarSerDeBase;
import org.apache.hadoop.hive.serde2.columnar.LazyBinaryColumnarSerDe;
import org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe;
import org.apache.hadoop.hive.serde2.objectinspector.*;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector.Category;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.typeinfo.ListTypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.MapTypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.PrimitiveTypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.StructTypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfoUtils;
import org.apache.thrift.TException;
import org.codehaus.jackson.map.ObjectMapper;
import org.codehaus.jackson.node.ArrayNode;
import org.codehaus.jackson.node.ObjectNode;


public class HiveSchemaUtils {
    protected static final String PATTERN_DECIMAL_TYPE = "\\s*DECIMAL\\s*\\(\\s*(\\d+)\\s*(,\\s*(\\d+)\\s*)?\\)";
    protected static final String PATTERN_VARCHAR_TYPE = "\\s*VARCHAR\\s*\\(\\s*(\\d+)\\s*\\)";
    protected static final String PATTERN_CHAR_TYPE = "\\s*CHAR\\s*\\(\\s*(\\d+)\\s*\\)";
    protected static final Pattern decimalPattern = Pattern.compile("\\s*DECIMAL\\s*\\(\\s*(\\d+)\\s*(,\\s*(\\d+)\\s*)?\\)");
    protected static final String PATTERN_MAP_TYPE = "\\s*(map\\s*[<].*[>])";
    protected static final String PATTERN_ARRAY_TYPE = "\\s*(array\\s*[<].*[>])";
    protected static final String PATTERN_STRUCT_TYPE = "\\s*(struct\\s*[<].*[>])";
    protected static final String PATTERN_UNION_TYPE = "\\s*(union\\s*[<].*[>])";
    public static final String LIST_COLUMNS = "columns";
    public static final String LIST_COLUMN_TYPES = "columns.types";

    public static HiveDataType lookupHiveDataTypeWritableImpl(int type) {
        switch (type) {
            case -2001:
                return HiveDataType.HiveDataTypeWritableImpl.VARCHAR;
            case HiveDataTypeDefinition.TYPE_STRUCT /*-1002*/:
                return HiveDataType.HiveDataTypeWritableImpl.STRUCT;
            case HiveDataTypeDefinition.TYPE_ARRAY /*-1001*/:
                return HiveDataType.HiveDataTypeWritableImpl.ARRAY;
            case HiveDataTypeDefinition.TYPE_MAP /*-1000*/:
                return HiveDataType.HiveDataTypeWritableImpl.MAP;
            case ConnectorDataTypeDefinition.TYPE_TINYINT /*-6*/:
                return HiveDataType.HiveDataTypeWritableImpl.TINYINT;
            case ConnectorDataTypeDefinition.TYPE_BIGINT /*-5*/:
                return HiveDataType.HiveDataTypeWritableImpl.BIGINT;
            case ConnectorDataTypeDefinition.TYPE_BINARY /*-2*/:
                return HiveDataType.HiveDataTypeWritableImpl.BINARY;
            case 1:
                return HiveDataType.HiveDataTypeWritableImpl.CHAR;
            case ConnectorDataTypeDefinition.TYPE_DECIMAL /*3*/:
                return HiveDataType.HiveDataTypeWritableImpl.DECIMAL;
            case 4:
                return HiveDataType.HiveDataTypeWritableImpl.INTEGER;
            case 5:
                return HiveDataType.HiveDataTypeWritableImpl.SMALLINT;
            case 6:
                return HiveDataType.HiveDataTypeWritableImpl.FLOAT;
            case ConnectorDataTypeDefinition.TYPE_DOUBLE /*8*/:
                return HiveDataType.HiveDataTypeWritableImpl.DOUBLE;
            case 12:
                return HiveDataType.HiveDataTypeWritableImpl.STRING;
            case ConnectorDataTypeDefinition.TYPE_BOOLEAN /*16*/:
                return HiveDataType.HiveDataTypeWritableImpl.BOOLEAN;
            case ConnectorDataTypeDefinition.TYPE_DATE /*91*/:
                return HiveDataType.HiveDataTypeWritableImpl.DATE;
            case ConnectorDataTypeDefinition.TYPE_TIMESTAMP /*93*/:
                return HiveDataType.HiveDataTypeWritableImpl.TIMESTAMP;
            default:
                return null;
        }
    }


    public static HiveDataType lookupHiveDataTypeLazyImpl(final int type) {
        switch (type) {
            case -2001:
                return HiveDataType.HiveDataTypeLazyImpl.VARCHAR;
            case HiveDataTypeDefinition.TYPE_STRUCT /*-1002*/:
                return HiveDataTypeLazyImpl.STRUCT;
            case HiveDataTypeDefinition.TYPE_ARRAY /*-1001*/:
                return HiveDataTypeLazyImpl.ARRAY;
            case HiveDataTypeDefinition.TYPE_MAP /*-1000*/:
                return HiveDataTypeLazyImpl.MAP;
            case ConnectorDataTypeDefinition.TYPE_TINYINT /*-6*/:
                return HiveDataTypeLazyImpl.TINYINT;
            case ConnectorDataTypeDefinition.TYPE_BIGINT /*-5*/:
                return HiveDataTypeLazyImpl.BIGINT;
            case ConnectorDataTypeDefinition.TYPE_BINARY /*-2*/:
                return HiveDataTypeLazyImpl.BINARY;
            case 1:
                return HiveDataTypeLazyImpl.CHAR;
            case ConnectorDataTypeDefinition.TYPE_DECIMAL /*3*/:
                return HiveDataTypeLazyImpl.DECIMAL;
            case 4:
                return HiveDataTypeLazyImpl.INTEGER;
            case 5:
                return HiveDataTypeLazyImpl.SMALLINT;
            case 6:
                return HiveDataTypeLazyImpl.FLOAT;
            case ConnectorDataTypeDefinition.TYPE_DOUBLE /*8*/:
                return HiveDataTypeLazyImpl.DOUBLE;
            case 12:
                return HiveDataTypeLazyImpl.STRING;
            case ConnectorDataTypeDefinition.TYPE_BOOLEAN /*16*/:
                return HiveDataTypeLazyImpl.BOOLEAN;
            case ConnectorDataTypeDefinition.TYPE_DATE /*91*/:
                return HiveDataTypeLazyImpl.DATE;
            case ConnectorDataTypeDefinition.TYPE_TIMESTAMP /*93*/:
                return HiveDataTypeLazyImpl.TIMESTAMP;
            default:
                return null;
        }

    }

    public static StructObjectInspector createStructObjectInspector(final String targetTableSchema) throws ConnectorException {
        final List<ObjectInspector> fieldInspectors = new ArrayList<ObjectInspector>();
        final List<String> fieldNames = new ArrayList<String>();
        TeradataTableDesc tableDesc;
        try {
            tableDesc = columnSchemaToTableDesc(targetTableSchema);
        } catch (ConnectorException e) {
            throw e;
        }
        final TeradataColumnDesc[] columnDescs = tableDesc.getColumns();
        for (int i = 0; i < columnDescs.length; ++i) {
            final TypeInfo type = TypeInfoUtils.getTypeInfoFromTypeString(columnDescs[i].getTypeName().toLowerCase());
            fieldNames.add(columnDescs[i].getName());
            if (columnDescs[i].getType() == 3) {
                try {
                    final Class<?> oi = Class.forName("org.apache.hadoop.hive.serde2.objectinspector.primitive.JavaHiveDecimalObjectInspector");
                    final Class<?> ti = Class.forName("org.apache.hadoop.hive.serde2.typeinfo.DecimalTypeInfo");
                    final Constructor<?> cnst1 = oi.getConstructor(ti);
                    final Constructor<?> cnst2 = ti.getConstructor(Integer.TYPE, Integer.TYPE);
                    fieldInspectors.add((ObjectInspector) cnst1.newInstance(cnst2.newInstance(columnDescs[i].getPrecision(), columnDescs[i].getScale())));
                    continue;
                } catch (ClassNotFoundException e3) {
                    fieldInspectors.add(getObjectInspector(type));
                    continue;
                } catch (NoSuchMethodException e4) {
                    fieldInspectors.add(getObjectInspector(type));
                    continue;
                } catch (Exception e2) {
                    throw new ConnectorException(e2.getMessage(), e2);
                }
            }
            fieldInspectors.add(getObjectInspector(type));
        }
        final StructObjectInspector structInspector = (StructObjectInspector) ObjectInspectorFactory.getStandardStructObjectInspector((List) fieldNames, (List) fieldInspectors);
        return structInspector;
    }

    public static ObjectInspector getObjectInspector(final TypeInfo type) throws ConnectorException {
        switch (type.getCategory()) {
            case PRIMITIVE: {
                final PrimitiveTypeInfo primitiveType = (PrimitiveTypeInfo) type;
                return (ObjectInspector) PrimitiveObjectInspectorFactory.getPrimitiveJavaObjectInspector(primitiveType.getPrimitiveCategory());
            }
            case MAP: {
                final MapTypeInfo mapType = (MapTypeInfo) type;
                final MapObjectInspector mapInspector = (MapObjectInspector) ObjectInspectorFactory.getStandardMapObjectInspector(getObjectInspector(mapType.getMapKeyTypeInfo()), getObjectInspector(mapType.getMapValueTypeInfo()));
                return (ObjectInspector) mapInspector;
            }
            case LIST: {
                final ListTypeInfo listType = (ListTypeInfo) type;
                final ListObjectInspector listInspector = (ListObjectInspector) ObjectInspectorFactory.getStandardListObjectInspector(getObjectInspector(listType.getListElementTypeInfo()));
                return (ObjectInspector) listInspector;
            }
            case STRUCT: {
                final StructTypeInfo structType = (StructTypeInfo) type;
                final List<TypeInfo> fieldTypes = (List<TypeInfo>) structType.getAllStructFieldTypeInfos();
                final List<ObjectInspector> fieldInspectors = new ArrayList<ObjectInspector>();
                for (final TypeInfo fieldType : fieldTypes) {
                    fieldInspectors.add(getObjectInspector(fieldType));
                }
                final StructObjectInspector structInspector = (StructObjectInspector) ObjectInspectorFactory.getStandardStructObjectInspector((List) structType.getAllStructFieldNames(), (List) fieldInspectors);
                return (ObjectInspector) structInspector;
            }
            default: {
                throw new ConnectorException(14006, new Object[]{type.getCategory()});
            }
        }
    }

    public static ColumnarSerDeBase initializeColumnarSerDe(final Configuration configuration, final String schema, final ConnectorConfiguration.direction direction) throws ConnectorException {
        final List<String> columns = ConnectorSchemaUtils.parseColumns(schema.toLowerCase());
        final List<String> columnNames = ConnectorSchemaUtils.parseColumnNames(columns);
        final List<String> columnTypes = ConnectorSchemaUtils.parseColumnTypes(columns);
        updateNonScaleDecimalColumns(columnTypes);
        final Properties props = new Properties();
        props.setProperty("columns", HiveUtils.listToString(columnNames));
        props.setProperty("columns.types", HiveUtils.listToString(columnTypes));
        if (direction.equals(ConnectorConfiguration.direction.input)) {
            props.setProperty("line.delim", HivePlugInConfiguration.getInputLineSeparator(configuration));
        } else {
            props.setProperty("line.delim", HivePlugInConfiguration.getOutputLineSeparator(configuration));
        }
        String value = configuration.get("serialization.null.format");
        if (value == null || value.isEmpty()) {
            if (direction.equals(ConnectorConfiguration.direction.input)) {
                value = HivePlugInConfiguration.getInputNullString(configuration);
            } else {
                value = HivePlugInConfiguration.getOutputNullString(configuration);
            }
        }
        if (value != null && !value.isEmpty()) {
            props.setProperty("serialization.null.format", value);
        }
        value = configuration.get("colelction.delim");
        if (value != null && !value.isEmpty()) {
            props.setProperty("colelction.delim", value);
        }
        value = configuration.get("mapkey.delim");
        if (value != null && !value.isEmpty()) {
            props.setProperty("mapkey.delim", value);
        }
        value = configuration.get("serialization.last.column.takes.rest");
        if (value != null && !value.isEmpty()) {
            props.setProperty("serialization.last.column.takes.rest", value);
        }
        value = configuration.get("escape.delim");
        if (value != null && !value.isEmpty()) {
            props.setProperty("escape.delim", value);
        }
        try {
            ColumnarSerDeBase serde = null;
            String defSerde;
            if (direction.equals(ConnectorConfiguration.direction.input)) {
                defSerde = HivePlugInConfiguration.getInputRCFileSerde(configuration);
            } else {
                defSerde = HivePlugInConfiguration.getOutputRCFileSerde(configuration);
            }
            if (defSerde.isEmpty()) {
                serde = (ColumnarSerDeBase) new LazyBinaryColumnarSerDe();
            } else if (defSerde.equals("org.apache.hadoop.hive.serde2.columnar.LazyBinaryColumnarSerDe")) {
                serde = (ColumnarSerDeBase) new LazyBinaryColumnarSerDe();
            } else {
                if (!defSerde.equals("org.apache.hadoop.hive.serde2.columnar.ColumnarSerDe")) {
                    throw new ConnectorException(32005);
                }
                serde = (ColumnarSerDeBase) new ColumnarSerDe();
            }
            serde.initialize(configuration, props);
            return serde;
        } catch (SerDeException e) {
            throw new ConnectorException(e.getMessage(), (Throwable) e);
        }
    }

    public static LazySimpleSerDe initializeLazySimpleSerde(final Configuration configuration, final String schema, final ConnectorConfiguration.direction direction) throws ConnectorException {
        final List<String> columns = ConnectorSchemaUtils.parseColumns(schema.toLowerCase());
        final List<String> columnNames = ConnectorSchemaUtils.parseColumnNames(columns);
        final List<String> columnTypes = ConnectorSchemaUtils.parseColumnTypes(columns);
        updateNonScaleDecimalColumns(columnTypes);
        final Properties props = new Properties();
        props.setProperty("columns", HiveUtils.listToString(columnNames));
        props.setProperty("columns.types", HiveUtils.listToString(columnTypes));
        if (direction.equals(ConnectorConfiguration.direction.input)) {
            props.setProperty("field.delim", HivePlugInConfiguration.getInputSeparator(configuration));
            props.setProperty("line.delim", HivePlugInConfiguration.getInputLineSeparator(configuration));
        } else {
            props.setProperty("field.delim", HivePlugInConfiguration.getOutputSeparator(configuration));
            props.setProperty("line.delim", HivePlugInConfiguration.getOutputLineSeparator(configuration));
        }
        String value = configuration.get("serialization.null.format");
        if (value == null || value.isEmpty()) {
            if (direction.equals(ConnectorConfiguration.direction.input)) {
                value = HivePlugInConfiguration.getInputNullString(configuration);
            } else {
                value = HivePlugInConfiguration.getOutputNullString(configuration);
            }
        }
        if (value != null && !value.isEmpty()) {
            props.setProperty("serialization.null.format", value);
        }
        value = configuration.get("colelction.delim");
        if (value != null && !value.isEmpty()) {
            props.setProperty("colelction.delim", value);
        }
        value = configuration.get("mapkey.delim");
        if (value != null && !value.isEmpty()) {
            props.setProperty("mapkey.delim", value);
        }
        value = configuration.get("serialization.last.column.takes.rest");
        if (value != null && !value.isEmpty()) {
            props.setProperty("serialization.last.column.takes.rest", value);
        }
        value = configuration.get("escape.delim");
        if (value != null && !value.isEmpty()) {
            props.setProperty("escape.delim", value);
        }
        try {
            final LazySimpleSerDe serde = new LazySimpleSerDe();
            serde.initialize(configuration, props);
            return serde;
        } catch (SerDeException e) {
            throw new ConnectorException(e.getMessage(), (Throwable) e);
        }
    }

    public static TeradataTableDesc columnSchemaToTableDesc(final String tableColumnSchema) throws ConnectorException {
        final TeradataTableDesc tableDesc = new TeradataTableDesc();
        final List<String> columns = ConnectorSchemaUtils.parseColumns(tableColumnSchema);
        final List<String> columnNames = ConnectorSchemaUtils.parseColumnNames(columns);
        final List<String> columnTypes = ConnectorSchemaUtils.parseColumnTypes(columns);
        final int[] precisions = lookupHiveDataTypePrecisions(columnTypes.toArray(new String[columnTypes.size()]));
        final int[] scales = lookupHiveDataTypeScales(columnTypes.toArray(new String[columnTypes.size()]));
        for (int i = 0; i < columnNames.size(); ++i) {
            final TeradataColumnDesc columnDesc = new TeradataColumnDesc();
            final String columnName = ConnectorSchemaUtils.unquoteFieldName(columnNames.get(i).trim());
            columnDesc.setName(columnName);
            columnDesc.setType(lookupHiveDataTypeByName(columnTypes.get(i).trim()));
            columnDesc.setTypeName(columnTypes.get(i).trim());
            columnDesc.setNullable(true);
            columnDesc.setPrecision(precisions[i]);
            columnDesc.setScale(scales[i]);
            tableDesc.addColumn(columnDesc);
        }
        return tableDesc;
    }

    public static int[] lookupHiveDataTypeByName(final String[] typeName) throws ConnectorException {
        final int[] types = new int[typeName.length];
        for (int i = 0; i < types.length; ++i) {
            types[i] = lookupHiveDataTypeByName(typeName[i]);
        }
        return types;
    }

    public static int lookupHiveDataTypeByName(String typeName) throws ConnectorException {
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
        if (typeName.equals("STRING") || Pattern.matches(PATTERN_VARCHAR_TYPE, typeName)) {
            return 12;
        }
        if (typeName.equals("DATE")) {
            return 91;
        }
        if (Pattern.matches(PATTERN_CHAR_TYPE, typeName)) {
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
            return HiveDataTypeDefinition.TYPE_MAP;
        }
        if (typeName.equals("ARRAY") || Pattern.matches("\\s*(array\\s*[<].*[>])", typeName.toLowerCase())) {
            return HiveDataTypeDefinition.TYPE_ARRAY;
        }
        if (typeName.equals("STRUCT") || Pattern.matches("\\s*(struct\\s*[<].*[>])", typeName.toLowerCase())) {
            return HiveDataTypeDefinition.TYPE_STRUCT;
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
        if (typeName.equals("DECIMAL") || Pattern.matches(PATTERN_DECIMAL_TYPE, typeName)) {
            return 3;
        }
        throw new ConnectorException((int) ErrorCode.FIELD_DATATYPE_UNSUPPORTED, typeName);
    }

    public static String typeNamesToJson(final String[] typeNames) {
        final ObjectMapper objectMapper = new ObjectMapper();
        final ObjectNode objectNode = objectMapper.createObjectNode();
        final ArrayNode types = objectNode.putArray("types");
        for (int i = 0; i < typeNames.length; ++i) {
            final ObjectNode type = types.addObject();
            type.put("type", typeNames[i]);
        }
        return objectNode.toString();
    }

    public static String[] typeNamesFromJson(final String typeJson) {
        if (typeJson == null || typeJson.length() == 0) {
            return new String[0];
        }
        try {
            ArrayList<Map<String, Object>> types = (ArrayList) ((Map) new ObjectMapper().readValue(typeJson, Map.class)).get("types");
            String[] typeName = new String[types.size()];
            for (int i = 0; i < types.size(); i++) {
                typeName[i] = (String) ((Map) types.get(i)).get("type");
            }
            return typeName;
        } catch (Exception e) {
            return null;
        }
    }

    public static String getFieldNamesFromTableDescText(final String tableDescText) throws ConnectorException {
        final TeradataTableDesc tableDesc = tableDescFromText(tableDescText);
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

    public static TeradataTableDesc tableDescFromText(final String tableDescText) {
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

    private static Map<String, Integer> arrayToHashMap(final String[] arr) {
        final Map<String, Integer> map = new HashMap<String, Integer>();
        for (int i = 0; i < arr.length; ++i) {
            map.put(arr[i].toLowerCase().trim(), i);
        }
        return map;
    }

    public static int[] getColumnMapping(final String[] ColNames, final String[] mappingNames) {
        final Map<String, Integer> map = arrayToHashMap(ColNames);
        final int[] mapping = new int[mappingNames.length];
        for (int i = 0; i < mappingNames.length; ++i) {
            mapping[i] = map.get(mappingNames[i].toLowerCase().trim());
        }
        return mapping;
    }

    public static HiveDataType[] lookupWritableHiveDataTypes(final String[] typeNames) throws ConnectorException {
        final HiveDataType[] fieldDataTypes = new HiveDataType[typeNames.length];
        for (int i = 0; i < typeNames.length; ++i) {
            final String typeName = typeNames[i].toUpperCase();
            if (Pattern.matches("\\s*VARCHAR\\s*\\(\\s*(\\d+)\\s*\\)", typeName)) {
                fieldDataTypes[i] = lookupHiveDataTypeWritableImpl(-2001);
            } else {
                fieldDataTypes[i] = lookupHiveDataTypeWritableImpl(lookupHiveDataTypeByName(typeNames[i]));
            }
        }
        return fieldDataTypes;
    }

    public static HiveDataType[] lookupLazyHiveDataTypes(final String[] typeNames) throws ConnectorException {
        final HiveDataType[] fieldDataTypes = new HiveDataType[typeNames.length];
        for (int i = 0; i < typeNames.length; ++i) {
            final String typeName = typeNames[i].toUpperCase();
            if (Pattern.matches("\\s*VARCHAR\\s*\\(\\s*(\\d+)\\s*\\)", typeName)) {
                fieldDataTypes[i] = lookupHiveDataTypeLazyImpl(-2001);
            } else {
                fieldDataTypes[i] = lookupHiveDataTypeLazyImpl(lookupHiveDataTypeByName(typeNames[i]));
            }
        }
        return fieldDataTypes;
    }

    public static int[] lookupHiveDataTypePrecisions(final String[] typeNames) throws ConnectorException {
        final int[] precisions = new int[typeNames.length];
        for (int i = 0; i < typeNames.length; ++i) {
            final HiveDataType dt = lookupHiveDataTypeLazyImpl(lookupHiveDataTypeByName(typeNames[i]));
            if (dt == HiveDataType.HiveDataTypeLazyImpl.DECIMAL) {
                final Matcher m;
                if ((m = HiveSchemaUtils.decimalPattern.matcher(typeNames[i].toUpperCase())).find()) {
                    precisions[i] = Integer.parseInt(m.group(1));
                } else {
                    precisions[i] = 10;
                }
            } else {
                precisions[i] = 5;
            }
        }
        return precisions;
    }

    public static int[] lookupHiveDataTypeScales(final String[] typeNames) throws ConnectorException {
        final int[] scales = new int[typeNames.length];
        for (int i = 0; i < typeNames.length; ++i) {
            final HiveDataType dt = lookupHiveDataTypeLazyImpl(lookupHiveDataTypeByName(typeNames[i]));
            if (dt == HiveDataType.HiveDataTypeLazyImpl.DECIMAL) {
                final Matcher m;
                if ((m = HiveSchemaUtils.decimalPattern.matcher(typeNames[i].toUpperCase())).find() && m.group(3) != null) {
                    scales[i] = Integer.parseInt(m.group(3));
                } else {
                    scales[i] = 0;
                }
            } else {
                scales[i] = 6;
            }
        }
        return scales;
    }

    public static void updateNonScaleDecimalColumns(final List<String> columnTypes) {
        for (int i = 0; i < columnTypes.size(); ++i) {
            if (columnTypes.get(i).toUpperCase().equals("DECIMAL")) {
                columnTypes.set(i, "decimal(10,0)");
            } else {
                final Matcher m;
                if ((m = HiveSchemaUtils.decimalPattern.matcher(columnTypes.get(i).toUpperCase())).find() && m.group(2) == null) {
                    String cType = columnTypes.get(i);
                    cType = cType.replace(")", ",0)");
                    columnTypes.set(i, cType);
                }
            }
        }
    }

    public static String lookupTypeNameByDataType(final int datatype) throws ConnectorException {
        switch (datatype) {
            case HiveDataTypeDefinition.TYPE_STRUCT /*-1002*/:
                return "struct";
            case HiveDataTypeDefinition.TYPE_ARRAY /*-1001*/:
                return "array";
            case HiveDataTypeDefinition.TYPE_MAP /*-1000*/:
                return "map";
            case ConnectorDataTypeDefinition.TYPE_TINYINT /*-6*/:
                return "tinyint";
            case ConnectorDataTypeDefinition.TYPE_BIGINT /*-5*/:
                return "bigint";
            case ConnectorDataTypeDefinition.TYPE_BINARY /*-2*/:
                return "BINARY";
            case 4:
                return "int";
            case 5:
                return "smallint";
            case 6:
                return "float";
            case ConnectorDataTypeDefinition.TYPE_DOUBLE /*8*/:
                return "double";
            case 12:
                return "string";
            case ConnectorDataTypeDefinition.TYPE_BOOLEAN /*16*/:
                return "boolean";
            case ConnectorDataTypeDefinition.TYPE_TIMESTAMP /*93*/:
                return "timestamp";
            default:
                throw new ConnectorException((int) ErrorCode.FIELD_DATATYPE_UNSUPPORTED, Integer.valueOf(datatype));
        }
    }

    public static void logHiveTableExtInfo(final Log logger, final String databaseName, final String tableName, final HiveConf hiveConf) throws ConnectorException, UnknownDBException, TException, HiveException {
        if (!logger.isDebugEnabled()) {
            return;
        }
        final Hive hive = Hive.get(hiveConf);
        final Table tbl = hive.getTable(databaseName, tableName);
        final String tblInfo = MetaDataFormatUtils.getTableInformation(tbl);
        final String colInfo = printHiveTableSchema(tbl.getCols(), tbl.isPartitioned() ? tbl.getPartCols() : null);
        logger.debug((Object) ("\n# Hive Table Info For " + databaseName + "." + tableName + ":\n" + colInfo + "\n" + tblInfo));
    }

    public static void logHivePartitionValues(final Log logger, final String databaseName, final String tableName, final HiveConf hiveConf) throws UnknownDBException, TException {
        if (!logger.isDebugEnabled()) {
            return;
        }
        final HiveMetaStoreClient client = new HiveMetaStoreClient(hiveConf);
        final List<Partition> partitions = (List<Partition>) client.listPartitions(databaseName, tableName, (short) 20000);
        if (partitions.size() > 0) {
            for (final Partition partition : partitions) {
                logger.debug((Object) ("#Partition Location: " + partition.getSd().getLocation()));
            }
        }
    }

    public static void checkSchemaMatch(final ConnectorRecordSchema r1, final ConnectorRecordSchema r2) throws ConnectorException {
        if (r1.getLength() != r2.getLength()) {
            throw new ConnectorException((int) ErrorCode.COLUMN_LENGTH_OF_SOURCE_RECORD_SCHEMA_MISMATCH_LENGTH_OF_FIELD_NAMES);
        }
        int i = 0;
        while (i < r1.getLength()) {
            if (r1.getFieldType(i) == ConnectorDataTypeDefinition.TYPE_UDF || r2.getFieldType(i) == ConnectorDataTypeDefinition.TYPE_UDF || ((isHiveComplexType(r1.getFieldType(i)) && r2.getFieldType(i) == 12) || ((isHiveComplexType(r2.getFieldType(i)) && r1.getFieldType(i) == 12) || r1.getFieldType(i) == r2.getFieldType(i)))) {
                i++;
            } else {
                throw new ConnectorException((int) ErrorCode.COLUMN_TYPE_OF_SOURCE_RECORD_SCHEMA_MISMATCH_FIELD_TYPE_IN_SCHEMA);
            }
        }
    }

    public static boolean isHiveComplexType(final int type) {
        return -1001 == type || -1000 == type || -1002 == type;
    }

    public static ConnectorDataTypeConverter[] initializeHiveTypeDecoder(final String targetTableSchema) throws ConnectorException {
        List<String> columns = ConnectorSchemaUtils.parseColumns(targetTableSchema.toLowerCase());
        List<String> columnNames = ConnectorSchemaUtils.parseColumnNames(columns);
        List<String> columnTypes = ConnectorSchemaUtils.parseColumnTypes(columns);
        ConnectorDataTypeConverter[] hiveDecoder = new ConnectorDataTypeConverter[columns.size()];
        for (int i = 0; i < columnTypes.size(); i++) {
            HiveDataTypeConverter converter = null;
            String targetName = (String) columnNames.get(i);
            String targetTypeNames = (String) columnTypes.get(i);
            int type = lookupHiveDataTypeByName(targetTypeNames);
            if (Pattern.matches(PATTERN_VARCHAR_TYPE, targetTypeNames.toUpperCase())) {
                type = -2001;
            }
            if (isHiveComplexType(type) || type == 3 || type == -2001 || type == 1) {
                switch (type) {
                    case -2001:
                        converter = new StringToHiveVarchar(targetName, targetTypeNames);
                        break;
                    case HiveDataTypeDefinition.TYPE_STRUCT /*-1002*/:
                        converter = new JsonStringToStruct(targetName, targetTypeNames);
                        break;
                    case HiveDataTypeDefinition.TYPE_ARRAY /*-1001*/:
                        converter = new JsonStringToArray(targetName, targetTypeNames);
                        break;
                    case HiveDataTypeDefinition.TYPE_MAP /*-1000*/:
                        converter = new JsonStringToMap(targetName, targetTypeNames);
                        converter.setDefaultValue(Integer.valueOf(HiveDataTypeDefinition.TYPE_MAP));
                        break;
                    case 1:
                        converter = new StringToHiveChar(targetName, targetTypeNames);
                        break;
                    case ConnectorDataTypeDefinition.TYPE_DECIMAL /*3*/:
                        if (HiveDecimal.class.getConstructors().length == 0) {
                            converter = new BigDecimalToHiveDecimalWithCreateMethod(targetName, targetTypeNames);
                            break;
                        }
                        converter = new BigDecimalToHiveDecimalWithConstructor(targetName, targetTypeNames);
                        break;
                }
                hiveDecoder[i] = converter;
            }
        }
        return hiveDecoder;
    }

    public static ConnectorDataTypeConverter[] initializeHiveTypeEncoder(final String sourceTableSchema) throws ConnectorException {
        List<String> columns = ConnectorSchemaUtils.parseColumns(sourceTableSchema.toLowerCase());
        List<String> columnTypes = ConnectorSchemaUtils.parseColumnTypes(columns);
        ConnectorDataTypeConverter[] jsonEncoder = new ConnectorDataTypeConverter[columns.size()];
        for (int i = 0; i < columnTypes.size(); i++) {
            ConnectorDataTypeConverter converter = new ObjectToJsonString();
            int type = lookupHiveDataTypeByName((String) columnTypes.get(i));
            if (isHiveComplexType(type)) {
                switch (type) {
                    case HiveDataTypeDefinition.TYPE_STRUCT /*-1002*/:
                    case HiveDataTypeDefinition.TYPE_ARRAY /*-1001*/:
                    case HiveDataTypeDefinition.TYPE_MAP /*-1000*/:
                        converter = new ObjectToJsonString();
                        converter.setDefaultValue("");
                        break;
                }
                jsonEncoder[i] = converter;
            }
        }
        return jsonEncoder;
    }

    private static String printHiveTableSchema(final List<FieldSchema> cols, final List<FieldSchema> partCols) {
        final StringBuffer sb = new StringBuffer();
        sb.append("# col_name              data_type               comment\n");
        printHiveTableColumn(cols, sb);
        if (partCols != null && !partCols.isEmpty()) {
            sb.append("# Partition Information\n");
            sb.append("# col_name              data_type               comment\n");
            printHiveTableColumn(partCols, sb);
        }
        return sb.toString();
    }

    private static void printHiveTableColumn(final List<FieldSchema> cols, final StringBuffer sb) {
        for (final FieldSchema col : cols) {
            final String colName = col.getName();
            final String colType = col.getType();
            final String colComment = (col.getComment() == null) ? "" : col.getComment();
            sb.append("   " + colName + "                 " + colType + "           " + colComment);
            sb.append("\n");
        }
    }
}
