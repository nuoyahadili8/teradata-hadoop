package com.teradata.connector.hive.converter;

import com.teradata.connector.common.converter.ConnectorDataTypeConverter;

import java.lang.reflect.Constructor;
import java.lang.reflect.Method;
import java.math.BigDecimal;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.regex.Pattern;

import org.apache.hadoop.hive.common.type.HiveChar;
import org.apache.hadoop.hive.common.type.HiveDecimal;
import org.apache.hadoop.hive.common.type.HiveVarchar;
import org.apache.hadoop.hive.serde2.typeinfo.ListTypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.MapTypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.StructTypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfoFactory;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfoUtils;
import org.codehaus.jackson.map.ObjectMapper;

public abstract class HiveDataTypeConverter extends ConnectorDataTypeConverter {
    protected static final String PATTERN_VARCHAR_TYPE = "\\s*VARCHAR\\s*\\(\\s*(\\d+)\\s*\\)";
    protected static final String PATTERN_CHAR_TYPE = "\\s*CHAR\\s*\\(\\s*(\\d+)\\s*\\)";
    protected String colName;
    protected String colTypeName;

    protected HiveDataTypeConverter(final String colName, final String typeName) {
        this.colName = colName;
        this.colTypeName = typeName;
    }

    private static final Object parseKeyField(final Object field, final TypeInfo fieldTypeInfo) {
        switch (fieldTypeInfo.getCategory()) {
            case PRIMITIVE: {
                final String typeName = fieldTypeInfo.getTypeName();
                if (typeName.equals("string")) {
                    return field;
                }
                if (Pattern.matches("\\s*CHAR\\s*\\(\\s*(\\d+)\\s*\\)", typeName.toUpperCase())) {
                    final HiveChar hc = new HiveChar(field.toString(), field.toString().length());
                    return hc;
                }
                if (Pattern.matches("\\s*VARCHAR\\s*\\(\\s*(\\d+)\\s*\\)", typeName.toUpperCase())) {
                    final HiveVarchar hv = new HiveVarchar(field.toString(), field.toString().length());
                    return hv;
                }
                if (typeName.equals("int")) {
                    return Integer.valueOf((String) field);
                }
                if (typeName.equals("bigint")) {
                    return Long.valueOf((String) field);
                }
                if (typeName.equals("smallint")) {
                    return Short.valueOf((String) field);
                }
                if (typeName.equals("tinyint")) {
                    return Byte.valueOf((String) field);
                }
                if (typeName.equals("float")) {
                    return Float.valueOf((String) field);
                }
                if (typeName.equals("double")) {
                    return Double.valueOf((String) field);
                }
                if (typeName.equals("timestamp")) {
                    return Timestamp.valueOf((String) field);
                }
                if (typeName.equals("binary")) {
                    return ((String) field).getBytes();
                }
                return field;
            }
            case LIST: {
                return parseList(field, (ListTypeInfo) fieldTypeInfo);
            }
            case MAP: {
                return parseMap(field, (MapTypeInfo) fieldTypeInfo);
            }
            case STRUCT: {
                return parseStruct(field, (StructTypeInfo) fieldTypeInfo);
            }
            default: {
                return null;
            }
        }
    }

    private static final Object parseValueField(final Object field, final TypeInfo fieldTypeInfo) {
        switch (fieldTypeInfo.getCategory()) {
            case PRIMITIVE: {
                final String typeName = fieldTypeInfo.getTypeName();
                if (typeName.equals("string")) {
                    return field.toString();
                }
                if (typeName.equals("float")) {
                    if (field instanceof Double) {
                        return ((Double) field).floatValue();
                    }
                    if (field instanceof String) {
                        return Float.valueOf((String) field);
                    }
                    return field;
                } else {
                    if (Pattern.matches("\\s*CHAR\\s*\\(\\s*(\\d+)\\s*\\)", typeName.toUpperCase())) {
                        final HiveChar hc = new HiveChar(field.toString(), field.toString().length());
                        return hc;
                    }
                    if (Pattern.matches("\\s*VARCHAR\\s*\\(\\s*(\\d+)\\s*\\)", typeName.toUpperCase())) {
                        final HiveVarchar hv = new HiveVarchar(field.toString(), field.toString().length());
                        return hv;
                    }
                    if (typeName.equals("bigint")) {
                        if (field instanceof Integer) {
                            return field;
                        }
                        if (field instanceof String) {
                            return Long.valueOf((String) field);
                        }
                        return field;
                    } else if (typeName.equals("smallint")) {
                        if (field instanceof Integer) {
                            return ((Integer) field).shortValue();
                        }
                        if (field instanceof String) {
                            return Short.valueOf((String) field);
                        }
                        return field;
                    } else if (typeName.equals("tinyint")) {
                        if (field instanceof Integer) {
                            return ((Integer) field).byteValue();
                        }
                        if (field instanceof String) {
                            return Byte.valueOf((String) field);
                        }
                        return field;
                    } else if (typeName.equals("timestamp")) {
                        if (field instanceof Long) {
                            return new Timestamp((long) field);
                        }
                        if (field instanceof String) {
                            return Timestamp.valueOf((String) field);
                        }
                        return field;
                    } else {
                        if (!typeName.equals("binary")) {
                            return field;
                        }
                        if (field instanceof String) {
                            return ((String) field).getBytes();
                        }
                        return field;
                    }
                }
            }
            case LIST: {
                return parseList(field, (ListTypeInfo) fieldTypeInfo);
            }
            case MAP: {
                return parseMap(field, (MapTypeInfo) fieldTypeInfo);
            }
            case STRUCT: {
                return parseStruct(field, (StructTypeInfo) fieldTypeInfo);
            }
            default: {
                return null;
            }
        }
    }

    private static final Object parseStruct(final Object field, final StructTypeInfo fieldTypeInfo) {
        final ArrayList<Object> list = (ArrayList<Object>) field;
        final ArrayList<TypeInfo> structTypes = (ArrayList<TypeInfo>) fieldTypeInfo.getAllStructFieldTypeInfos();
        final ArrayList<String> structNames = (ArrayList<String>) fieldTypeInfo.getAllStructFieldNames();
        final List<Object> newList = new ArrayList<Object>();
        for (int i = 0; i < structNames.size(); ++i) {
            newList.add(parseValueField(list.get(i), structTypes.get(i)));
        }
        return newList;
    }

    private static final Object parseList(final Object field, final ListTypeInfo fieldTypeInfo) {
        final ArrayList<Object> list = (ArrayList<Object>) field;
        final TypeInfo elemTypeInfo = fieldTypeInfo.getListElementTypeInfo();
        final ArrayList<Object> newList = new ArrayList<Object>(list.size());
        for (int i = 0; i < list.size(); ++i) {
            newList.add(parseValueField(list.get(i), elemTypeInfo));
        }
        return newList;
    }

    private static final Object parseMap(final Object field, final MapTypeInfo fieldTypeInfo) {
        final Map<Object, Object> map = (Map<Object, Object>) field;
        final TypeInfo keyTypeInfo = fieldTypeInfo.getMapKeyTypeInfo();
        final TypeInfo valueTypeInfo = fieldTypeInfo.getMapValueTypeInfo();
        final Map<Object, Object> newMap = new HashMap<Object, Object>();
        for (final Map.Entry<Object, Object> entry : map.entrySet()) {
            newMap.put(parseKeyField(entry.getKey(), keyTypeInfo), parseValueField(entry.getValue(), valueTypeInfo));
        }
        return newMap;
    }

    public String getColName() {
        return this.colName;
    }

    public void setColName(final String colName) {
        this.colName = colName;
    }

    public String getColTypeName() {
        return this.colTypeName;
    }

    public void setColTypeName(final String colTypeName) {
        this.colTypeName = colTypeName;
    }

    public static final class JsonStringToMap extends HiveDataTypeConverter {
        public JsonStringToMap(final String colName, final String typeName) {
            super(colName, typeName);
            this.defaultValue = HiveDataTypeDefinition.MAP_NULL_VALUE;
        }

        @Override
        public final Object convert(final Object object) {
            if (object == null) {
                return this.nullable ? null : this.defaultValue;
            }
            final List<TypeInfo> columnType = (List<TypeInfo>) TypeInfoUtils.getTypeInfosFromTypeString(this.colTypeName.toLowerCase());
            final List<String> columnName = new ArrayList<String>();
            columnName.add(this.colName);
            final StructTypeInfo rowTypeInfo = (StructTypeInfo) TypeInfoFactory.getStructTypeInfo((List) columnName, (List) columnType);
            Map<?, ?> root = null;
            try {
                final ObjectMapper mapper = new ObjectMapper();
                root = (Map<?, ?>) mapper.readValue(object.toString(), (Class) Map.class);
            } catch (Exception e) {
                throw new RuntimeException(e.getMessage(), e);
            }
            Object value = null;
            try {
                final TypeInfo fieldTypeInfo = rowTypeInfo.getStructFieldTypeInfo((String) columnName.get(0));
                value = parseValueField(root, fieldTypeInfo);
            } catch (Exception e2) {
                throw new RuntimeException(e2.getMessage(), e2);
            }
            return value;
        }
    }

    public static final class JsonStringToArray extends HiveDataTypeConverter {
        public JsonStringToArray(final String colName, final String typeName) {
            super(colName, typeName);
            this.defaultValue = HiveDataTypeDefinition.ARRAY_NULL_VALUE;
        }

        @Override
        public final Object convert(final Object object) {
            if (object == null) {
                return this.nullable ? null : this.defaultValue;
            }
            final List<TypeInfo> columnType = (List<TypeInfo>) TypeInfoUtils.getTypeInfosFromTypeString(this.colTypeName);
            final List<String> columnName = new ArrayList<String>();
            columnName.add(this.colName);
            final StructTypeInfo rowTypeInfo = (StructTypeInfo) TypeInfoFactory.getStructTypeInfo((List) columnName, (List) columnType);
            List<?> root = null;
            try {
                final ObjectMapper mapper = new ObjectMapper();
                root = (List<?>) mapper.readValue(object.toString(), (Class) List.class);
            } catch (Exception e) {
                throw new RuntimeException(e.getMessage(), e);
            }
            Object value = null;
            try {
                final TypeInfo fieldTypeInfo = rowTypeInfo.getStructFieldTypeInfo((String) columnName.get(0));
                value = parseValueField(root, fieldTypeInfo);
            } catch (Exception e2) {
                throw new RuntimeException(e2.getMessage(), e2);
            }
            return value;
        }
    }

    public static final class JsonStringToStruct extends HiveDataTypeConverter {
        public JsonStringToStruct(final String colName, final String typeName) {
            super(colName, typeName);
            this.defaultValue = HiveDataTypeDefinition.STRUCT_NULL_VALUE;
        }

        @Override
        public Object convert(final Object object) {
            if (object == null) {
                return this.nullable ? null : this.defaultValue;
            }
            final List<TypeInfo> columnType = (List<TypeInfo>) TypeInfoUtils.getTypeInfosFromTypeString(this.colTypeName);
            final List<String> columnName = new ArrayList<String>();
            columnName.add(this.colName);
            final StructTypeInfo rowTypeInfo = (StructTypeInfo) TypeInfoFactory.getStructTypeInfo((List) columnName, (List) columnType);
            List<?> root = null;
            try {
                final ObjectMapper mapper = new ObjectMapper();
                root = (List<?>) mapper.readValue(object.toString(), (Class) List.class);
            } catch (Exception e) {
                throw new RuntimeException(e.getMessage(), e);
            }
            Object value = null;
            try {
                final TypeInfo fieldTypeInfo = rowTypeInfo.getStructFieldTypeInfo((String) columnName.get(0));
                value = parseValueField(root, fieldTypeInfo);
            } catch (Exception e2) {
                throw new RuntimeException(e2.getMessage(), e2);
            }
            return value;
        }
    }

    public static final class BigDecimalToHiveDecimalWithConstructor extends HiveDataTypeConverter {
        Constructor<?> cost;

        public BigDecimalToHiveDecimalWithConstructor(final String colName, final String typeName) {
            super(colName, typeName);
            this.cost = null;
            try {
                this.cost = HiveDecimal.class.getDeclaredConstructor(BigDecimal.class);
            } catch (Exception e) {
                throw new RuntimeException(e.getMessage(), e);
            }
        }

        @Override
        public Object convert(final Object object) {
            if (object == null) {
                return this.nullable ? null : this.defaultValue;
            }
            try {
                return (HiveDecimal) this.cost.newInstance((BigDecimal) object);
            } catch (Exception e) {
                throw new RuntimeException(e.getMessage(), e);
            }
        }
    }

    public static final class StringToHiveVarchar extends HiveDataTypeConverter {
        public StringToHiveVarchar(final String colName, final String typeName) {
            super(colName, typeName);
        }

        @Override
        public final Object convert(final Object object) {
            if (object == null) {
                return this.nullable ? null : this.defaultValue;
            }
            final String objstr = (String) object;
            final HiveVarchar hv = new HiveVarchar(objstr, objstr.length());
            return hv;
        }
    }

    public static final class StringToHiveChar extends HiveDataTypeConverter {
        public StringToHiveChar(final String colName, final String typeName) {
            super(colName, typeName);
        }

        @Override
        public final Object convert(final Object object) {
            if (object == null) {
                return this.nullable ? null : this.defaultValue;
            }
            final String objstr = (String) object;
            final HiveChar hc = new HiveChar(objstr, objstr.length());
            return hc;
        }
    }

    public static final class BigDecimalToHiveDecimalWithCreateMethod extends HiveDataTypeConverter {
        Method create;

        public BigDecimalToHiveDecimalWithCreateMethod(final String colName, final String typeName) {
            super(colName, typeName);
            this.create = null;
            try {
                this.create = HiveDecimal.class.getDeclaredMethod("create", BigDecimal.class);
            } catch (Exception e) {
                throw new RuntimeException(e.getMessage(), e);
            }
        }

        @Override
        public Object convert(final Object object) {
            if (object == null) {
                return this.nullable ? null : this.defaultValue;
            }
            try {
                return this.create.invoke(null, (BigDecimal) object);
            } catch (Exception e) {
                throw new RuntimeException(e.getMessage(), e);
            }
        }
    }
}
