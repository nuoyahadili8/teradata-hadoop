package com.teradata.connector.hdfs.utils;

import com.teradata.connector.common.exception.ConnectorException;
import com.teradata.connector.common.utils.ConnectorSchemaUtils;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.regex.Pattern;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

/**
 * @author Administrator
 */
public class HdfsSchemaUtils {
    private static Log logger = LogFactory.getLog((Class) HdfsSchemaUtils.class);

    public static void checkFieldNamesInSchema(final String[] fieldNames, final String tableSchema) throws ConnectorException {
        final List<String> tableFields = ConnectorSchemaUtils.parseColumns(tableSchema.toLowerCase());
        final List<String> tableFieldNames = ConnectorSchemaUtils.parseColumnNames(tableFields);
        checkFieldNames(fieldNames, tableFieldNames);
    }

    public static void checkFieldNames(final String[] fieldNames, final List<String> sourceFieldNames) throws ConnectorException {
        final List<String> lowerCaseFieldNames = new ArrayList<String>(sourceFieldNames.size());
        for (final String name : sourceFieldNames) {
            lowerCaseFieldNames.add(name.toLowerCase());
        }
        for (final String name2 : fieldNames) {
            if (!lowerCaseFieldNames.contains(name2.toLowerCase())) {
                throw new ConnectorException(14005);
            }
        }
    }

    public static int[] getColumnMapping(final List<String> ColNames, final String[] mappingNames) {
        final Map<String, Integer> map = new HashMap<String, Integer>();
        final String[] ColNamesArray = new String[ColNames.size()];
        ColNames.toArray(ColNamesArray);
        for (int i = 0; i < ColNamesArray.length; ++i) {
            map.put(ColNamesArray[i].toLowerCase().trim(), i);
        }
        final int[] mapping = new int[mappingNames.length];
        for (int j = 0; j < mappingNames.length; ++j) {
            mapping[j] = map.get(mappingNames[j].toLowerCase().trim());
        }
        return mapping;
    }

    public static void checkDataType(final List<String> schemaDataTypes, final int[] recordDataType) throws ConnectorException {
        for (final int datatype : recordDataType) {
            if (datatype != 1883) {
                int j = 0;
                for (final String schema : schemaDataTypes) {
                    if (lookupHdfsAvroDatatype(schema) == datatype) {
                        break;
                    }
                    ++j;
                }
                if (j == schemaDataTypes.size()) {
                    throw new ConnectorException(14015);
                }
            }
        }
    }

    public static int[] getDataTypes(final List<String> recordSchema) {
        final List<String> recordDataTypes = ConnectorSchemaUtils.parseColumnTypes(recordSchema);
        final int[] dataTypes = new int[recordDataTypes.size()];
        int i = 0;
        for (final String dataType : recordDataTypes) {
            dataTypes[i] = lookupHdfsAvroDatatype(dataType);
            ++i;
        }
        return dataTypes;
    }

    public static int[] getDataTypes(final List<String> recordSchema, final int[] mappings) {
        final List<String> recordDataTypes = ConnectorSchemaUtils.parseColumnTypes(recordSchema);
        final int[] dataTypes = new int[mappings.length];
        for (int i = mappings.length - 1; i >= 0; --i) {
            dataTypes[i] = lookupHdfsAvroDatatype(recordDataTypes.get(mappings[i]));
        }
        return dataTypes;
    }

    public static boolean isCharType(final int dataType) {
        switch (dataType) {
            case -1:
            case 1:
            case 12:
            case 2005: {
                return true;
            }
            default: {
                return false;
            }
        }
    }

    public static int lookupHdfsAvroDatatype(String typeName) {
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
        if (typeName.equals("MAP") || Pattern.matches("\\s*(map\\s*[<].*[>])", typeName.toLowerCase())) {
            return -2004;
        }
        if (typeName.equals("ARRAY") || Pattern.matches("\\s*(array\\s*[<].*[>])", typeName.toLowerCase())) {
            return -2003;
        }
        if (typeName.equals("STRUCT") || typeName.equals("RECORD") || Pattern.matches("\\s*(struct\\s*[<].*[>])", typeName.toLowerCase())) {
            return -2001;
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
            return -2002;
        }
        if (typeName.equals("NULL")) {
            return -2000;
        }
        if (typeName.equals("UNION")) {
            return -2005;
        }
        if (typeName.equals("FIXED")) {
            return -2006;
        }
        if (typeName.equals("NUMERIC")) {
            return 3;
        }
        if (typeName.equals("OTHER")) {
            return 1882;
        }
        return 1883;
    }
}
