package com.teradata.connector.hcat.utils;

import com.teradata.connector.common.ConnectorRecordSchema;
import com.teradata.connector.common.exception.ConnectorException;
import com.teradata.connector.common.utils.ConnectorConfiguration;
import com.teradata.connector.common.utils.ConnectorSchemaUtils;
import com.teradata.connector.hive.utils.HiveSchemaUtils;
import java.lang.reflect.Constructor;
import java.lang.reflect.Method;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.security.Credentials;

@Deprecated
public class HCatSchemaUtils
{
    private static Class<?> HCatFieldSchemaClass;
    private static Class<?> HCatInputFormatClass;
    private static Class<?> HCatOutputFormatClass;
    private static Class<?> HCatOutputJobInfoClass;
    private static Class<?> HCatSchemaClass;
    private static Class<?> HCatTableInfoClass;
    private static Constructor<?> HCatSchemaConstructor;
    private static Method HCatFieldSchemaGetNameMethod;
    private static Method HCatFieldSchemaGetTypeStringMethod;
    private static Method HCatInputFormatGetTableSchemaMethod;
    private static Method HCatInputFormatSetInputMethod;
    private static Method HCatOutputFormatSetOutputMethod;
    private static Method HCatOutputJobInfoCreateMethod;
    private static Method HCatOutputJobInfoGetTableInfoMethod;
    private static Method HCatOutputFormatSetSchemaMethod;
    private static Method HCatSchemaAppendMethod;
    private static Method HCatSchemaGetIntTypeMethod;
    private static Method HCatSchemaGetStringTypeMethod;
    private static Method HCatSchemaGetFieldsMethod;
    private static Method HCatSchemaGetFieldNamesMethod;
    private static Method HCatSchemaSizeMethod;
    private static Method HCatTableInfoGetDataColumnsMethod;
    private static Method HCatTableInfoGetPartitionColumnsMethod;

    static {
        HCatSchemaUtils.HCatFieldSchemaClass = null;
        HCatSchemaUtils.HCatInputFormatClass = null;
        HCatSchemaUtils.HCatOutputFormatClass = null;
        HCatSchemaUtils.HCatOutputJobInfoClass = null;
        HCatSchemaUtils.HCatSchemaClass = null;
        HCatSchemaUtils.HCatTableInfoClass = null;
        HCatSchemaUtils.HCatSchemaConstructor = null;
        HCatSchemaUtils.HCatFieldSchemaGetNameMethod = null;
        HCatSchemaUtils.HCatFieldSchemaGetTypeStringMethod = null;
        HCatSchemaUtils.HCatInputFormatGetTableSchemaMethod = null;
        HCatSchemaUtils.HCatInputFormatSetInputMethod = null;
        HCatSchemaUtils.HCatOutputFormatSetOutputMethod = null;
        HCatSchemaUtils.HCatOutputJobInfoCreateMethod = null;
        HCatSchemaUtils.HCatOutputJobInfoGetTableInfoMethod = null;
        HCatSchemaUtils.HCatOutputFormatSetSchemaMethod = null;
        HCatSchemaUtils.HCatSchemaAppendMethod = null;
        HCatSchemaUtils.HCatSchemaGetIntTypeMethod = null;
        HCatSchemaUtils.HCatSchemaGetStringTypeMethod = null;
        HCatSchemaUtils.HCatSchemaGetFieldsMethod = null;
        HCatSchemaUtils.HCatSchemaGetFieldNamesMethod = null;
        HCatSchemaUtils.HCatSchemaSizeMethod = null;
        HCatSchemaUtils.HCatTableInfoGetDataColumnsMethod = null;
        HCatSchemaUtils.HCatTableInfoGetPartitionColumnsMethod = null;
        try {
            HCatSchemaUtils.HCatInputFormatClass = Class.forName("org.apache.hive.hcatalog.mapreduce.HCatInputFormat");
            HCatSchemaUtils.HCatOutputFormatClass = Class.forName("org.apache.hive.hcatalog.mapreduce.HCatOutputFormat");
            HCatSchemaUtils.HCatOutputJobInfoClass = Class.forName("org.apache.hive.hcatalog.mapreduce.OutputJobInfo");
            HCatSchemaUtils.HCatTableInfoClass = Class.forName("org.apache.hive.hcatalog.mapreduce.HCatTableInfo");
            HCatSchemaUtils.HCatSchemaClass = Class.forName("org.apache.hive.hcatalog.data.schema.HCatSchema");
            HCatSchemaUtils.HCatFieldSchemaClass = Class.forName("org.apache.hive.hcatalog.data.schema.HCatFieldSchema");
        }
        catch (ClassNotFoundException e) {
            try {
                HCatSchemaUtils.HCatInputFormatClass = Class.forName("org.apache.hcatalog.mapreduce.HCatInputFormat");
                HCatSchemaUtils.HCatOutputFormatClass = Class.forName("org.apache.hcatalog.mapreduce.HCatOutputFormat");
                HCatSchemaUtils.HCatOutputJobInfoClass = Class.forName("org.apache.hcatalog.mapreduce.OutputJobInfo");
                HCatSchemaUtils.HCatTableInfoClass = Class.forName("org.apache.hcatalog.mapreduce.HCatTableInfo");
                HCatSchemaUtils.HCatSchemaClass = Class.forName("org.apache.hcatalog.data.schema.HCatSchema");
                HCatSchemaUtils.HCatFieldSchemaClass = Class.forName("org.apache.hcatalog.data.schema.HCatFieldSchema");
            }
            catch (ClassNotFoundException e3) {
                throw new RuntimeException(e);
            }
        }
        try {
            HCatSchemaUtils.HCatSchemaConstructor = HCatSchemaUtils.HCatSchemaClass.getConstructor(List.class);
            HCatSchemaUtils.HCatInputFormatSetInputMethod = HCatSchemaUtils.HCatInputFormatClass.getMethod("setInput", Configuration.class, String.class, String.class);
            HCatSchemaUtils.HCatOutputFormatSetOutputMethod = HCatSchemaUtils.HCatOutputFormatClass.getMethod("setOutput", Configuration.class, Credentials.class, HCatSchemaUtils.HCatOutputJobInfoClass);
            HCatSchemaUtils.HCatInputFormatGetTableSchemaMethod = HCatSchemaUtils.HCatInputFormatClass.getMethod("getTableSchema", Configuration.class);
            HCatSchemaUtils.HCatSchemaAppendMethod = HCatSchemaUtils.HCatSchemaClass.getMethod("append", HCatSchemaUtils.HCatFieldSchemaClass);
            HCatSchemaUtils.HCatSchemaSizeMethod = HCatSchemaUtils.HCatSchemaClass.getMethod("size", (Class<?>[])new Class[0]);
            HCatSchemaUtils.HCatSchemaGetIntTypeMethod = HCatSchemaUtils.HCatSchemaClass.getMethod("get", Integer.TYPE);
            HCatSchemaUtils.HCatSchemaGetStringTypeMethod = HCatSchemaUtils.HCatSchemaClass.getMethod("get", String.class);
            HCatSchemaUtils.HCatSchemaGetFieldsMethod = HCatSchemaUtils.HCatSchemaClass.getMethod("getFields", (Class<?>[])new Class[0]);
            HCatSchemaUtils.HCatSchemaGetFieldNamesMethod = HCatSchemaUtils.HCatSchemaClass.getMethod("getFieldNames", (Class<?>[])new Class[0]);
            HCatSchemaUtils.HCatFieldSchemaGetNameMethod = HCatSchemaUtils.HCatFieldSchemaClass.getMethod("getName", (Class<?>[])new Class[0]);
            HCatSchemaUtils.HCatFieldSchemaGetTypeStringMethod = HCatSchemaUtils.HCatFieldSchemaClass.getMethod("getTypeString", (Class<?>[])new Class[0]);
            HCatSchemaUtils.HCatOutputJobInfoCreateMethod = HCatSchemaUtils.HCatOutputJobInfoClass.getMethod("create", String.class, String.class, Map.class);
            HCatSchemaUtils.HCatOutputJobInfoGetTableInfoMethod = HCatSchemaUtils.HCatOutputJobInfoClass.getMethod("getTableInfo", (Class<?>[])new Class[0]);
            HCatSchemaUtils.HCatOutputFormatSetSchemaMethod = HCatSchemaUtils.HCatOutputFormatClass.getMethod("setSchema", Configuration.class, HCatSchemaUtils.HCatSchemaClass);
            HCatSchemaUtils.HCatTableInfoGetDataColumnsMethod = HCatSchemaUtils.HCatTableInfoClass.getMethod("getDataColumns", (Class<?>[])new Class[0]);
            HCatSchemaUtils.HCatTableInfoGetPartitionColumnsMethod = HCatSchemaUtils.HCatTableInfoClass.getMethod("getPartitionColumns", (Class<?>[])new Class[0]);
        }
        catch (NoSuchMethodException e2) {
            throw new RuntimeException(e2);
        }
    }
    
    public static String[] getTargetFieldsTypeNameFromHCatOutputFormat(final JobContext context, final String databaseName, final String tableName, final String[] fieldNames) throws ConnectorException {
        final String[] types = new String[fieldNames.length];
        int i = 0;
        try {
            final Object HCatSchemaInstance = buildHCatSchemaInstanceFromHCatOutputFormat(context, databaseName, tableName);
            for (final String name : fieldNames) {
                final Object HCatFieldSchemaInstance = HCatSchemaUtils.HCatSchemaGetStringTypeMethod.invoke(HCatSchemaInstance, name);
                types[i++] = (String)HCatSchemaUtils.HCatFieldSchemaGetTypeStringMethod.invoke(HCatFieldSchemaInstance, new Object[0]);
            }
        }
        catch (Exception e) {
            throw new ConnectorException(e.getMessage());
        }
        return types;
    }
    
    public static List<String> getHCatSchemaFieldNamesFromHCatInputFormat(final Configuration configuration, final String databaseName, final String tableName) throws ConnectorException {
        List<String> fieldNames = null;
        try {
            HCatSchemaUtils.HCatInputFormatSetInputMethod.invoke(null, configuration, databaseName, tableName);
            final Object HCatSchemaInstance = HCatSchemaUtils.HCatInputFormatGetTableSchemaMethod.invoke(null, configuration);
            fieldNames = (List<String>)HCatSchemaUtils.HCatSchemaGetFieldNamesMethod.invoke(HCatSchemaInstance, new Object[0]);
        }
        catch (Exception e) {
            throw new ConnectorException(e.getMessage());
        }
        return fieldNames;
    }
    
    private static Object buildHCatSchemaInstanceFromHCatOutputFormat(final JobContext context, final String databaseName, final String tableName) throws ConnectorException {
        final Configuration configuration = context.getConfiguration();
        Object HCatSchemaInstance;
        try {
            final Object HCatOutputJobInfoInstance = HCatSchemaUtils.HCatOutputJobInfoCreateMethod.invoke(null, databaseName, tableName, null);
            HCatSchemaUtils.HCatOutputFormatSetOutputMethod.invoke(null, configuration, context.getCredentials(), HCatOutputJobInfoInstance);
            HCatSchemaInstance = HCatSchemaUtils.HCatSchemaConstructor.newInstance(new LinkedList());
            final Object HCatTableInfoInstance = HCatSchemaUtils.HCatOutputJobInfoGetTableInfoMethod.invoke(HCatOutputJobInfoInstance, new Object[0]);
            final Object HCatSchemaInstanceDataColumns = HCatSchemaUtils.HCatTableInfoGetDataColumnsMethod.invoke(HCatTableInfoInstance, new Object[0]);
            final Object HCatSchemaInstancePartitionColumns = HCatSchemaUtils.HCatTableInfoGetPartitionColumnsMethod.invoke(HCatTableInfoInstance, new Object[0]);
            final List DataColumnFields = (List)HCatSchemaUtils.HCatSchemaGetFieldsMethod.invoke(HCatSchemaInstanceDataColumns, new Object[0]);
            final List PartitionColumnFields = (List)HCatSchemaUtils.HCatSchemaGetFieldsMethod.invoke(HCatSchemaInstancePartitionColumns, new Object[0]);
            for (final Object field : DataColumnFields) {
                HCatSchemaUtils.HCatSchemaAppendMethod.invoke(HCatSchemaInstance, field);
            }
            for (final Object field : PartitionColumnFields) {
                HCatSchemaUtils.HCatSchemaAppendMethod.invoke(HCatSchemaInstance, field);
            }
            HCatSchemaUtils.HCatOutputFormatSetSchemaMethod.invoke(null, configuration, HCatSchemaInstance);
        }
        catch (Exception e) {
            throw new ConnectorException(e.getMessage());
        }
        return HCatSchemaInstance;
    }
    
    public static List<String> getHCatSchemaFieldNamesFromHCatOutputFormat(final JobContext context, final String databaseName, final String tableName) throws ConnectorException {
        final Configuration configuration = context.getConfiguration();
        List<String> fieldNames = null;
        try {
            final Object HCatSchemaInstance = buildHCatSchemaInstanceFromHCatOutputFormat(context, databaseName, tableName);
            fieldNames = (List<String>)HCatSchemaUtils.HCatSchemaGetFieldNamesMethod.invoke(HCatSchemaInstance, new Object[0]);
        }
        catch (Exception e) {
            throw new ConnectorException(e.getMessage());
        }
        return fieldNames;
    }
    
    public static String getTableSchemaFromHCatInputFormat(final Configuration configuration, final String databaseName, final String tableName) throws ConnectorException {
        String tableSchema = null;
        try {
            HCatSchemaUtils.HCatInputFormatSetInputMethod.invoke(null, configuration, databaseName, tableName);
            final Object HCatSchemaInstance = HCatSchemaUtils.HCatInputFormatGetTableSchemaMethod.invoke(null, configuration);
            final int schemaSize = (int)HCatSchemaUtils.HCatSchemaSizeMethod.invoke(HCatSchemaInstance, new Object[0]);
            String delimiter = "";
            String fieldName = null;
            String typeName = null;
            Object HCatFieldSchemaInstance = null;
            final StringBuffer tableSchemaSB = new StringBuffer();
            for (int i = 0; i < schemaSize; ++i) {
                HCatFieldSchemaInstance = HCatSchemaUtils.HCatSchemaGetIntTypeMethod.invoke(HCatSchemaInstance, i);
                fieldName = (String)HCatSchemaUtils.HCatFieldSchemaGetNameMethod.invoke(HCatFieldSchemaInstance, new Object[0]);
                typeName = (String)HCatSchemaUtils.HCatFieldSchemaGetTypeStringMethod.invoke(HCatFieldSchemaInstance, new Object[0]);
                tableSchemaSB.append(delimiter + fieldName + " " + typeName);
                delimiter = ",";
            }
            tableSchema = tableSchemaSB.toString();
        }
        catch (Exception e) {
            throw new ConnectorException(e.getMessage());
        }
        return tableSchema;
    }
    
    public static String getTableSchemaFromHCatOutputFormat(final JobContext context, final String databaseName, final String tableName) throws ConnectorException {
        final Configuration configuration = context.getConfiguration();
        String tableSchema = null;
        try {
            final Object HCatSchemaInstance = buildHCatSchemaInstanceFromHCatOutputFormat(context, databaseName, tableName);
            final int schemaSize = (int)HCatSchemaUtils.HCatSchemaSizeMethod.invoke(HCatSchemaInstance, new Object[0]);
            String delimiter = "";
            String fieldName = null;
            String typeName = null;
            Object HCatFieldSchemaInstance = null;
            final StringBuffer tableSchemaSB = new StringBuffer();
            for (int i = 0; i < schemaSize; ++i) {
                HCatFieldSchemaInstance = HCatSchemaUtils.HCatSchemaGetIntTypeMethod.invoke(HCatSchemaInstance, i);
                fieldName = (String)HCatSchemaUtils.HCatFieldSchemaGetNameMethod.invoke(HCatFieldSchemaInstance, new Object[0]);
                typeName = (String)HCatSchemaUtils.HCatFieldSchemaGetTypeStringMethod.invoke(HCatFieldSchemaInstance, new Object[0]);
                tableSchemaSB.append(delimiter + fieldName + " " + typeName);
                delimiter = ",";
            }
            tableSchema = tableSchemaSB.toString();
        }
        catch (Exception e) {
            throw new ConnectorException(e.getMessage());
        }
        return tableSchema;
    }
    
    public static ConnectorRecordSchema getRecordSchema(final JobContext context, final String databaseName, final String tableName, final String[] fieldNames, final ConnectorConfiguration.direction direction) throws ConnectorException {
        final Configuration configuration = context.getConfiguration();
        Object HCatSchemaInstance = null;
        try {
            if (ConnectorConfiguration.direction.input == direction) {
                HCatSchemaUtils.HCatInputFormatSetInputMethod.invoke(null, configuration, databaseName, tableName);
                HCatSchemaInstance = HCatSchemaUtils.HCatInputFormatGetTableSchemaMethod.invoke(null, configuration);
            }
            else {
                HCatSchemaInstance = buildHCatSchemaInstanceFromHCatOutputFormat(context, databaseName, tableName);
            }
        }
        catch (Exception e) {
            throw new ConnectorException(e.getMessage());
        }
        if (HCatSchemaInstance == null) {
            throw new ConnectorException(14012);
        }
        ConnectorRecordSchema recordSchema;
        if (ConnectorConfiguration.direction.input == direction) {
            recordSchema = ConnectorSchemaUtils.recordSchemaFromString(ConnectorConfiguration.getInputConverterRecordSchema(configuration));
        }
        else {
            recordSchema = ConnectorSchemaUtils.recordSchemaFromString(ConnectorConfiguration.getOutputConverterRecordSchema(configuration));
        }
        List<String> hcatFieldNames = null;
        try {
            hcatFieldNames = (List<String>)HCatSchemaUtils.HCatSchemaGetFieldNamesMethod.invoke(HCatSchemaInstance, new Object[0]);
        }
        catch (Exception e2) {
            throw new ConnectorException(e2.getMessage());
        }
        ConnectorRecordSchema schema;
        if (recordSchema != null && recordSchema.getLength() > 0) {
            schema = new ConnectorRecordSchema(recordSchema.getLength());
            for (int i = 0; i < recordSchema.getLength(); ++i) {
                schema.setFieldType(i, recordSchema.getFieldType(i));
                schema.setDataTypeConverter(i, recordSchema.getDataTypeConverter(i));
                schema.setParameters(i, recordSchema.getParameters(i));
            }
        }
        else {
            schema = new ConnectorRecordSchema(fieldNames.length);
        }
        int i = 0;
        try {
            for (final String name : fieldNames) {
                Object hcatFieldSchemaInstance = null;
                int j = 0;
                for (final String fieldname : hcatFieldNames) {
                    if (name.equalsIgnoreCase(fieldname)) {
                        try {
                            hcatFieldSchemaInstance = HCatSchemaUtils.HCatSchemaGetStringTypeMethod.invoke(HCatSchemaInstance, fieldname);
                            break;
                        }
                        catch (Exception e3) {
                            throw new ConnectorException(e3.getMessage());
                        }
                    }
                    if (++j == hcatFieldNames.size()) {
                        throw new ConnectorException(14005);
                    }
                }
                String typeName = null;
                try {
                    typeName = (String)HCatSchemaUtils.HCatFieldSchemaGetTypeStringMethod.invoke(hcatFieldSchemaInstance, new Object[0]);
                }
                catch (Exception e4) {
                    throw new ConnectorException(e4.getMessage());
                }
                if (recordSchema == null || recordSchema.getLength() <= 0 || recordSchema.getFieldType(i) != 1883) {
                    schema.setFieldType(i++, HiveSchemaUtils.lookupHiveDataTypeByName(typeName));
                }
            }
        }
        catch (ConnectorException e5) {
            e5.printStackTrace();
        }
        return schema;
    }
    
    public static int[] getColumnMapping(final String[] ColNames, final String[] mappingNames) {
        final Map<String, Integer> map = new HashMap<String, Integer>();
        for (int i = 0; i < ColNames.length; ++i) {
            map.put(ColNames[i].toLowerCase().trim(), i);
        }
        final int[] mapping = new int[mappingNames.length];
        for (int j = 0; j < mappingNames.length; ++j) {
            mapping[j] = map.get(mappingNames[j].toLowerCase().trim());
        }
        return mapping;
    }
    
    public static String buildTableSchema(final Object HCatSchemaInstance) throws ConnectorException {
        int schemaSize = 0;
        try {
            schemaSize = (int)HCatSchemaUtils.HCatSchemaSizeMethod.invoke(HCatSchemaInstance, new Object[0]);
        }
        catch (Exception e) {
            throw new ConnectorException(e.getMessage());
        }
        final StringBuffer tableSchema = new StringBuffer();
        String delimiter = "";
        for (int i = 0; i < schemaSize; ++i) {
            String fieldName = null;
            String typeName = null;
            try {
                final Object HCatFieldSchemaInstance = HCatSchemaUtils.HCatSchemaGetIntTypeMethod.invoke(HCatSchemaInstance, i);
                fieldName = (String)HCatSchemaUtils.HCatFieldSchemaGetNameMethod.invoke(HCatFieldSchemaInstance, new Object[0]);
                typeName = (String)HCatSchemaUtils.HCatFieldSchemaGetTypeStringMethod.invoke(HCatFieldSchemaInstance, new Object[0]);
            }
            catch (Exception e2) {
                throw new ConnectorException(e2.getMessage());
            }
            tableSchema.append(delimiter + fieldName + " " + typeName);
            delimiter = ",";
        }
        return tableSchema.toString();
    }
}
