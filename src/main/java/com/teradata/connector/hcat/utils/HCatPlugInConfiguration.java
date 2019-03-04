package com.teradata.connector.hcat.utils;


import com.teradata.connector.common.utils.ConnectorSchemaParser;
import com.teradata.connector.common.utils.ConnectorSchemaUtils;
import com.teradata.connector.hive.utils.HiveSchemaUtils;

import java.util.List;

import org.apache.hadoop.conf.Configuration;

/**
 * @author Administrator
 */
@Deprecated
public class HCatPlugInConfiguration {
    public static final String TDCH_INPUT_HCAT_DATABASE = "tdch.input.hcat.database";
    public static final String TDCH_INPUT_HCAT_TABLE = "tdch.input.hcat.table";
    public static final String TDCH_INPUT_HCAT_TABLE_SCHEMA = "tdch.input.hcat.table.schema";
    public static final String TDCH_INPUT_HCAT_PARTITION_SCHEMA = "tdch.input.hcat.partition.schema";
    public static final String TDCH_INPUT_HCAT_FIELD_NAMES = "tdch.input.hcat.field.names";
    public static final String TDCH_INPUT_HCAT_SCHEMA_FIELD_NAMES = "tdch.input.hcat.schema.field.names";
    public static final String TDCH_INPUT_HCAT_TYPE_NAME = "tdch.input.hcat.schema.type.names";
    public static final String TDCH_OUTPUT_HCAT_DATABASE = "tdch.output.hcat.database";
    public static final String TDCH_OUTPUT_HCAT_TABLE = "tdch.output.hcat.table";
    public static final String TDCH_OUTPUT_HCAT_TABLE_SCHEMA = "tdch.output.hcat.table.schema";
    public static final String TDCH_OUTPUT_HCAT_FIELD_NAMES = "tdch.output.hcat.field.names";
    public static final String TDCH_OUTPUT_HCAT_FIELD_TYPE_NAMES = "tdch.output.hcat.field.type.names";
    public static final String TDCH_OUTPUT_HCAT_SCHEMA_FIELD_NAMES = "tdch.output.hcat.schema.field.names";
    public static final String TDCH_OUTPUT_HCAT_TYPE_NAME = "tdch.output.hcat.schema.type.names";

    public static String getInputDatabase(final Configuration configuration) {
        return configuration.get("tdch.input.hcat.database", "");
    }

    public static void setInputDatabase(final Configuration configuration, final String inputDatabase) {
        configuration.set("tdch.input.hcat.database", inputDatabase);
    }

    public static String getInputTable(final Configuration configuration) {
        return configuration.get("tdch.input.hcat.table", "");
    }

    public static void setInputTable(final Configuration configuration, final String inputTable) {
        ConnectorSchemaParser parser = new ConnectorSchemaParser();
        parser.setDelimChar('.');
        List<String> tokens = parser.tokenize(inputTable, 2, false);
        switch (tokens.size()) {
            case 1:
                configuration.set(TDCH_INPUT_HCAT_TABLE, (String) tokens.get(0));
                return;
            case 2:
                configuration.set(TDCH_INPUT_HCAT_DATABASE, (String) tokens.get(0));
                configuration.set(TDCH_INPUT_HCAT_TABLE, (String) tokens.get(1));
                return;
            default:
                return;
        }
    }

    public static void setInputTableSchema(final Configuration configuration, final String schema) {
        configuration.set("tdch.input.hcat.table.schema", schema);
    }

    public static String getInputTableSchema(final Configuration configuration) {
        return configuration.get("tdch.input.hcat.table.schema", "");
    }

    public static void setInputPartitionSchema(final Configuration configuration, final String partitionSchema) {
        configuration.set("tdch.input.hcat.partition.schema", partitionSchema);
    }

    public static String getOutputDatabase(final Configuration configuration) {
        return configuration.get("tdch.output.hcat.database", "");
    }

    public static void setOutputDatabase(final Configuration configuration, final String outputDatabase) {
        configuration.set("tdch.output.hcat.database", outputDatabase);
    }

    public static String getOutputTable(final Configuration configuration) {
        return configuration.get("tdch.output.hcat.table");
    }

    public static void setOutputTable(final Configuration configuration, final String outputTable) {
        ConnectorSchemaParser parser = new ConnectorSchemaParser();
        parser.setDelimChar('.');
        List<String> tokens = parser.tokenize(outputTable, 2, false);
        switch (tokens.size()) {
            case 1:
                configuration.set(TDCH_OUTPUT_HCAT_TABLE, (String) tokens.get(0));
                return;
            case 2:
                configuration.set(TDCH_OUTPUT_HCAT_DATABASE, (String) tokens.get(0));
                configuration.set(TDCH_OUTPUT_HCAT_TABLE, (String) tokens.get(1));
                return;
            default:
                return;
        }

    }

    public static void setOutputTableSchema(final Configuration configuration, final String schema) {
        configuration.set("tdch.output.hcat.table.schema", schema);
    }

    public static String getOutputTableSchema(final Configuration configuration) {
        return configuration.get("tdch.output.hcat.table.schema", "");
    }

    public static void setOutputFieldNamesArray(final Configuration configuration, final String[] targetFieldNamesArray) {
        configuration.setStrings("tdch.output.hcat.field.names", targetFieldNamesArray);
    }

    public static String[] getOutputFieldNamesArray(final Configuration configuration) {
        return configuration.getStrings("tdch.output.hcat.field.names", new String[0]);
    }

    public static void setOutputFieldTypeNamesArray(final Configuration configuration, String[] targetFieldNamesArray) {
        targetFieldNamesArray = ConnectorSchemaUtils.unquoteFieldNamesArray(targetFieldNamesArray);
        final String targetFieldNamesArrayJson = ConnectorSchemaUtils.fieldNamesToJson(targetFieldNamesArray);
        configuration.set("tdch.output.hcat.field.type.names", targetFieldNamesArrayJson);
    }

    public static String[] getOutputFieldTypeNamesArray(final Configuration configuration) {
        final String targetFieldNamesArrayJson = configuration.get("tdch.output.hcat.field.type.names", "");
        return ConnectorSchemaUtils.fieldNamesFromJson(targetFieldNamesArrayJson);
    }

    public static void setInputFieldNamesArray(final Configuration configuration, final String[] sourceFieldNamesArray) {
        configuration.setStrings("tdch.input.hcat.field.names", sourceFieldNamesArray);
    }

    public static String[] getInputFieldNamesArray(final Configuration configuration) {
        return configuration.getStrings("tdch.input.hcat.field.names", new String[0]);
    }

    public static void setTargetSchemaFieldNamesArray(final Configuration configuration, String[] fieldNames) {
        fieldNames = ConnectorSchemaUtils.unquoteFieldNamesArray(fieldNames);
        final String fieldNamesJson = ConnectorSchemaUtils.fieldNamesToJson(fieldNames);
        configuration.set("tdch.output.hcat.schema.field.names", fieldNamesJson);
    }

    public static String[] getTargetSchemaFieldNamesArray(final Configuration configuration) {
        final String fieldNamesJson = configuration.get("tdch.output.hcat.schema.field.names", "");
        return ConnectorSchemaUtils.fieldNamesFromJson(fieldNamesJson);
    }

    public static void setSourceSchemaFieldNamesArray(final Configuration configuration, String[] fieldNames) {
        fieldNames = ConnectorSchemaUtils.unquoteFieldNamesArray(fieldNames);
        final String fieldNamesJson = ConnectorSchemaUtils.fieldNamesToJson(fieldNames);
        configuration.set("tdch.input.hcat.schema.field.names", fieldNamesJson);
    }

    public static String[] getSourceSchemaFieldNamesArray(final Configuration configuration) {
        final String fieldNamesJson = configuration.get("tdch.input.hcat.schema.field.names", "");
        return ConnectorSchemaUtils.fieldNamesFromJson(fieldNamesJson);
    }

    public static void setInputTableFieldTypes(final Configuration configuration, final String[] typeNames) {
        configuration.set("tdch.input.hcat.schema.type.names", HiveSchemaUtils.typeNamesToJson(typeNames));
    }

    public static String[] getInputTableFieldTypes(final Configuration configuration) {
        return HiveSchemaUtils.typeNamesFromJson(configuration.get("tdch.input.hcat.schema.type.names", ""));
    }

    public static void setOutputTableFieldTypes(final Configuration configuration, String[] typeNames) {
        typeNames = ConnectorSchemaUtils.unquoteFieldNamesArray(typeNames);
        final String typeNamesJson = ConnectorSchemaUtils.fieldNamesToJson(typeNames);
        configuration.set("tdch.output.hcat.schema.type.names", typeNamesJson);
    }

    public static String[] getOutputTableFieldTypes(final Configuration configuration) {
        final String targetFieldNamesArrayJson = configuration.get("tdch.output.hcat.schema.type.names", "");
        return ConnectorSchemaUtils.fieldNamesFromJson(targetFieldNamesArrayJson);
    }
}
