package com.teradata.connector.sample.plugin.utils;

import com.teradata.connector.common.utils.ConnectorSchemaUtils;
import org.apache.hadoop.conf.Configuration;

/**
 * @author Administrator
 */
public class CommonDBConfiguration {
    private static int VALUE_BATCH_SIZE = 1000;
    public static final String DB_INPUT_FIELD_NAMES_ARRAY = "db.input.field.name.array";
    public static final String DB_INPUT_BATCH_SIZE = "db.input.batch.size";
    public static final String DB_INPUT_TABLE_DESC = "db.input.table.desc";
    public static final String DB_INPUT_SPLIT_SQL = "db.input.split.sql";
    public static final String DB_OUTPUT_TABLE_DESC = "db.output.table.desc";
    public static final String DB_OUTPUT_FIELD_NAMES = "db.output.field.names";
    public static final String DB_OUTPUT_BATCH_SIZE = "db.output.batch.size";

    public static String[] getInputFieldNamesArray(final Configuration configuration) {
        final String fieldNamesJson = configuration.get(DB_INPUT_FIELD_NAMES_ARRAY, "");
        return ConnectorSchemaUtils.fieldNamesFromJson(fieldNamesJson);
    }

    public static void setInputFieldNamesArray(final Configuration configuration, String[] sourceFieldNamesArray) {
        sourceFieldNamesArray = CommonDBSchemaUtils.unquoteFieldNamesArray(sourceFieldNamesArray);
        final String fieldNamesJson = ConnectorSchemaUtils.fieldNamesToJson(sourceFieldNamesArray);
        configuration.set(DB_INPUT_FIELD_NAMES_ARRAY, fieldNamesJson);
    }

    public static void setInputBatchSize(final Configuration configuration, final int batchSize) {
        configuration.setInt(DB_INPUT_BATCH_SIZE, batchSize);
    }

    public static int getInputBatchSize(final Configuration configuration) {
        return configuration.getInt(DB_OUTPUT_BATCH_SIZE, CommonDBConfiguration.VALUE_BATCH_SIZE);
    }

    public static void setInputTableDesc(final Configuration configuration, final String inputTableDesc) {
        configuration.set(DB_INPUT_TABLE_DESC, inputTableDesc);
    }

    public static String getInputTableDesc(final Configuration configuration) {
        return configuration.get(DB_OUTPUT_TABLE_DESC, "");
    }

    public static void setOutputTableDesc(final Configuration configuration, final String outputTableDesc) {
        configuration.set(DB_OUTPUT_TABLE_DESC, outputTableDesc);
    }

    public static String getOutputTableDesc(final Configuration configuration) {
        return configuration.get(DB_OUTPUT_TABLE_DESC, "");
    }

    public static void setOutputFieldNamesArray(final Configuration configuration, String[] targetFieldNamesArray) {
        targetFieldNamesArray = CommonDBSchemaUtils.unquoteFieldNamesArray(targetFieldNamesArray);
        final String fieldNamesJson = ConnectorSchemaUtils.fieldNamesToJson(targetFieldNamesArray);
        configuration.set(DB_OUTPUT_FIELD_NAMES, fieldNamesJson);
    }

    public static String[] getOutputFieldNamesArray(final Configuration configuration) {
        final String fieldNamesJson = configuration.get(DB_OUTPUT_FIELD_NAMES, "");
        return ConnectorSchemaUtils.fieldNamesFromJson(fieldNamesJson);
    }

    public static void setOutputBatchSize(final Configuration configuration, final int batchSize) {
        configuration.setInt(DB_OUTPUT_BATCH_SIZE, batchSize);
    }

    public static int getOutputBatchSize(final Configuration configuration) {
        return configuration.getInt(DB_OUTPUT_BATCH_SIZE, CommonDBConfiguration.VALUE_BATCH_SIZE);
    }

    public static void setInputSplitSql(final Configuration configuration, final String splitSql) {
        configuration.set(DB_INPUT_SPLIT_SQL, splitSql);
    }

    public static String getInputSplitSql(final Configuration configuration) {
        return configuration.get(DB_INPUT_SPLIT_SQL, "");
    }
}
