package com.teradata.connector.hive.utils;

import com.teradata.connector.common.exception.ConnectorException;
import com.teradata.connector.common.utils.ConnectorConfiguration;
import com.teradata.connector.common.utils.ConnectorSchemaParser;
import com.teradata.connector.common.utils.ConnectorSchemaUtils;
import com.teradata.connector.common.utils.ConnectorUnicodeCharacterConverter;
import java.util.List;
import org.apache.hadoop.conf.Configuration;


/**
 * @author Administrator
 */
public class HivePlugInConfiguration extends ConnectorConfiguration {
    private static final String TDCH_INPUT_HIVE_NULL_STRING = "tdch.input.hive.null.string";
    private static final String TDCH_INPUT_HIVE_FIELDS_SEPARATOR = "tdch.input.hive.fields.separator";
    private static final String TDCH_INPUT_HIVE_TABLE_SCHEMA = "tdch.input.hive.table.schema";
    private static final String TDCH_INPUT_HIVE_PARTITION_SCHEMA = "tdch.input.hive.partition.schema";
    private static final String TDCH_INPUT_HIVE_CONF_FILE = "tdch.input.hive.conf.file";
    private static final String TDCH_INPUT_HIVE_LINE_SEPARATOR = "tdch.input.hive.line.separator";
    private static final String TDCH_INPUT_HIVE_PATHS = "tdch.input.hive.paths";
    private static final String TDCH_INPUT_PARQUET_READ_SUPPORT_CLASS = "parquet.read.support.class";
    private static final String TDCH_INPUT_READ_SUPPORT_CLASS_VALUE = "com.teradata.connector.hive.HiveParquetReadSupportExt";
    private static final String TDCH_INPUT_HIVE_DATABASE = "tdch.input.hive.database";
    private static final String TDCH_INPUT_HIVE_TABLE = "tdch.input.hive.table";
    private static final String TDCH_INPUT_HIVE_TYPE_NAME = "tdch.input.hive.type.name";
    private static final String TDCH_INPUT_HIVE_SCHEMA_FIELD_NAMES = "tdch.input.hive.schema.field.names";
    private static final String TDCH_INPUT_HIVE_FIELD_NAMES = "tdch.input.hive.field.names";
    private static final String TDCH_INPUT_HIVE_PARTITION_FILTERS = "tdch.input.hive.partition.filter";
    private static final String TDCH_INPUT_HIVE_RCFILE_SERDE = "tdch.input.hive.rcfile.serde";
    private static final String TDCH_OUTPUT_HIVE_NULL_STRING = "tdch.output.hive.null.string";
    private static final String TDCH_OUTPUT_HIVE_FIELDS_SEPARATOR = "tdch.output.hive.fields_separator";
    private static final String TDCH_OUTPUT_HIVE_TABLE_SCHEMA = "tdch.output.hive.table.schema";
    private static final String TDCH_OUTPUT_HIVE_PARTITION_SCHEMA = "tdch.output.hive.partition.schema";
    private static final String TDCH_OUTPUT_HIVE_CONF_FILE = "tdch.output.hive.conf.file";
    private static final String TDCH_OUTPUT_HIVE_LINE_SEPARATOR = "tdch.output.hive.line.separator";
    private static final String TDCH_OUTPUT_HIVE_PATHS = "tdch.output.hive.paths";
    private static final String TDCH_OUTPUT_HIVE_DATABASE = "tdch.output.hive.database";
    private static final String TDCH_OUTPUT_HIVE_TABLE = "tdch.output.hive.table";
    private static final String TDCH_OUTPUT_HIVE_TYPE_NAME = "tdch.output.hive.type.name";
    private static final String TDCH_OUTPUT_HIVE_SCHEMA_FIELD_NAMES = "tdch.output.hive.schema.field.names";
    private static final String TDCH_OUTPUT_HIVE_FIELD_NAMES = "tdch.output.hive.field.names";
    private static final String TDCH_OUTPUT_HIVE_RCFILE_SERDE = "tdch.output.hive.rcfile.serde";
    private static final String TDCH_OUTPUT_HIVE_OVERWRITE = "tdch.output.hive.overwrite";
    private static final String TDCH_HIVE_TABLE_AVRO_URL = "avro.schema.url";
    private static final String TDCH_HIVE_TABLE_AVRO_LITERAL = "avro.schema.literal";
    public static final String VALUE_FIELDS_SEPARATOR = "\\u0001";
    public static final String VALUE_LINE_SEPARATOR = "\n";

    public static String getInputNullString(final Configuration configuration) {
        return configuration.get("tdch.input.hive.null.string");
    }

    public static void setInputNullString(final Configuration configuration, final String nullString) {
        configuration.set("tdch.input.hive.null.string", nullString.trim());
    }

    public static String getReadSupportClass(final Configuration configuration) {
        return configuration.get("parquet.read.support.class");
    }

    public static void setReadSupportClass(final Configuration configuration) {
        configuration.set("parquet.read.support.class", "com.teradata.connector.hive.HiveParquetReadSupportExt");
    }

    public static void setInputSeparator(final Configuration configuration, String separator) throws ConnectorException {
        if (separator == null) {
            separator = "\\u0001";
        }
        if (separator != null && separator.length() > 0) {
            configuration.set("tdch.input.hive.fields.separator", separator);
            ConnectorUnicodeCharacterConverter.fromEncodedUnicode(separator);
        }
    }

    public static String getInputSeparator(final Configuration configuration) throws ConnectorException {
        String separator = configuration.get("tdch.input.hive.fields.separator", "\\u0001");
        if (separator != null && separator.length() > 0) {
            separator = ConnectorUnicodeCharacterConverter.fromEncodedUnicode(separator);
            separator = separator.substring(0, 1);
        }
        return separator;
    }

    public static void setInputTableSchema(final Configuration configuration, final String schema) {
        configuration.set("tdch.input.hive.table.schema", schema);
    }

    public static String getInputTableSchema(final Configuration configuration) {
        return configuration.get("tdch.input.hive.table.schema", "");
    }

    public static void setInputTableAvroURL(final Configuration configuration, final String url) {
        configuration.set("avro.schema.url", url);
    }

    public static String getInputTableAvroURL(final Configuration configuration) {
        return configuration.get("avro.schema.url", "");
    }

    public static void setInputTableAvroLiteral(final Configuration configuration, final String literal) {
        configuration.set("avro.schema.literal", literal);
    }

    public static String getInputTableAvroLiteral(final Configuration configuration) {
        return configuration.get("avro.schema.literal", "");
    }

    public static void setInputPartitionSchema(final Configuration configuration, final String schema) {
        configuration.set("tdch.input.hive.partition.schema", schema);
    }

    public static String getInputPartitionSchema(final Configuration configuration) {
        return configuration.get("tdch.input.hive.partition.schema", "");
    }

    public static void setInputConfigureFile(final Configuration configuration, final String filePath) {
        if (filePath != null && filePath.trim().length() != 0) {
            configuration.set("tdch.input.hive.conf.file", filePath.trim());
        }
    }

    public static String getInputConfigureFile(final Configuration configuration) {
        return configuration.get("tdch.input.hive.conf.file", "");
    }

    public static void setInputLineSeparator(final Configuration configuration, final String lineSeparator) throws ConnectorException {
        configuration.set("tdch.input.hive.line.separator", lineSeparator);
        if (lineSeparator != null && lineSeparator.length() > 0) {
            ConnectorUnicodeCharacterConverter.fromEncodedUnicode(lineSeparator);
        }
    }

    public static String getInputLineSeparator(final Configuration configuration) throws ConnectorException {
        String lineSeparator = configuration.get("tdch.input.hive.line.separator", "\n");
        if (lineSeparator != null && lineSeparator.length() > 0) {
            lineSeparator = ConnectorUnicodeCharacterConverter.fromEncodedUnicode(lineSeparator);
            lineSeparator = lineSeparator.substring(0, 1);
        }
        return lineSeparator;
    }

    public static void setInputPaths(final Configuration configuration, final String paths) {
        configuration.set("tdch.input.hive.paths", paths);
        configuration.set("mapred.input.dir", paths);
    }

    public static String getInputPaths(final Configuration configuration) {
        return configuration.get("tdch.input.hive.paths", "");
    }

    public static void setInputDatabase(final Configuration configuration, final String inputDatabase) {
        configuration.set("tdch.input.hive.database", inputDatabase);
    }

    public static String getInputDatabase(final Configuration configuration) {
        return configuration.get("tdch.input.hive.database", "");
    }

    public static void setInputTable(final Configuration configuration, final String inputTable) {
        final ConnectorSchemaParser parser = new ConnectorSchemaParser();
        parser.setDelimChar('.');
        final List<String> tokens = parser.tokenize(inputTable, 2, false);
        switch (tokens.size()) {
            case 1: {
                configuration.set("tdch.input.hive.table", (String) tokens.get(0));
                break;
            }
            case 2: {
                configuration.set("tdch.input.hive.database", (String) tokens.get(0));
                configuration.set("tdch.input.hive.table", (String) tokens.get(1));
                break;
            }
        }
    }

    public static String getInputTable(final Configuration configuration) {
        return configuration.get("tdch.input.hive.table", "");
    }

    public static void setInputTableFieldTypes(final Configuration configuration, final String[] typeNames) {
        configuration.set("tdch.input.hive.type.name", HiveSchemaUtils.typeNamesToJson(typeNames));
    }

    public static String[] getInputTableFieldTypes(final Configuration configuration) {
        return HiveSchemaUtils.typeNamesFromJson(configuration.get("tdch.input.hive.type.name", ""));
    }

    public static void setInputTableFieldNamesArray(final Configuration configuration, String[] fieldNames) {
        fieldNames = ConnectorSchemaUtils.unquoteFieldNamesArray(fieldNames);
        final String fieldNamesJson = ConnectorSchemaUtils.fieldNamesToJson(fieldNames);
        configuration.set("tdch.input.hive.schema.field.names", fieldNamesJson);
    }

    public static String[] getInputTableFieldNamesArray(final Configuration configuration) {
        final String fieldNamesJson = configuration.get("tdch.input.hive.schema.field.names", "");
        return ConnectorSchemaUtils.fieldNamesFromJson(fieldNamesJson);
    }

    public static void setInputFieldNamesArray(final Configuration configuration, String[] fieldNames) {
        fieldNames = ConnectorSchemaUtils.unquoteFieldNamesArray(fieldNames);
        final String fieldNamesJson = ConnectorSchemaUtils.fieldNamesToJson(fieldNames);
        configuration.set("tdch.input.hive.field.names", fieldNamesJson);
    }

    public static String[] getInputFieldNamesArray(final Configuration configuration) {
        final String fieldNamesJson = configuration.get("tdch.input.hive.field.names", "");
        return ConnectorSchemaUtils.fieldNamesFromJson(fieldNamesJson);
    }

    public static void setInputPartitionFilters(final Configuration configuration, final String[] filters) {
        configuration.setStrings("tdch.input.hive.partition.filter", filters);
    }

    public static String[] getInputPartitionFilters(final Configuration configuration) {
        return configuration.getStrings("tdch.input.hive.partition.filter", new String[0]);
    }

    public static void setInputRCFileSerde(final Configuration configuration, final String serde) {
        configuration.set("tdch.input.hive.rcfile.serde", serde);
    }

    public static String getInputRCFileSerde(final Configuration configuration) {
        return configuration.get("tdch.input.hive.rcfile.serde", "");
    }

    public static String getOutputNullString(final Configuration configuration) {
        return configuration.get("tdch.output.hive.null.string");
    }

    public static void setOutputNullString(final Configuration configuration, final String nullString) {
        configuration.set("tdch.output.hive.null.string", nullString.trim());
    }

    public static void setOutputSeparator(final Configuration configuration, String separator) throws ConnectorException {
        if (separator == null) {
            separator = "\\u0001";
        }
        if (separator != null && separator.length() > 0) {
            configuration.set("tdch.output.hive.fields_separator", separator);
            ConnectorUnicodeCharacterConverter.fromEncodedUnicode(separator);
        }
    }

    public static String getOutputSeparator(final Configuration configuration) throws ConnectorException {
        String separator = configuration.get("tdch.output.hive.fields_separator", "\\u0001");
        if (separator != null && separator.length() > 0) {
            separator = ConnectorUnicodeCharacterConverter.fromEncodedUnicode(separator);
            separator = separator.substring(0, 1);
        }
        return separator;
    }

    public static void setOutputLineSeparator(final Configuration configuration, final String separator) throws ConnectorException {
        configuration.set("tdch.output.hive.line.separator", separator);
        if (separator != null && separator.length() > 0) {
            ConnectorUnicodeCharacterConverter.fromEncodedUnicode(separator);
        }
    }

    public static String getOutputLineSeparator(final Configuration configuration) throws ConnectorException {
        String lineSeparator = configuration.get("tdch.output.hive.line.separator", "\n");
        if (lineSeparator != null && lineSeparator.length() > 0) {
            lineSeparator = ConnectorUnicodeCharacterConverter.fromEncodedUnicode(lineSeparator);
            lineSeparator = lineSeparator.substring(0, 1);
        }
        return lineSeparator;
    }

    public static void setOutputTableSchema(final Configuration configuration, final String schema) {
        configuration.set("tdch.output.hive.table.schema", schema);
    }

    public static String getOutputTableSchema(final Configuration configuration) {
        return configuration.get("tdch.output.hive.table.schema", "");
    }

    public static void setOutputTableAvroURL(final Configuration configuration, final String url) {
        configuration.set("avro.schema.url", url);
    }

    public static String getOutputTableAvroURL(final Configuration configuration) {
        return configuration.get("avro.schema.url", "");
    }

    public static void setOutputTableAvroLiteral(final Configuration configuration, final String literal) {
        configuration.set("avro.schema.literal", literal);
    }

    public static String getOutputTableAvroLiteral(final Configuration configuration) {
        return configuration.get("avro.schema.literal", "");
    }

    public static void setOutputPaths(final Configuration configuration, final String paths) {
        configuration.set("tdch.output.hive.paths", paths);
        configuration.set("mapred.output.dir", paths);
    }

    public static String getOutputPaths(final Configuration configuration) {
        return configuration.get("tdch.output.hive.paths", "");
    }

    public static void setOutputConfigureFile(final Configuration configuration, final String filePath) {
        if (filePath != null && filePath.trim().length() != 0) {
            configuration.set("tdch.output.hive.conf.file", filePath.trim());
        }
    }

    public static String getOutputConfigureFile(final Configuration configuration) {
        return configuration.get("tdch.output.hive.conf.file", "");
    }

    public static void setOutputDatabase(final Configuration configuration, final String inputDatabase) {
        configuration.set("tdch.output.hive.database", inputDatabase);
    }

    public static String getOutputDatabase(final Configuration configuration) {
        return configuration.get("tdch.output.hive.database", "");
    }

    public static void setOutputTable(final Configuration configuration, final String inputTable) {
        final ConnectorSchemaParser parser = new ConnectorSchemaParser();
        parser.setDelimChar('.');
        final List<String> tokens = parser.tokenize(inputTable, 2, false);
        switch (tokens.size()) {
            case 1: {
                configuration.set("tdch.output.hive.table", (String) tokens.get(0));
                break;
            }
            case 2: {
                configuration.set("tdch.output.hive.database", (String) tokens.get(0));
                configuration.set("tdch.output.hive.table", (String) tokens.get(1));
                break;
            }
        }
    }

    public static String getOutputTable(final Configuration configuration) {
        return configuration.get("tdch.output.hive.table", "");
    }

    public static void setOutputTableFieldTypes(final Configuration configuration, String[] typeNames) {
        typeNames = ConnectorSchemaUtils.unquoteFieldNamesArray(typeNames);
        final String typeNamesJson = ConnectorSchemaUtils.fieldNamesToJson(typeNames);
        configuration.set("tdch.output.hive.type.name", typeNamesJson);
    }

    public static String[] getOutputTableFieldTypes(final Configuration configuration) {
        final String targetFieldNamesArrayJson = configuration.get("tdch.output.hive.type.name", "");
        return ConnectorSchemaUtils.fieldNamesFromJson(targetFieldNamesArrayJson);
    }

    public static void setOutputTableFieldNamesArray(final Configuration configuration, String[] fieldNames) {
        fieldNames = ConnectorSchemaUtils.unquoteFieldNamesArray(fieldNames);
        final String fieldNamesJson = ConnectorSchemaUtils.fieldNamesToJson(fieldNames);
        configuration.set("tdch.output.hive.schema.field.names", fieldNamesJson);
    }

    public static String[] getOutputTableFieldNamesArray(final Configuration configuration) {
        final String fieldNamesJson = configuration.get("tdch.output.hive.schema.field.names", "");
        return ConnectorSchemaUtils.fieldNamesFromJson(fieldNamesJson);
    }

    public static void setOutputPartitionSchema(final Configuration configuration, final String schema) {
        configuration.set("tdch.output.hive.partition.schema", schema);
    }

    public static String getOutputPartitionSchema(final Configuration configuration) {
        return configuration.get("tdch.output.hive.partition.schema", "");
    }

    public static void setOutputFieldNamesArray(final Configuration configuration, String[] fieldNames) {
        fieldNames = ConnectorSchemaUtils.unquoteFieldNamesArray(fieldNames);
        final String fieldNamesJson = ConnectorSchemaUtils.fieldNamesToJson(fieldNames);
        configuration.set("tdch.output.hive.field.names", fieldNamesJson);
    }

    public static String[] getOutputFieldNamesArray(final Configuration configuration) {
        final String fieldNamesJson = configuration.get("tdch.output.hive.field.names", "");
        return ConnectorSchemaUtils.fieldNamesFromJson(fieldNamesJson);
    }

    public static void setOutputRCFileSerde(final Configuration configuration, final String serde) {
        configuration.set("tdch.output.hive.rcfile.serde", serde);
    }

    public static String getOutputRCFileSerde(final Configuration configuration) {
        return configuration.get("tdch.output.hive.rcfile.serde", "");
    }

    public static void setOutputOverwrite(final Configuration configuration, final boolean value) {
        configuration.setBoolean("tdch.output.hive.overwrite", value);
    }

    public static boolean getOutputOverwrite(final Configuration configuration) {
        return configuration.getBoolean("tdch.output.hive.overwrite", false);
    }
}
