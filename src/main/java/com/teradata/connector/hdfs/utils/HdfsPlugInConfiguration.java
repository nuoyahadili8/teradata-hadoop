package com.teradata.connector.hdfs.utils;

import com.teradata.connector.common.exception.ConnectorException;
import com.teradata.connector.common.utils.ConnectorUnicodeCharacterConverter;
import org.apache.hadoop.conf.Configuration;

/**
 * @author Administrator
 */
public class HdfsPlugInConfiguration {
    public static final String TDCH_INPUT_HDFS_SEPARATOR = "tdch.input.hdfs.separator";
    public static final String TDCH_INPUT_HDFS_PATHS = "tdch.input.hdfs.paths";
    public static final String TDCH_INPUT_HDFS_AVRO_SCHEMA = "tdch.input.hdfs.avro.schema";
    public static final String TDCH_INPUT_HDFS_SCHEMA = "tdch.input.hdfs.schema";
    public static final String TDCH_INPUT_HDFS_NULL_STRING = "tdch.input.hdfs.null.string";
    public static final String TDCH_INPUT_HDFS_NULL_NON_STRING = "tdch.input.hdfs.null.non.string";
    public static final String TDCH_INPUT_HDFS_ENCLOSED_BY = "tdch.input.hdfs.enclosed.by";
    public static final String TDCH_INPUT_HDFS_ESCAPED_BY = "tdch.input.hdfs.escaped.by";
    public static final String TDCH_INPUT_HDFS_AVRO_SCHEMA_FILE = "tdch.input.hdfs.avro.schema.file";
    public static final String TDCH_INPUT_HDFS_FIELD_NAMES_ARRAY = "tdch.input.hdfs.field.names";
    public static final String TDCH_OUTPUT_HDFS_SEPARATOR = "tdch.output.hdfs.separator";
    public static final String TDCH_OUTPUT_HDFS_PATHS = "tdch.output.hdfs.paths";
    public static final String TDCH_OUTPUT_HDFS_AVRO_SCHEMA = "tdch.output.hdfs.avro.schema";
    public static final String TDCH_OUTPUT_HDFS_SCHEMA = "tdch.output.hdfs.schema";
    public static final String TDCH_OUTPUT_HDFS_NULL_STRING = "tdch.output.hdfs.null.string";
    public static final String TDCH_OUTPUT_HDFS_NULL_NON_STRING = "tdch.output.hdfs.null.non.string";
    public static final String TDCH_OUTPUT_HDFS_ENCLOSE_BY = "tdch.output.hdfs.enclosed.by";
    public static final String TDCH_OUTPUT_HDFS_ESCAPED_BY = "tdch.output.hdfs.escaped.by";
    public static final String TDCH_OUTPUT_HDFS_AVRO_SCHEMA_FILE = "tdch.output.hdfs.avro.schema.file";
    public static final String TDCH_OUTPUT_HDFS_FIELD_NAMES_ARRAY = "tdch.output.hdfs.field.names";
    public static final String VALUE_HDFS_AVRO_SCHEMA_INPUT_KEY = "avro.schema.input.key";
    public static final String VALUE_HDFS_AVRO_SCHEMA_OUTPUT_KEY = "avro.schema.output.key";
    public static final String VALUE_FIELDS_SEPARATOR = "\t";
    public static final boolean VALUE_HDFS_CUSTOM_PARSE_OFF = false;

    public static String getInputPaths(final Configuration configuration) {
        return configuration.get("tdch.input.hdfs.paths", "");
    }

    public static void setInputPaths(final Configuration configuration, final String inputPath) {
        configuration.set("tdch.input.hdfs.paths", inputPath);
        configuration.set("mapred.input.dir", inputPath);
    }

    public static String[] getInputFieldNamesArray(final Configuration configuration) {
        return configuration.getStrings("tdch.input.hdfs.field.names", new String[0]);
    }

    public static void setInputFieldNamesArray(final Configuration configuration, final String[] inputFieldNamesArray) {
        configuration.setStrings("tdch.input.hdfs.field.names", inputFieldNamesArray);
    }

    public static String getInputSchema(final Configuration configuration) {
        return configuration.get("tdch.input.hdfs.schema", "");
    }

    public static void setInputSchema(final Configuration configuration, final String inputSchema) {
        configuration.set("tdch.input.hdfs.schema", inputSchema);
    }

    public static String getInputAvroSchema(final Configuration configuration) {
        return configuration.get("tdch.input.hdfs.avro.schema", "");
    }

    public static void setInputAvroSchema(final Configuration configuration, final String inputAvroSchema) {
        configuration.set("tdch.input.hdfs.avro.schema", inputAvroSchema);
    }

    public static String getInputAvroSchemaFile(final Configuration configuration) {
        return configuration.get("tdch.input.hdfs.avro.schema.file", "");
    }

    public static void setInputAvroSchemaFile(final Configuration configuration, final String inputAvroSchemaFile) {
        configuration.set("tdch.input.hdfs.avro.schema.file", inputAvroSchemaFile);
    }

    public static String getInputEscapedBy(final Configuration configuration) throws ConnectorException {
        String escapedByString = configuration.get("tdch.input.hdfs.escaped.by");
        escapedByString = ConnectorUnicodeCharacterConverter.fromEncodedUnicode(escapedByString);
        return (escapedByString == null) ? "" : escapedByString.substring(0, 1);
    }

    public static void setInputEscapedBy(final Configuration configuration, final String inputEscapedBy) throws ConnectorException {
        configuration.set("tdch.input.hdfs.escaped.by", inputEscapedBy);
        if (inputEscapedBy != null && inputEscapedBy.length() > 0) {
            ConnectorUnicodeCharacterConverter.fromEncodedUnicode(inputEscapedBy);
        }
    }

    public static String getInputEnclosedBy(final Configuration configuration) throws ConnectorException {
        String enclosedBy = configuration.get("tdch.input.hdfs.enclosed.by");
        enclosedBy = ConnectorUnicodeCharacterConverter.fromEncodedUnicode(enclosedBy);
        return (enclosedBy == null) ? "" : enclosedBy.substring(0, 1);
    }

    public static void setInputEnclosedBy(final Configuration configuration, final String inputEnclosedBy) throws ConnectorException {
        configuration.set("tdch.input.hdfs.enclosed.by", inputEnclosedBy);
        if (inputEnclosedBy != null && inputEnclosedBy.length() > 0) {
            ConnectorUnicodeCharacterConverter.fromEncodedUnicode(inputEnclosedBy);
        }
    }

    public static String getInputNullNonString(final Configuration configuration) {
        return configuration.get("tdch.input.hdfs.null.non.string", "");
    }

    public static void setInputNullNonString(final Configuration configuration, final String inputNullNonString) {
        configuration.set("tdch.input.hdfs.null.non.string", inputNullNonString);
    }

    public static String getInputNullString(final Configuration configuration) {
        return configuration.get("tdch.input.hdfs.null.string", "");
    }

    public static void setInputNullString(final Configuration configuration, final String inputNullString) {
        configuration.set("tdch.input.hdfs.null.string", inputNullString);
    }

    public static String getInputSeparator(final Configuration configuration) throws ConnectorException {
        String separator = configuration.get("tdch.input.hdfs.separator", "\t");
        if (separator != null && separator.length() > 0) {
            separator = ConnectorUnicodeCharacterConverter.fromEncodedUnicode(separator);
            separator = separator.substring(0, 1);
        }
        return separator;
    }

    public static void setInputSeparator(final Configuration configuration, final String inputSeparator) throws ConnectorException {
        configuration.set("tdch.input.hdfs.separator", inputSeparator);
        if (inputSeparator != null && inputSeparator.length() > 0) {
            ConnectorUnicodeCharacterConverter.fromEncodedUnicode(inputSeparator);
        }
    }

    public static String getOutputPaths(final Configuration configuration) {
        return configuration.get("tdch.output.hdfs.paths", "");
    }

    public static void setOutputPaths(final Configuration configuration, final String outPath) {
        configuration.set("tdch.output.hdfs.paths", outPath);
        configuration.set("mapred.output.dir", outPath);
    }

    public static String[] getOutputFieldNamesArray(final Configuration configuration) {
        return configuration.getStrings("tdch.output.hdfs.field.names", new String[0]);
    }

    public static void setOutputFieldNamesArray(final Configuration configuration, final String[] outputFieldNamesArray) {
        configuration.setStrings("tdch.output.hdfs.field.names", outputFieldNamesArray);
    }

    public static String getOutputSchema(final Configuration configuration) {
        return configuration.get("tdch.output.hdfs.schema", "");
    }

    public static void setOutputSchema(final Configuration configuration, final String outputSchema) {
        configuration.set("tdch.output.hdfs.schema", outputSchema);
    }

    public static String getOutputAvroSchema(final Configuration configuration) {
        return configuration.get("tdch.output.hdfs.avro.schema", "");
    }

    public static void setOutputAvroSchema(final Configuration configuration, final String outputAvroSchema) {
        configuration.set("tdch.output.hdfs.avro.schema", outputAvroSchema);
    }

    public static String getOutputAvroSchemaFile(final Configuration configuration) {
        return configuration.get("tdch.output.hdfs.avro.schema.file", "");
    }

    public static void setOutputAvroSchemaFile(final Configuration configuration, final String outputAvroSchemaFile) {
        configuration.set("tdch.output.hdfs.avro.schema.file", outputAvroSchemaFile);
    }

    public static String getOutputEscapedBy(final Configuration configuration) throws ConnectorException {
        String escapedByString = configuration.get("tdch.output.hdfs.escaped.by");
        escapedByString = ConnectorUnicodeCharacterConverter.fromEncodedUnicode(escapedByString);
        return (escapedByString == null) ? "" : escapedByString.substring(0, 1);
    }

    public static void setOutputEscapedBy(final Configuration configuration, final String outputEscapedBy) throws ConnectorException {
        configuration.set("tdch.output.hdfs.escaped.by", outputEscapedBy);
        if (outputEscapedBy != null && outputEscapedBy.length() > 0) {
            ConnectorUnicodeCharacterConverter.fromEncodedUnicode(outputEscapedBy);
        }
    }

    public static String getOutputEnclosedBy(final Configuration configuration) throws ConnectorException {
        String enclosedBy = configuration.get("tdch.output.hdfs.enclosed.by");
        enclosedBy = ConnectorUnicodeCharacterConverter.fromEncodedUnicode(enclosedBy);
        return (enclosedBy == null) ? "" : enclosedBy.substring(0, 1);
    }

    public static void setOutputEnclosedBy(final Configuration configuration, final String outputEnclosedBy) throws ConnectorException {
        configuration.set("tdch.output.hdfs.enclosed.by", outputEnclosedBy);
        if (outputEnclosedBy != null && outputEnclosedBy.length() > 0) {
            ConnectorUnicodeCharacterConverter.fromEncodedUnicode(outputEnclosedBy);
        }
    }

    public static String getOutputNullNonString(final Configuration configuration) {
        return configuration.get("tdch.output.hdfs.null.non.string", "");
    }

    public static void setOutputNullNonString(final Configuration configuration, final String outputNullNonString) {
        configuration.set("tdch.output.hdfs.null.non.string", outputNullNonString);
    }

    public static String getOutputNullString(final Configuration configuration) {
        return configuration.get("tdch.output.hdfs.null.string", "");
    }

    public static void setOutputNullString(final Configuration configuration, final String outputNullString) {
        configuration.set("tdch.output.hdfs.null.string", outputNullString);
    }

    public static String getOutputSeparator(final Configuration configuration) throws ConnectorException {
        String separator = configuration.get("tdch.output.hdfs.separator", "\t");
        if (separator != null && separator.length() > 0) {
            separator = ConnectorUnicodeCharacterConverter.fromEncodedUnicode(separator);
            separator = separator.substring(0, 1);
        }
        return separator;
    }

    public static void setOutputSeparator(final Configuration configuration, final String outputSeparator) throws ConnectorException {
        configuration.set("tdch.output.hdfs.separator", outputSeparator);
        if (outputSeparator != null && outputSeparator.length() > 0) {
            ConnectorUnicodeCharacterConverter.fromEncodedUnicode(outputSeparator);
        }
    }
}
