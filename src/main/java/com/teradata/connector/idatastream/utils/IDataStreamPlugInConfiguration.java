package com.teradata.connector.idatastream.utils;

import com.teradata.connector.common.utils.ConnectorSchemaUtils;
import org.apache.hadoop.conf.Configuration;

/**
 * @author Administrator
 */
public class IDataStreamPlugInConfiguration {
    protected static final String TDCH_OUTPUT_IDATASTREAM_SOCKET_HOST = "tdch.output.idatastream.socket.host";
    protected static final String TDCH_OUTPUT_IDATASTREAM_SOCKET_PORT = "tdch.output.idatastream.socket.port";
    protected static final String TDCH_OUTPUT_IDATASTREAM_FIELD_TYPES = "tdch.output.idatastream.schema.types";
    protected static final String TDCH_OUTPUT_IDATASTREAM_FIELD_TYPES_JSON = "tdch.output.idatastream.schema.types.json";
    protected static final String TDCH_OUTPUT_IDATASTREAM_FIELD_NAMES = "tdch.output.idatastream.schema.names";
    protected static final String TDCH_OUTPUT_IDATASTREAM_FIELD_NAMES_JSON = "tdch.output.idatastream.schema.names.json";
    protected static final String TDCH_OUTPUT_IDATASTREAM_LE_SERVER = "tdch.output.idatastream.le.server";
    protected static final String TDCH_INPUT_IDATASTREAM_SOCKET_HOST = "tdch.input.idatastream.socket.host";
    protected static final String TDCH_INPUT_IDATASTREAM_SOCKET_PORT = "tdch.input.idatastream.socket.port";
    protected static final String TDCH_INPUT_IDATASTREAM_FIELD_TYPES = "tdch.input.idatastream.schema.types";
    protected static final String TDCH_INPUT_IDATASTREAM_FIELD_TYPES_JSON = "tdch.input.idatastream.schema.types.json";
    protected static final String TDCH_INPUT_IDATASTREAM_FIELD_NAMES = "tdch.input.idatastream.schema.names";
    protected static final String TDCH_INPUT_IDATASTREAM_FIELD_NAMES_JSON = "tdch.input.idatastream.schema.names.json";
    protected static final String TDCH_INPUT_IDATASTREAM_LE_SERVER = "tdch.input.idatastream.le.server";
    public static final String VALUE_IDATASTREAM_METHOD = "idata.stream";

    public static void setOutputSocketHost(final Configuration configuration, final String iDataStreamSocketHost) {
        configuration.set("tdch.output.idatastream.socket.host", iDataStreamSocketHost);
    }

    public static String getOutputSocketHost(final Configuration configuration) {
        return configuration.get("tdch.output.idatastream.socket.host", "");
    }

    public static void setOutputSocketPort(final Configuration configuration, final String tptSocketPort) {
        configuration.set("tdch.output.idatastream.socket.port", tptSocketPort);
    }

    public static String getOutputSocketPort(final Configuration configuration) {
        return configuration.get("tdch.output.idatastream.socket.port", "");
    }

    public static void setOutputFieldNames(final Configuration configuration, final String fieldNames) {
        final String quotedFieldNames = ConnectorSchemaUtils.quoteFieldNames(fieldNames);
        configuration.set("tdch.output.idatastream.schema.names", quotedFieldNames);
        String[] fieldNamesArray = ConnectorSchemaUtils.convertFieldNamesToArray(fieldNames);
        fieldNamesArray = ConnectorSchemaUtils.unquoteFieldNamesArray(fieldNamesArray);
        final String fieldNamesJson = ConnectorSchemaUtils.fieldNamesToJson(fieldNamesArray);
        configuration.set("tdch.output.idatastream.schema.names.json", fieldNamesJson);
    }

    public static String getOutputFieldNames(final Configuration configuration) {
        final String fieldNamesJson = configuration.get("tdch.output.idatastream.schema.names", "");
        return fieldNamesJson;
    }

    public static String[] getOutputFieldNamesArray(final Configuration configuration) {
        final String fieldNamesJson = configuration.get("tdch.output.idatastream.schema.names.json", "");
        return ConnectorSchemaUtils.fieldNamesFromJson(fieldNamesJson);
    }

    public static void setOutputFieldTypes(final Configuration configuration, final String fieldTypes) {
        final String quotedFieldTypes = ConnectorSchemaUtils.quoteFieldNames(fieldTypes);
        configuration.set("tdch.output.idatastream.schema.types", quotedFieldTypes);
        String[] fieldTypesArray = ConnectorSchemaUtils.convertFieldNamesToArray(fieldTypes);
        fieldTypesArray = ConnectorSchemaUtils.unquoteFieldNamesArray(fieldTypesArray);
        final String fieldTypesJson = ConnectorSchemaUtils.fieldNamesToJson(fieldTypesArray);
        configuration.set("tdch.output.idatastream.schema.types.json", fieldTypesJson);
    }

    public static String getOutputFieldTypes(final Configuration configuration) {
        final String fieldTypesJson = configuration.get("tdch.output.idatastream.schema.types", "");
        return fieldTypesJson;
    }

    public static String[] getOutputFieldTypesArray(final Configuration configuration) {
        final String fieldTypesJson = configuration.get("tdch.output.idatastream.schema.types.json", "");
        return ConnectorSchemaUtils.fieldNamesFromJson(fieldTypesJson);
    }

    public static void setOutputLittleEndianServer(final Configuration configuration, final String tptLittleEndianServer) {
        configuration.set("tdch.output.idatastream.le.server", tptLittleEndianServer);
    }

    public static String getOutputLittleEndianServer(final Configuration configuration) {
        return configuration.get("tdch.output.idatastream.le.server", "");
    }

    public static void setInputSocketHost(final Configuration configuration, final String tptSocketHost) {
        configuration.set("tdch.input.idatastream.socket.host", tptSocketHost);
    }

    public static String getInputSocketHost(final Configuration configuration) {
        return configuration.get("tdch.input.idatastream.socket.host", "");
    }

    public static void setInputSocketPort(final Configuration configuration, final String tptSocketPort) {
        configuration.set("tdch.input.idatastream.socket.port", tptSocketPort);
    }

    public static String getInputSocketPort(final Configuration configuration) {
        return configuration.get("tdch.input.idatastream.socket.port", "");
    }

    public static void setInputFieldNames(final Configuration configuration, final String fieldNames) {
        final String quotedFieldNames = ConnectorSchemaUtils.quoteFieldNames(fieldNames);
        configuration.set("tdch.input.idatastream.schema.names", quotedFieldNames);
        String[] fieldNamesArray = ConnectorSchemaUtils.convertFieldNamesToArray(fieldNames);
        fieldNamesArray = ConnectorSchemaUtils.unquoteFieldNamesArray(fieldNamesArray);
        final String fieldNamesJson = ConnectorSchemaUtils.fieldNamesToJson(fieldNamesArray);
        configuration.set("tdch.input.idatastream.schema.names.json", fieldNamesJson);
    }

    public static String getInputFieldNames(final Configuration configuration) {
        final String fieldNamesJson = configuration.get("tdch.input.idatastream.schema.names", "");
        return fieldNamesJson;
    }

    public static String[] getInputFieldNamesArray(final Configuration configuration) {
        final String fieldNamesJson = configuration.get("tdch.input.idatastream.schema.names.json", "");
        return ConnectorSchemaUtils.fieldNamesFromJson(fieldNamesJson);
    }

    public static void setInputFieldTypes(final Configuration configuration, final String fieldTypes) {
        final String quotedFieldTypes = ConnectorSchemaUtils.quoteFieldNames(fieldTypes);
        configuration.set("tdch.input.idatastream.schema.types", quotedFieldTypes);
        String[] fieldTypesArray = ConnectorSchemaUtils.convertFieldNamesToArray(fieldTypes);
        fieldTypesArray = ConnectorSchemaUtils.unquoteFieldNamesArray(fieldTypesArray);
        final String fieldTypesJson = ConnectorSchemaUtils.fieldNamesToJson(fieldTypesArray);
        configuration.set("tdch.input.idatastream.schema.types.json", fieldTypesJson);
    }

    public static String getInputFieldTypes(final Configuration configuration) {
        final String fieldTypesJson = configuration.get("tdch.input.idatastream.schema.types", "");
        return fieldTypesJson;
    }

    public static String[] getInputFieldTypesArray(final Configuration configuration) {
        final String fieldTypesJson = configuration.get("tdch.input.idatastream.schema.types.json", "");
        return ConnectorSchemaUtils.fieldNamesFromJson(fieldTypesJson);
    }

    public static void setInputLittleEndianServer(final Configuration configuration, final String tptLittleEndianServer) {
        configuration.set("tdch.input.idatastream.le.server", tptLittleEndianServer);
    }

    public static String getInputLittleEndianServer(final Configuration configuration) {
        return configuration.get("tdch.input.idatastream.le.server", "");
    }
}
