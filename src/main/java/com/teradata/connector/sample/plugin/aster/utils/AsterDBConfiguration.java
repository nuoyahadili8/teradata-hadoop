package com.teradata.connector.sample.plugin.aster.utils;

import com.teradata.connector.common.converter.ConnectorDataTypeDefinition;
import com.teradata.connector.common.utils.ConnectorSchemaParser;
import java.util.List;
import org.apache.hadoop.conf.Configuration;

public class AsterDBConfiguration {
    public static final String ASTER_INPUT_JDBC_DRIVER_CLASS = "tdch.input.aster.jdbc.driver.class";
    public static final String ASTER_INPUT_JDBC_URL = "tdch.input.aster.jdbc.url";
    public static final String ASTER_INPUT_JDBC_USER_NAME = "tdch.input.aster.jdbc.user.name";
    public static final String ASTER_INPUT_JDBC_PASSWORD = "tdch.input.aster.jdbc.password";
    public static final String ASTER_INPUT_SPLIT_BY_COLUMN = "tdch.input.aster.split.by.column";
    public static final String ASTER_INPUT_DATABASE = "tdch.input.aster.database";
    public static final String ASTER_INPUT_SCHEMA = "tdch.input.aster.schema";
    public static final String ASTER_INPUT_TABLE = "tdch.input.aster.table";
    public static final String ASTER_OUTPUT_JDBC_DRIVER_CLASS = "tdch.output.aster.jdbc.driver.class";
    public static final String ASTER_OUTPUT_JDBC_URL = "tdch.output.aster.jdbc.url";
    public static final String ASTER_OUTPUT_JDBC_USER_NAME = "tdch.output.aster.jdbc.user.name";
    public static final String ASTER_OUTPUT_JDBC_PASSWORD = "tdch.output.aster.jdbc.password";
    public static final String ASTER_OUTPUT_TABLE = "tdch.output.aster.table";
    public static final String ASTER_OUTPUT_DATABASE = "tdch.output.aster.database";
    public static final String ASTER_OUTPUT_SCHEMA = "tdch.output.aster.schema";

    public static void setAsterInputTable(final Configuration configuration, final String inputTable) {
        ConnectorSchemaParser parser = new ConnectorSchemaParser();
        parser.setDelimChar('.');
        List<String> tokens = parser.tokenize(inputTable, 3, false);
        switch (tokens.size()) {
            case 1:
                configuration.set(ASTER_INPUT_TABLE, (String) tokens.get(0));
                return;
            case 2:
                configuration.set(ASTER_INPUT_SCHEMA, (String) tokens.get(0));
                configuration.set(ASTER_INPUT_TABLE, (String) tokens.get(1));
                return;
            case ConnectorDataTypeDefinition.TYPE_DECIMAL /*3*/:
                configuration.set(ASTER_INPUT_DATABASE, (String) tokens.get(0));
                configuration.set(ASTER_INPUT_SCHEMA, (String) tokens.get(1));
                configuration.set(ASTER_INPUT_TABLE, (String) tokens.get(2));
                return;
            default:
                return;
        }

    }

    public static String getAsterInputTable(final Configuration configuration) {
        return configuration.get("tdch.input.aster.table", (String) null);
    }

    public static String getAsterInputSchema(final Configuration configuration) {
        return configuration.get("tdch.input.aster.schema", "public");
    }

    public static String getAsterInputDatabase(final Configuration configuration) {
        return configuration.get("tdch.input.aster.database", "");
    }

    public static void setAsterInputSchema(final Configuration configuration, final String schema) {
        configuration.set("tdch.input.aster.schema", schema);
    }

    public static void setAsterInputDatabase(final Configuration configuration, final String value) {
        configuration.set("tdch.input.aster.database", value);
    }

    public static String getAsterOutputDatabase(final Configuration configuration) {
        return configuration.get("tdch.output.aster.database", "");
    }

    public static void setAsterOutputDatabase(final Configuration configuration, final String value) {
        configuration.set("tdch.output.aster.database", value);
    }

    public static String getAsterOutputSchema(final Configuration configuration) {
        return configuration.get("tdch.output.aster.schema", "public");
    }

    public static void setAsterOutputSchema(final Configuration configuration, final String value) {
        configuration.set("tdch.output.aster.schema", value);
    }

    public static String getAsterOutputTable(final Configuration configuration) {
        return configuration.get("tdch.output.aster.table", (String) null);
    }

    public static void setAsterOutputTable(final Configuration configuration, final String outputTable) {
        ConnectorSchemaParser parser = new ConnectorSchemaParser();
        parser.setDelimChar('.');
        List<String> tokens = parser.tokenize(outputTable, 3, false);
        switch (tokens.size()) {
            case 1:
                configuration.set(ASTER_OUTPUT_TABLE, (String) tokens.get(0));
                return;
            case 2:
                configuration.set(ASTER_OUTPUT_SCHEMA, (String) tokens.get(0));
                configuration.set(ASTER_OUTPUT_TABLE, (String) tokens.get(1));
                return;
            case ConnectorDataTypeDefinition.TYPE_DECIMAL /*3*/:
                configuration.set(ASTER_OUTPUT_DATABASE, (String) tokens.get(0));
                configuration.set(ASTER_OUTPUT_SCHEMA, (String) tokens.get(1));
                configuration.set(ASTER_OUTPUT_TABLE, (String) tokens.get(2));
                return;
            default:
                return;
        }

    }

    public static void setOutputJdbcDriverClass(final Configuration configuration, final String jdbcDriverClass) {
        configuration.set("tdch.output.aster.jdbc.driver.class", jdbcDriverClass);
    }

    public static String getOutputJdbcDriverClass(final Configuration configuration) {
        return configuration.get("tdch.output.aster.jdbc.driver.class", (String) null);
    }

    public static void setOutputJdbcUrl(final Configuration configuration, final String jdbcUrl) {
        configuration.set("tdch.output.aster.jdbc.url", jdbcUrl);
    }

    public static String getOutputJdbcUrl(final Configuration configuration) {
        return configuration.get("tdch.output.aster.jdbc.url", "");
    }

    public static void setOutputJdbcUserName(final Configuration configuration, final String jdbcUserName) {
        configuration.set("tdch.output.aster.jdbc.user.name", jdbcUserName);
    }

    public static String getOutputJdbcUserName(final Configuration configuration) {
        return configuration.get("tdch.output.aster.jdbc.user.name", "");
    }

    public static void setOutputJdbcPassword(final Configuration configuration, final String jdbcPassword) {
        configuration.set("tdch.output.aster.jdbc.password", jdbcPassword);
    }

    public static String getOutputJdbcPassword(final Configuration configuration) {
        return configuration.get("tdch.output.aster.jdbc.password", "");
    }

    public static void setInputSplitByColumn(final Configuration configuration, final String splitByColumn) {
        configuration.set("tdch.input.aster.split.by.column", splitByColumn);
    }

    public static String getInputSplitByColumn(final Configuration configuration) {
        return configuration.get("tdch.input.aster.split.by.column", "");
    }

    public static void setInputJdbcDriverClass(final Configuration configuration, final String jdbcDriverClass) {
        configuration.set("tdch.input.aster.jdbc.driver.class", jdbcDriverClass);
    }

    public static String getInputJdbcDriverClass(final Configuration configuration) {
        return configuration.get("tdch.input.aster.jdbc.driver.class", (String) null);
    }

    public static void setInputJdbcUrl(final Configuration configuration, final String jdbcUrl) {
        configuration.set("tdch.input.aster.jdbc.url", jdbcUrl);
    }

    public static String getInputJdbcUrl(final Configuration configuration) {
        return configuration.get("tdch.input.aster.jdbc.url", "");
    }

    public static void setInputJdbcUserName(final Configuration configuration, final String jdbcUserName) {
        configuration.set("tdch.input.aster.jdbc.user.name", jdbcUserName);
    }

    public static String getInputJdbcUserName(final Configuration configuration) {
        return configuration.get("tdch.input.aster.jdbc.user.name", "");
    }

    public static void setInputJdbcPassword(final Configuration configuration, final String jdbcPassword) {
        configuration.set("tdch.input.aster.jdbc.password", jdbcPassword);
    }

    public static String getInputJdbcPassword(final Configuration configuration) {
        return configuration.get("tdch.input.aster.jdbc.password", "");
    }
}
