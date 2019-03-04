package com.teradata.hadoop.db;

import com.teradata.connector.common.exception.ConnectorException;
import com.teradata.connector.common.exception.ConnectorException.ErrorCode;
import com.teradata.connector.common.utils.ConnectorSchemaParser;
import com.teradata.connector.common.utils.ConnectorSchemaUtils;
import com.teradata.connector.hive.utils.HiveUtils;
import com.teradata.connector.teradata.utils.TeradataSchemaUtils;
import java.util.List;
import org.apache.hadoop.conf.Configuration;

/**
 * @author Administrator
 */
public final class TeradataConfiguration {
    public static final String PREFIX_INPUT_STAGE_VIEW = "V";
    public static final String PROPERTY_CUSTOM_PARSE = "teradata.db.custom_parse";
    public static final String PROPERTY_HIVE_CONF_FILE = "teradata.db.hive.configuration.file";
    public static final String PROPERTY_INPUT_ACCESS_LOCK = "teradata.db.input.access.lock";
    public static final String PROPERTY_INPUT_BATCH_SIZE = "teradata.db.input.batch.size";
    public static final String PROPERTY_INPUT_ENCLOSED_BY = "teradata.db.input.enclosed.by";
    public static final String PROPERTY_INPUT_ESCAPED_BY = "teradata.db.input.escaped.by";
    public static final String PROPERTY_INPUT_FASTEXPORT_SOCKET_HOST = "teradata.db.input.fastexport.socket.host";
    public static final String PROPERTY_INPUT_FASTEXPORT_SOCKET_PORT = "teradata.db.input.fastexport.socket.port";
    public static final String PROPERTY_INPUT_FASTEXPORT_SOCKET_TIMEOUT = "teradata.db.input.fastexport.socket.timeout";
    public static final String PROPERTY_INPUT_FIELDS_SEPARATOR = "teradata.db.input.fields.separator";
    public static final String PROPERTY_INPUT_FILE_FORMAT = "teradata.db.input.file.format";
    protected static final String PROPERTY_INPUT_JOB_FIELD_TYPES = "teradata.db.input.job.field.types";
    protected static final String PROPERTY_INPUT_JOB_NUMBER_AMPS = "teradata.db.input.job.num.amps";
    protected static final String PROPERTY_INPUT_JOB_NUMBER_PARTITIONS_IN_STAGING_PROVIDED = "teradata.db.input.job.num.partitions.in.staging.provided";
    protected static final String PROPERTY_INPUT_JOB_SOURCE_FIELDS_DESC = "teradata.db.input.job.source.fields.desc";
    protected static final String PROPERTY_INPUT_JOB_SOURCE_TABLE_DESC = "teradata.db.input.job.source.table.desc";
    protected static final String PROPERTY_INPUT_JOB_SPLIT_SQL = "teradata.db.input.job.split.sql";
    protected static final String PROPERTY_INPUT_JOB_STAGE_ENABLED = "teradata.db.input.job.stage.enabled";
    protected static final String PROPERTY_INPUT_JOB_STAGING_AREAS = "teradata.db.input.job.staging.areas";
    protected static final String PROPERTY_INPUT_JOB_TARGET_FIELDS_DESC = "teradata.db.input.job.target.fields.desc";
    protected static final String PROPERTY_INPUT_JOB_TARGET_TABLE_DESC = "teradata.db.input.job.target.table.desc";
    public static final String PROPERTY_INPUT_JOB_TYPE = "teradata.db.input.job.type";
    public static final String PROPERTY_INPUT_LINE_SEPARATOR = "teradata.db.input.line.separator";
    public static final String PROPERTY_INPUT_METHOD = "teradata.db.input.method";
    public static final String PROPERTY_INPUT_NUMBER_MAPPERS = "teradata.db.input.num.mappers";
    public static final String PROPERTY_INPUT_NUMBER_PARTITIONS_IN_STAGING = "teradata.db.input.num.partitions.in.staging";
    public static final String PROPERTY_INPUT_QUERY_BAND = "teradata.db.input.query.band";
    public static final String PROPERTY_INPUT_SOURCE_CONDITIONS = "teradata.db.input.source.conditions";
    public static final String PROPERTY_INPUT_SOURCE_COUNT_QUERY = "teradata.db.input.source.count.query";
    public static final String PROPERTY_INPUT_SOURCE_DATABASE = "teradata.db.input.source.database";
    public static final String PROPERTY_INPUT_SOURCE_FIELD_NAMES = "teradata.db.input.source.field.names";
    public static final String PROPERTY_INPUT_SOURCE_FIELD_NAMES_JSON = "teradata.db.input.source.field.names.json";
    public static final String PROPERTY_INPUT_SOURCE_QUERY = "teradata.db.input.source.query";
    public static final String PROPERTY_INPUT_SOURCE_TABLE = "teradata.db.input.source.table";
    public static final String PROPERTY_INPUT_SPLIT_BY_COLUMN = "teradata.db.input.split.by.column";
    public static final String PROPERTY_INPUT_STAGE_DATABASE = "teradata.db.input.stage.database";
    public static final String PROPERTY_INPUT_STAGE_DATABASE_FOR_TABLE = "teradata.db.input.stage.database.for.table";
    public static final String PROPERTY_INPUT_STAGE_DATABASE_FOR_VIEW = "teradata.db.input.stage.database.for.view";
    public static final String PROPERTY_INPUT_STAGE_FORCED = "teradata.db.input.stage.forced";
    public static final String PROPERTY_INPUT_STAGE_TABLE_DROP_ENABLED = "teradata.db.input.stage.table.drop.enabled";
    public static final String PROPERTY_INPUT_STAGE_TABLE_NAME = "teradata.db.input.stage.table.name";
    public static final String PROPERTY_INPUT_TARGET_DATABASE = "teradata.db.input.target.database";
    public static final String PROPERTY_INPUT_TARGET_FIELD_NAMES = "teradata.db.input.target.field.names";
    public static final String PROPERTY_INPUT_TARGET_FIELD_NAMES_JSON = "teradata.db.input.target.field.names.json";
    public static final String PROPERTY_INPUT_TARGET_PARTITION_SCHEMA = "teradata.db.input.target.partition.schema";
    public static final String PROPERTY_INPUT_TARGET_PATHS = "teradata.db.input.target.paths";
    public static final String PROPERTY_INPUT_TARGET_TABLE = "teradata.db.input.target.table";
    public static final String PROPERTY_INPUT_TARGET_TABLE_SCHEMA = "teradata.db.input.target.table.schema";
    public static final String PROPERTY_JDBC_DRIVER_CLASS = "mapreduce.jdbc.driver.class";
    public static final String PROPERTY_JDBC_PASSWORD = "mapreduce.jdbc.password";
    public static final String PROPERTY_JDBC_URL = "mapreduce.jdbc.url";
    public static final String PROPERTY_JDBC_USERNAME = "mapreduce.jdbc.username";
    public static final String PROPERTY_JOB_AVRO_SCHEMA_FILE = "teradata.db.avro.schema.file";
    protected static final String PROPERTY_JOB_DATA_DICTIONARY_USE_XVIEWS = "teradata.db.job.data.dictionary.usexviews";
    public static final String PROPERTY_NULL_NON_STRING = "teradata.db.null.non.string";
    public static final String PROPERTY_NULL_STRING = "teradata.db.null.string";
    public static final String PROPERTY_OUTPUT_BATCH_SIZE = "teradata.db.output.batch.size";
    public static final String PROPERTY_OUTPUT_ENCLOSED_BY = "teradata.db.output.enclosed.by";
    public static final String PROPERTY_OUTPUT_ERROR_TABLE_NAME = "teradata.db.output.error.table.name";
    public static final String PROPERTY_OUTPUT_ESCAPED_BY = "teradata.db.output.escaped.by";
    public static final String PROPERTY_OUTPUT_FASTLOAD_SOCKET_HOST = "teradata.db.output.fastload.socket.host";
    public static final String PROPERTY_OUTPUT_FASTLOAD_SOCKET_PORT = "teradata.db.output.fastload.socket.port";
    public static final String PROPERTY_OUTPUT_FASTLOAD_SOCKET_TIMEOUT = "teradata.db.output.fastload.socket.timeout";
    public static final String PROPERTY_OUTPUT_FIELDS_SEPARATOR = "teradata.db.output.fields.separator";
    public static final String PROPERTY_OUTPUT_FILE_FORMAT = "teradata.db.output.file.format";
    protected static final String PROPERTY_OUTPUT_JOB_ERROR_TABLE1 = "teradata.db.output.job.error.table1";
    protected static final String PROPERTY_OUTPUT_JOB_ERROR_TABLE2 = "teradata.db.output.job.error.table2";
    protected static final String PROPERTY_OUTPUT_JOB_FASTLOAD_LSN = "teradata.db.output.job.fastload.lsn";
    protected static final String PROPERTY_OUTPUT_JOB_FIELD_TYPES = "teradata.db.output.job.field.types";
    protected static final String PROPERTY_OUTPUT_JOB_NUMBER_AMPS = "teradata.db.output.job.num.amps";
    protected static final String PROPERTY_OUTPUT_JOB_SOURCE_FIELDS_DESC = "teradata.db.output.job.source.fields.desc";
    protected static final String PROPERTY_OUTPUT_JOB_SOURCE_TABLE_DESC = "teradata.db.output.job.source.table.desc";
    protected static final String PROPERTY_OUTPUT_JOB_STAGE_ENABLED = "teradata.db.output.job.stage.enabled";
    protected static final String PROPERTY_OUTPUT_JOB_STAGING_AREAS = "teradata.db.output.job.staging.areas";
    protected static final String PROPERTY_OUTPUT_JOB_TARGET_FIELDS_DESC = "teradata.db.output.job.target.fields.desc";
    protected static final String PROPERTY_OUTPUT_JOB_TARGET_TABLE_DESC = "teradata.db.output.job.target.table.desc";
    public static final String PROPERTY_OUTPUT_JOB_TYPE = "teradata.db.output.job.type";
    public static final String PROPERTY_OUTPUT_LINE_SEPARATOR = "teradata.db.output.line.separator";
    public static final String PROPERTY_OUTPUT_METHOD = "teradata.db.output.method";
    public static final String PROPERTY_OUTPUT_NUMBER_MAPPERS = "teradata.db.output.num.mappers";
    public static final String PROPERTY_OUTPUT_NUMBER_REDUCERS = "teradata.db.output.num.reducers";
    public static final String PROPERTY_OUTPUT_QUERY_BAND = "teradata.db.output.query.band";
    public static final String PROPERTY_OUTPUT_SOURCE_DATABASE = "teradata.db.output.source.database";
    public static final String PROPERTY_OUTPUT_SOURCE_FIELD_NAMES = "teradata.db.output.source.field.names";
    public static final String PROPERTY_OUTPUT_SOURCE_FIELD_NAMES_JSON = "teradata.db.output.source.field.names.json";
    public static final String PROPERTY_OUTPUT_SOURCE_PARTITION_SCHEMA = "teradata.db.output.source.partition.schema";
    public static final String PROPERTY_OUTPUT_SOURCE_PATHS = "teradata.db.output.source.paths";
    public static final String PROPERTY_OUTPUT_SOURCE_TABLE = "teradata.db.output.source.table";
    public static final String PROPERTY_OUTPUT_SOURCE_TABLE_SCHEMA = "teradata.db.output.source.table.schema";
    public static final String PROPERTY_OUTPUT_STAGE_DATABASE = "teradata.db.output.stage.database";
    public static final String PROPERTY_OUTPUT_STAGE_DATABASE_FOR_TABLE = "teradata.db.output.stage.database.for.table";
    public static final String PROPERTY_OUTPUT_STAGE_DATABASE_FOR_VIEW = "teradata.db.output.stage.database.for.view";
    public static final String PROPERTY_OUTPUT_STAGE_FORCED = "teradata.db.output.stage.forced";
    public static final String PROPERTY_OUTPUT_STAGE_TABLE_KEPT = "teradata.db.output.stage.table.kept";
    public static final String PROPERTY_OUTPUT_STAGE_TABLE_NAME = "teradata.db.output.stage.table.name";
    public static final String PROPERTY_OUTPUT_TARGET_DATABASE = "teradata.db.output.target.database";
    public static final String PROPERTY_OUTPUT_TARGET_FIELD_COUNT = "teradata.db.output.target.field.count";
    public static final String PROPERTY_OUTPUT_TARGET_FIELD_NAMES = "teradata.db.output.target.field.names";
    public static final String PROPERTY_OUTPUT_TARGET_FIELD_NAMES_JSON = "teradata.db.output.target.field.names.json";
    public static final String PROPERTY_OUTPUT_TARGET_TABLE = "teradata.db.output.target.table";
    public static final boolean VALUE_CUSTOM_PARSE_OFF = false;
    public static final String VALUE_FIELDS_SEPARATOR = "\t";
    public static final String VALUE_FILE_FORMAT_AVROFILE = "avrofile";
    public static final String VALUE_FILE_FORMAT_ORCFILE = "orcfile";
    public static final String VALUE_FILE_FORMAT_RCFILE = "rcfile";
    public static final String VALUE_FILE_FORMAT_SEQUENCEFILE = "sequencefile";
    public static final String VALUE_FILE_FORMAT_TEXTFILE = "textfile";
    protected static final String VALUE_INPUT_FASTEXPORT_SOCKET_HOST_DEFAULT = "localhost";
    protected static final int VALUE_INPUT_FASTEXPORT_SOCKET_PORT_DEFAULT = 0;
    protected static final int VALUE_INPUT_FASTEXPORT_SOCKET_TIMEOUT_DEFAULT = 480000;
    public static final String VALUE_INPUT_METHOD_SPLIT_BY_AMP = "split.by.amp";
    public static final String VALUE_INPUT_METHOD_SPLIT_BY_HASH = "split.by.hash";
    public static final String VALUE_INPUT_METHOD_SPLIT_BY_PARTITION = "split.by.partition";
    public static final String VALUE_INPUT_METHOD_SPLIT_BY_VALUE = "split.by.value";
    protected static final int VALUE_INPUT_NUMBER_AMPS_DEFAULT = 2;
    protected static final int VALUE_INPUT_NUMBER_MAPPERS_DEFAULT = 2;
    protected static final int VALUE_INPUT_NUMBER_PARTITIONS_IN_STAGING_DEFAULT = 0;
    protected static final boolean VALUE_INPUT_NUMBER_PARTITIONS_IN_STAGING_PROVIDED = false;
    public static final String VALUE_JDBC_DRIVER_CLASS_DEFAULT = "com.teradata.jdbc.TeraDriver";
    protected static final int VALUE_JDBC_INPUT_BATCH_SIZE_DEFAULT = 10000;
    protected static final int VALUE_JDBC_OUTPUT_BATCH_SIZE_DEFAULT = 10000;
    public static final boolean VALUE_JOB_DATA_DICTIONARY_USE_XVIEWS = true;
    public static final String VALUE_JOB_TYPE_HCAT = "hcat";
    public static final String VALUE_JOB_TYPE_HDFS = "hdfs";
    public static final String VALUE_JOB_TYPE_HIVE = "hive";
    public static final String VALUE_LINE_SEPARATOR = "\n";
    protected static final String VALUE_OUTPUT_FASTLOAD_SOCKET_HOST_DEFAULT = "localhost";
    protected static final int VALUE_OUTPUT_FASTLOAD_SOCKET_PORT_DEFAULT = 0;
    protected static final int VALUE_OUTPUT_FASTLOAD_SOCKET_TIMEOUT_DEFAULT = 480000;
    public static final String VALUE_OUTPUT_METHOD_BATCH_INSERT = "batch.insert";
    public static final String VALUE_OUTPUT_METHOD_INTERNAL_FASTLOAD = "internal.fastload";
    public static final String VALUE_OUTPUT_METHOD_MULTIPLE_FASTLOAD = "multiple.fastload";
    protected static final int VALUE_OUTPUT_NUMBER_AMPS_DEFAULT = 2;
    protected static final int VALUE_OUTPUT_NUMBER_MAPPERS_DEFAULT = 2;
    protected static final int VALUE_OUTPUT_NUMBER_REDUCERS_DEFAULT = 0;
    public static final Boolean VALUE_STAGE_TABLE_KEPT = Boolean.valueOf(false);
    public static final int VALUE_STAGE_TABLE_NAME_MAX_LENGTH = 20;
    public static final int VALUE_STAGE_TABLE_NAME_MAX_LENGTH_EON = 118;
    public static final int VALUE_TABLE_NAME_MAX_LENGTH = 30;
    public static final int VALUE_TABLE_NAME_MAX_LENGTH_EON = 128;

    public static void setJDBCDriverClass(Configuration configuration, String driverClass) {
        configuration.set(PROPERTY_JDBC_DRIVER_CLASS, driverClass);
    }

    public static String getJDBCDriverClass(Configuration configuration) {
        return configuration.get(PROPERTY_JDBC_DRIVER_CLASS, "com.teradata.jdbc.TeraDriver");
    }

    public static void setJDBCURL(Configuration configuration, String url) {
        configuration.set(PROPERTY_JDBC_URL, url);
    }

    public static String getJDBCURL(Configuration configuration) {
        return configuration.get(PROPERTY_JDBC_URL, "");
    }

    public static void setJDBCUsername(Configuration configuration, String username) {
        configuration.set(PROPERTY_JDBC_USERNAME, username);
    }

    public static String getJDBCUsername(Configuration configuration) {
        return configuration.get(PROPERTY_JDBC_USERNAME, "");
    }

    public static void setJDBCPassword(Configuration configuration, String password) {
        configuration.set(PROPERTY_JDBC_PASSWORD, password);
    }

    public static String getJDBCPassword(Configuration configuration) {
        return configuration.get(PROPERTY_JDBC_PASSWORD, "");
    }

    public static void setHiveConfigureFile(Configuration configuration, String filePath) {
        if (filePath != null && filePath.trim().length() != 0) {
            configuration.set(PROPERTY_HIVE_CONF_FILE, filePath.trim());
        }
    }

    public static String getHiveConfigureFile(Configuration configuration) {
        return configuration.get(PROPERTY_HIVE_CONF_FILE, "");
    }

    public static void setNullString(Configuration configuration, String nullString) {
        configuration.set(PROPERTY_NULL_STRING, nullString.trim());
    }

    public static String getNullString(Configuration configuration) {
        return configuration.get(PROPERTY_NULL_STRING);
    }

    public static void setNullNonString(Configuration configuration, String nullNonString) {
        configuration.set(PROPERTY_NULL_NON_STRING, nullNonString.trim());
    }

    public static String getNullNonString(Configuration configuration) {
        return configuration.get(PROPERTY_NULL_NON_STRING);
    }

    public static void setInputSeparator(Configuration configuration, String separator) throws ConnectorException {
        configuration.set(PROPERTY_INPUT_FIELDS_SEPARATOR, separator);
    }

    public static String getInputSeparator(Configuration configuration) throws ConnectorException {
        return configuration.get(PROPERTY_INPUT_FIELDS_SEPARATOR, "\t");
    }

    public static void setInputLineSeparator(Configuration configuration, String separator) throws ConnectorException {
        configuration.set(PROPERTY_INPUT_LINE_SEPARATOR, separator);
    }

    public static String getInputLineSeparator(Configuration configuration) throws ConnectorException {
        return configuration.get(PROPERTY_INPUT_LINE_SEPARATOR, "\n");
    }

    public static void setInputEnclosedByString(Configuration configuration, String enclosedByString) throws ConnectorException {
        configuration.set(PROPERTY_INPUT_ENCLOSED_BY, enclosedByString);
    }

    public static String getInputEnclosedByString(Configuration configuration) throws ConnectorException {
        return configuration.get(PROPERTY_INPUT_ENCLOSED_BY);
    }

    public static void setInputEscapedByString(Configuration configuration, String escapedByString) throws ConnectorException {
        configuration.set(PROPERTY_INPUT_ESCAPED_BY, escapedByString);
    }

    public static String getInputEscapedByString(Configuration configuration) throws ConnectorException {
        return configuration.get(PROPERTY_INPUT_ESCAPED_BY);
    }

    public static void setInputJobType(Configuration configuration, String type) {
        configuration.set(PROPERTY_INPUT_JOB_TYPE, type);
    }

    public static String getInputJobType(Configuration configuration) {
        return configuration.get(PROPERTY_INPUT_JOB_TYPE, "hdfs");
    }

    public static void setInputFileFormat(Configuration configuration, String format) {
        configuration.set(PROPERTY_INPUT_FILE_FORMAT, format);
    }

    public static String getInputFileFormat(Configuration configuration) {
        return configuration.get(PROPERTY_INPUT_FILE_FORMAT, "textfile");
    }

    public static void setInputMethod(Configuration configuration, String inputMethod) {
        if (inputMethod != null && inputMethod.trim().length() > 0) {
            configuration.set(PROPERTY_INPUT_METHOD, inputMethod);
        }
    }

    public static String getInputMethod(Configuration configuration) {
        return configuration.get(PROPERTY_INPUT_METHOD, "split.by.hash");
    }

    public static String getInputSourceDatabase(Configuration configuration) {
        return configuration.get(PROPERTY_INPUT_SOURCE_DATABASE, "");
    }

    public static void setInputSourceTable(Configuration configuration, String table) {
        ConnectorSchemaParser parser = new ConnectorSchemaParser();
        parser.setDelimChar('.');
        List<String> tokens = parser.tokenize(table, 2, false);
        switch (tokens.size()) {
            case 1:
                configuration.set(PROPERTY_INPUT_SOURCE_TABLE, (String) tokens.get(0));
                return;
            case 2:
                configuration.set(PROPERTY_INPUT_SOURCE_DATABASE, (String) tokens.get(0));
                configuration.set(PROPERTY_INPUT_SOURCE_TABLE, (String) tokens.get(1));
                return;
            default:
                return;
        }
    }

    public static String getInputSourceTable(Configuration configuration) {
        return configuration.get(PROPERTY_INPUT_SOURCE_TABLE, "");
    }

    public static void setInputSourceQuery(Configuration configuration, String query) {
        configuration.set(PROPERTY_INPUT_SOURCE_QUERY, query);
    }

    public static String getInputSourceQuery(Configuration configuration) {
        return configuration.get(PROPERTY_INPUT_SOURCE_QUERY, "");
    }

    public static void setInputSourceConditions(Configuration configuration, String conditions) {
        configuration.set(PROPERTY_INPUT_SOURCE_CONDITIONS, conditions);
    }

    public static String getInputSourceConditions(Configuration configuration) {
        return configuration.get(PROPERTY_INPUT_SOURCE_CONDITIONS, "");
    }

    public static void setInputSourceOrderBy(Configuration configuration, String orderBy) {
    }

    public static String getInputSourceOrderBy(Configuration configuration) {
        return "";
    }

    public static void setInputSourceCountQuery(Configuration configuration, String countQuery) {
        configuration.set(PROPERTY_INPUT_SOURCE_COUNT_QUERY, countQuery);
    }

    public static String getInputSourceCountQuery(Configuration configuration) {
        return configuration.get(PROPERTY_INPUT_SOURCE_COUNT_QUERY, "");
    }

    public static void setInputSourceFieldNames(Configuration configuration, String fieldNames) {
        configuration.set(PROPERTY_INPUT_SOURCE_FIELD_NAMES, fieldNames);
        configuration.set(PROPERTY_INPUT_SOURCE_FIELD_NAMES_JSON, ConnectorSchemaUtils.fieldNamesToJson(ConnectorSchemaUtils.unquoteFieldNamesArray(ConnectorSchemaUtils.convertFieldNamesToArray(fieldNames))));
    }

    public static String getInputSourceFieldNames(Configuration configuration) {
        return configuration.get(PROPERTY_INPUT_SOURCE_FIELD_NAMES, "");
    }

    public static String[] getInputSourceFieldNamesArray(Configuration configuration) {
        return ConnectorSchemaUtils.fieldNamesFromJson(configuration.get(PROPERTY_INPUT_SOURCE_FIELD_NAMES_JSON, ""));
    }

    public static void setInputSourceFieldNamesArray(Configuration configuration, String[] fieldNamesArray) {
        configuration.set(PROPERTY_INPUT_SOURCE_FIELD_NAMES, ConnectorSchemaUtils.concatFieldNamesArray(ConnectorSchemaUtils.quoteFieldNamesArray(fieldNamesArray)));
        configuration.set(PROPERTY_INPUT_SOURCE_FIELD_NAMES_JSON, ConnectorSchemaUtils.fieldNamesToJson(fieldNamesArray));
    }

    public static void setInputTargetDatabase(Configuration configuration, String database) {
        configuration.set(PROPERTY_INPUT_TARGET_DATABASE, database);
    }

    public static String getInputTargetDatabase(Configuration configuration) {
        return configuration.get(PROPERTY_INPUT_TARGET_DATABASE, HiveUtils.DEFAULT_DATABASE);
    }

    public static void setInputTargetTable(Configuration configuration, String table) {
        ConnectorSchemaParser parser = new ConnectorSchemaParser();
        parser.setDelimChar('.');
        List<String> tokens = parser.tokenize(table, 2, false);
        switch (tokens.size()) {
            case 1:
                configuration.set(PROPERTY_INPUT_TARGET_TABLE, (String) tokens.get(0));
                return;
            case 2:
                configuration.set(PROPERTY_INPUT_TARGET_DATABASE, (String) tokens.get(0));
                configuration.set(PROPERTY_INPUT_TARGET_TABLE, (String) tokens.get(1));
                return;
            default:
                return;
        }
    }

    public static String getInputTargetTable(Configuration configuration) {
        return configuration.get(PROPERTY_INPUT_TARGET_TABLE, "");
    }

    public static void setInputTargetPaths(Configuration configuration, String paths) {
        configuration.set(PROPERTY_INPUT_TARGET_PATHS, paths);
    }

    public static String getInputTargetPaths(Configuration configuration) {
        return configuration.get(PROPERTY_INPUT_TARGET_PATHS, "");
    }

    public static void setInputTargetFieldNames(Configuration configuration, String fieldNames) {
        configuration.set(PROPERTY_INPUT_TARGET_FIELD_NAMES, fieldNames);
        configuration.set(PROPERTY_INPUT_TARGET_FIELD_NAMES_JSON, ConnectorSchemaUtils.fieldNamesToJson(ConnectorSchemaUtils.unquoteFieldNamesArray(ConnectorSchemaUtils.convertFieldNamesToArray(fieldNames))));
    }

    public static String getInputTargetFieldNames(Configuration configuration) {
        return configuration.get(PROPERTY_INPUT_TARGET_FIELD_NAMES, "");
    }

    public static void setInputTargetFieldNamesArray(Configuration configuration, String[] fieldNamesArray) {
        configuration.set(PROPERTY_INPUT_TARGET_FIELD_NAMES, ConnectorSchemaUtils.concatFieldNamesArray(ConnectorSchemaUtils.quoteFieldNamesArray(fieldNamesArray)));
        configuration.set(PROPERTY_INPUT_TARGET_FIELD_NAMES_JSON, ConnectorSchemaUtils.fieldNamesToJson(fieldNamesArray));
    }

    public static String[] getInputTargetFieldNamesArray(Configuration configuration) {
        return ConnectorSchemaUtils.fieldNamesFromJson(configuration.get(PROPERTY_INPUT_TARGET_FIELD_NAMES_JSON, ""));
    }

    public static void setInputTargetTableSchema(Configuration configuration, String schema) {
        configuration.set(PROPERTY_INPUT_TARGET_TABLE_SCHEMA, schema);
    }

    public static String getInputTargetTableSchema(Configuration configuration) {
        return configuration.get(PROPERTY_INPUT_TARGET_TABLE_SCHEMA, "");
    }

    public static void setInputTargetPartitionSchema(Configuration configuration, String schema) {
        configuration.set(PROPERTY_INPUT_TARGET_PARTITION_SCHEMA, schema);
    }

    public static String getInputTargetPartitionSchema(Configuration configuration) {
        return configuration.get(PROPERTY_INPUT_TARGET_PARTITION_SCHEMA, "");
    }

    public static String[] getInputTargetPartitionColumnNames(Configuration configuration) throws ConnectorException {
        String targetPartitionSchema = getInputTargetPartitionSchema(configuration);
        if (targetPartitionSchema == null || targetPartitionSchema.length() == 0) {
            return new String[0];
        }
        return ConnectorSchemaUtils.unquoteFieldNamesArray(ConnectorSchemaUtils.convertFieldNamesToArray(TeradataSchemaUtils.getFieldNamesFromSchema(targetPartitionSchema)));
    }

    public static void setInputStageDatabase(Configuration configuration, String stageDatabase) {
        if (stageDatabase != null && stageDatabase.trim().length() > 0) {
            configuration.set(PROPERTY_INPUT_STAGE_DATABASE, stageDatabase.trim());
        }
    }

    public static String getInputStageDatabase(Configuration configuration) {
        return configuration.get(PROPERTY_INPUT_STAGE_DATABASE, "");
    }

    public static void setInputStageDatabaseForTable(Configuration configuration, String stageDatabaseForTable) {
        if (stageDatabaseForTable != null && stageDatabaseForTable.trim().length() > 0) {
            configuration.set(PROPERTY_INPUT_STAGE_DATABASE_FOR_TABLE, stageDatabaseForTable.trim());
        }
    }

    public static String getInputStageDatabaseForTable(Configuration configuration) {
        return configuration.get(PROPERTY_INPUT_STAGE_DATABASE_FOR_TABLE, "");
    }

    public static void setInputStageDatabaseForView(Configuration configuration, String stageDatabaseForView) {
        if (stageDatabaseForView != null && stageDatabaseForView.trim().length() > 0) {
            configuration.set(PROPERTY_INPUT_STAGE_DATABASE_FOR_VIEW, stageDatabaseForView.trim());
        }
    }

    public static String getInputStageDatabaseForView(Configuration configuration) {
        return configuration.get(PROPERTY_INPUT_STAGE_DATABASE_FOR_VIEW, "");
    }

    public static void setInputStageTableName(Configuration configuration, String stageTableName) {
        if (stageTableName != null && !stageTableName.isEmpty() && stageTableName != null && !stageTableName.isEmpty()) {
            ConnectorSchemaParser parser = new ConnectorSchemaParser();
            parser.setDelimChar('.');
            List<String> tokens = parser.tokenize(stageTableName, 2, false);
            switch (tokens.size()) {
                case 1:
                    configuration.set(PROPERTY_INPUT_STAGE_TABLE_NAME, (String) tokens.get(0));
                    return;
                case 2:
                    configuration.set(PROPERTY_INPUT_STAGE_DATABASE, (String) tokens.get(0));
                    configuration.set(PROPERTY_INPUT_STAGE_TABLE_NAME, (String) tokens.get(1));
                    return;
                default:
                    return;
            }
        }
    }

    public static String getInputStageTableName(Configuration configuration) {
        return configuration.get(PROPERTY_INPUT_STAGE_TABLE_NAME, "");
    }

    public static void setInputStageForced(Configuration configuration, boolean force) {
        configuration.setBoolean(PROPERTY_INPUT_STAGE_FORCED, force);
    }

    public static boolean getInputStageForced(Configuration configuration) {
        return configuration.getBoolean(PROPERTY_INPUT_STAGE_FORCED, false);
    }

    public static void setInputNumMappers(Configuration configuration, int numMappers) {
        configuration.setInt(PROPERTY_INPUT_NUMBER_MAPPERS, numMappers);
    }

    public static int getInputNumMappers(Configuration configuration) {
        return configuration.getInt(PROPERTY_INPUT_NUMBER_MAPPERS, 2);
    }

    public static void setInputBatchSize(Configuration configuration, int batchSize) {
        configuration.setInt(PROPERTY_INPUT_BATCH_SIZE, batchSize);
    }

    public static int getInputBatchSize(Configuration configuration) {
        return configuration.getInt(PROPERTY_INPUT_BATCH_SIZE, ErrorCode.UNKNOW_ERROR);
    }

    public static void setInputSplitByColumn(Configuration configuration, String column) {
        if (column != null && column.trim().length() > 0) {
            configuration.set(PROPERTY_INPUT_SPLIT_BY_COLUMN, column.trim());
        }
    }

    public static String getInputSplitByColumn(Configuration configuration) {
        return configuration.get(PROPERTY_INPUT_SPLIT_BY_COLUMN, "");
    }

    public static void setInputAccessLock(Configuration configuration, boolean accessLock) {
        configuration.setBoolean(PROPERTY_INPUT_ACCESS_LOCK, accessLock);
    }

    public static boolean getInputAccessLock(Configuration configuration) {
        return configuration.getBoolean(PROPERTY_INPUT_ACCESS_LOCK, false);
    }

    public static void setInputQueryBand(Configuration configuration, String queryBandProperty) {
        configuration.set(PROPERTY_INPUT_QUERY_BAND, queryBandProperty);
    }

    public static String getInputQueryBand(Configuration configuration) {
        return configuration.get(PROPERTY_INPUT_QUERY_BAND, "");
    }

    public static void setInputNumPartitionsInStaging(Configuration configuration, long numPartitionsInStaging) {
        configuration.setLong(PROPERTY_INPUT_NUMBER_PARTITIONS_IN_STAGING, numPartitionsInStaging);
    }

    public static long getInputNumPartitionsInStaging(Configuration configuration) {
        return configuration.getLong(PROPERTY_INPUT_NUMBER_PARTITIONS_IN_STAGING, 0);
    }

    public static void setInputFastExportSocketHost(Configuration configuration, String host) {
        if (host != null && host.trim().length() > 0) {
            configuration.set(PROPERTY_INPUT_FASTEXPORT_SOCKET_HOST, host.trim());
        }
    }

    public static String getInputFastExportSocketHost(Configuration configuration) {
        return configuration.get(PROPERTY_INPUT_FASTEXPORT_SOCKET_HOST, "localhost");
    }

    public static void setInputFastExportSocketPort(Configuration configuration, Integer port) {
        configuration.setInt(PROPERTY_INPUT_FASTEXPORT_SOCKET_PORT, port.intValue());
    }

    public static int getInputFastExportSocketPort(Configuration configuration) {
        return configuration.getInt(PROPERTY_INPUT_FASTEXPORT_SOCKET_PORT, 0);
    }

    public static void setInputFastExportSocketTimeout(Configuration configuration, Long timeout) {
        configuration.setLong(PROPERTY_INPUT_FASTEXPORT_SOCKET_TIMEOUT, timeout.longValue());
    }

    public static long getInputFastExportSocketTimeout(Configuration configuration) {
        return (long) configuration.getInt(PROPERTY_INPUT_FASTEXPORT_SOCKET_TIMEOUT, 480000);
    }

    public static void setInputSourceDatabase(Configuration configuration, String database) {
        configuration.set(PROPERTY_INPUT_SOURCE_DATABASE, database);
    }

    public static void setInputJobStagingAreas(Configuration configuration, String stagingAreas) {
        if (stagingAreas != null && stagingAreas.trim().length() > 0) {
            configuration.set(PROPERTY_INPUT_JOB_STAGING_AREAS, stagingAreas.trim());
        }
    }

    public static String getInputJobStagingAreas(Configuration configuration) {
        return configuration.get(PROPERTY_INPUT_JOB_STAGING_AREAS, "");
    }

    public static void setInputJobNumAmps(Configuration configuration, int numAmps) {
        configuration.setInt(PROPERTY_INPUT_JOB_NUMBER_AMPS, numAmps);
    }

    public static int getInputJobNumAmps(Configuration configuration) {
        return configuration.getInt(PROPERTY_INPUT_JOB_NUMBER_AMPS, 2);
    }

    public static void setInputJobFieldTypes(Configuration configuration, String[] typeValues) {
        if (typeValues != null) {
            configuration.setStrings(PROPERTY_INPUT_JOB_FIELD_TYPES, typeValues);
        }
    }

    public static int[] getInputJobFieldTypes(Configuration configuration) {
        String[] typeValues = configuration.getStrings(PROPERTY_INPUT_JOB_FIELD_TYPES);
        if (typeValues == null) {
            return null;
        }
        int[] types = new int[typeValues.length];
        for (int i = 0; i < types.length; i++) {
            types[i] = Integer.valueOf(typeValues[i]).intValue();
        }
        return types;
    }

    public static void setInputJobStageEnabled(Configuration configuration, boolean isSkipped) {
        configuration.setBoolean(PROPERTY_INPUT_JOB_STAGE_ENABLED, isSkipped);
    }

    public static boolean isInputJobStageEnabled(Configuration configuration) {
        return configuration.getBoolean(PROPERTY_INPUT_JOB_STAGE_ENABLED, false);
    }

    public static void setInputJobSplitSql(Configuration configuration, String splitSQL) {
        if (splitSQL != null && splitSQL.length() > 0) {
            configuration.set(PROPERTY_INPUT_JOB_SPLIT_SQL, splitSQL);
        }
    }

    public static String getInputJobSplitSql(Configuration configuration) {
        return configuration.get(PROPERTY_INPUT_JOB_SPLIT_SQL, "");
    }

    public static void setInputJobSourceTableDesc(Configuration configuration, String tableDescJson) {
        configuration.set(PROPERTY_INPUT_JOB_SOURCE_TABLE_DESC, tableDescJson);
    }

    public static String getInputJobSourceTableDesc(Configuration configuration) {
        return configuration.get(PROPERTY_INPUT_JOB_SOURCE_TABLE_DESC, "");
    }

    public static void setInputJobTargetTableDesc(Configuration configuration, String tableDescJson) {
        configuration.set(PROPERTY_INPUT_JOB_TARGET_TABLE_DESC, tableDescJson);
    }

    public static String getInputJobTargetTableDesc(Configuration configuration) {
        return configuration.get(PROPERTY_INPUT_JOB_TARGET_TABLE_DESC, "");
    }

    public static void setInputJobSourceFieldsDesc(Configuration configuration, String fieldsDescJson) {
        configuration.set(PROPERTY_INPUT_JOB_SOURCE_FIELDS_DESC, fieldsDescJson);
    }

    public static String getInputJobSourceFieldsDesc(Configuration configuration) {
        return configuration.get(PROPERTY_INPUT_JOB_SOURCE_FIELDS_DESC, "");
    }

    public static void setInputJobTargetFieldsDesc(Configuration configuration, String fieldsDescJson) {
        configuration.set(PROPERTY_INPUT_JOB_TARGET_FIELDS_DESC, fieldsDescJson);
    }

    public static String getInputJobTargetFieldsDesc(Configuration configuration) {
        return configuration.get(PROPERTY_INPUT_JOB_TARGET_FIELDS_DESC, "");
    }

    public static void setInputNumPartitionsInStagingProvided(Configuration configuration, boolean provided) {
        configuration.setBoolean(PROPERTY_INPUT_JOB_NUMBER_PARTITIONS_IN_STAGING_PROVIDED, provided);
    }

    public static boolean getInputNumPartitionsInStagingProvided(Configuration configuration) {
        return configuration.getBoolean(PROPERTY_INPUT_JOB_NUMBER_PARTITIONS_IN_STAGING_PROVIDED, false);
    }

    public static void setOutputSeparator(Configuration configuration, String separator) throws ConnectorException {
        configuration.set(PROPERTY_OUTPUT_FIELDS_SEPARATOR, separator);
    }

    public static String getOutputSeparator(Configuration configuration) throws ConnectorException {
        return configuration.get(PROPERTY_OUTPUT_FIELDS_SEPARATOR, "\t");
    }

    public static void setOutputLineSeparator(Configuration configuration, String lineSeparator) throws ConnectorException {
        configuration.set(PROPERTY_OUTPUT_LINE_SEPARATOR, lineSeparator);
    }

    public static String getOutputLineSeparator(Configuration configuration) throws ConnectorException {
        return configuration.get(PROPERTY_OUTPUT_LINE_SEPARATOR, "\n");
    }

    public static void setOutputEnclosedByString(Configuration configuration, String enclosedByString) throws ConnectorException {
        configuration.set(PROPERTY_OUTPUT_ENCLOSED_BY, enclosedByString);
    }

    public static String getOutputEnclosedByString(Configuration configuration) throws ConnectorException {
        return configuration.get(PROPERTY_OUTPUT_ENCLOSED_BY);
    }

    public static void setOutputEscapedByString(Configuration configuration, String escapedByString) throws ConnectorException {
        configuration.set(PROPERTY_OUTPUT_ESCAPED_BY, escapedByString);
    }

    public static String getOutputEscapedByString(Configuration configuration) throws ConnectorException {
        return configuration.get(PROPERTY_OUTPUT_ESCAPED_BY);
    }

    public static void setOutputJobType(Configuration configuration, String type) {
        configuration.set(PROPERTY_OUTPUT_JOB_TYPE, type);
    }

    public static String getOutputJobType(Configuration configuration) {
        return configuration.get(PROPERTY_OUTPUT_JOB_TYPE, "hdfs");
    }

    public static void setOutputFileFormat(Configuration configuration, String format) {
        configuration.set(PROPERTY_OUTPUT_FILE_FORMAT, format);
    }

    public static String getOutputFileFormat(Configuration configuration) {
        return configuration.get(PROPERTY_OUTPUT_FILE_FORMAT, "textfile");
    }

    public static void setOutputMethod(Configuration configuration, String outputMethod) {
        if (outputMethod != null && outputMethod.trim().length() > 0) {
            configuration.set(PROPERTY_OUTPUT_METHOD, outputMethod);
        }
    }

    public static String getOutputMethod(Configuration configuration) {
        return configuration.get(PROPERTY_OUTPUT_METHOD, "batch.insert");
    }

    public static void setOutputSourceDatabase(Configuration configuration, String database) {
        configuration.set(PROPERTY_OUTPUT_SOURCE_DATABASE, database);
    }

    public static String getOutputSourceDatabase(Configuration configuration) {
        return configuration.get(PROPERTY_OUTPUT_SOURCE_DATABASE, "");
    }

    public static void setOutputSourceTable(Configuration configuration, String table) {
        ConnectorSchemaParser parser = new ConnectorSchemaParser();
        parser.setDelimChar('.');
        List<String> tokens = parser.tokenize(table, 2, false);
        switch (tokens.size()) {
            case 1:
                configuration.set(PROPERTY_OUTPUT_SOURCE_TABLE, (String) tokens.get(0));
                return;
            case 2:
                configuration.set(PROPERTY_OUTPUT_SOURCE_DATABASE, (String) tokens.get(0));
                configuration.set(PROPERTY_OUTPUT_SOURCE_TABLE, (String) tokens.get(1));
                return;
            default:
                return;
        }
    }

    public static String getOutputSourceTable(Configuration configuration) {
        return configuration.get(PROPERTY_OUTPUT_SOURCE_TABLE, "");
    }

    public static void setOutputSourcePaths(Configuration configuration, String paths) {
        configuration.set(PROPERTY_OUTPUT_SOURCE_PATHS, paths);
    }

    public static String getOutputSourcePaths(Configuration configuration) {
        return configuration.get(PROPERTY_OUTPUT_SOURCE_PATHS, "");
    }

    public static void setOutputSourceTableSchema(Configuration configuration, String schema) {
        configuration.set(PROPERTY_OUTPUT_SOURCE_TABLE_SCHEMA, schema);
    }

    public static String getOutputSourceTableSchema(Configuration configuration) {
        return configuration.get(PROPERTY_OUTPUT_SOURCE_TABLE_SCHEMA, "");
    }

    public static void setOutputSourcePartitionSchema(Configuration configuration, String schema) {
        configuration.set(PROPERTY_OUTPUT_SOURCE_PARTITION_SCHEMA, schema);
    }

    public static String getOutputSourcePartitionSchema(Configuration configuration) {
        return configuration.get(PROPERTY_OUTPUT_SOURCE_PARTITION_SCHEMA, "");
    }

    public static String getOutputTargetDatabase(Configuration configuration) {
        return configuration.get(PROPERTY_OUTPUT_TARGET_DATABASE, "");
    }

    public static void setOutputTargetTable(Configuration configuration, String table) {
        ConnectorSchemaParser parser = new ConnectorSchemaParser();
        parser.setDelimChar('.');
        List<String> tokens = parser.tokenize(table, 2, false);
        switch (tokens.size()) {
            case 1:
                configuration.set(PROPERTY_OUTPUT_TARGET_TABLE, (String) tokens.get(0));
                return;
            case 2:
                configuration.set(PROPERTY_OUTPUT_TARGET_DATABASE, (String) tokens.get(0));
                configuration.set(PROPERTY_OUTPUT_TARGET_TABLE, (String) tokens.get(1));
                return;
            default:
                return;
        }
    }

    public static String getOutputTargetTable(Configuration configuration) {
        return configuration.get(PROPERTY_OUTPUT_TARGET_TABLE, "");
    }

    public static void setOutputSourceFieldNames(Configuration configuration, String fieldNames) {
        configuration.set(PROPERTY_OUTPUT_SOURCE_FIELD_NAMES, fieldNames);
        configuration.set(PROPERTY_OUTPUT_SOURCE_FIELD_NAMES_JSON, ConnectorSchemaUtils.fieldNamesToJson(ConnectorSchemaUtils.unquoteFieldNamesArray(ConnectorSchemaUtils.convertFieldNamesToArray(fieldNames))));
    }

    public static String getOutputSourceFieldNames(Configuration configuration) {
        return configuration.get(PROPERTY_OUTPUT_SOURCE_FIELD_NAMES, "");
    }

    public static void setOutputTargetFieldNames(Configuration configuration, String fieldNames) {
        configuration.set(PROPERTY_OUTPUT_TARGET_FIELD_NAMES, fieldNames);
        configuration.set(PROPERTY_OUTPUT_TARGET_FIELD_NAMES_JSON, ConnectorSchemaUtils.fieldNamesToJson(ConnectorSchemaUtils.unquoteFieldNamesArray(ConnectorSchemaUtils.convertFieldNamesToArray(fieldNames))));
    }

    public static String getOutputTargetFieldNames(Configuration configuration) {
        return configuration.get(PROPERTY_OUTPUT_TARGET_FIELD_NAMES, "");
    }

    public static void setOutputSourceFieldNamesArray(Configuration configuration, String[] fieldNamesArray) {
        configuration.set(PROPERTY_OUTPUT_SOURCE_FIELD_NAMES, ConnectorSchemaUtils.concatFieldNamesArray(ConnectorSchemaUtils.quoteFieldNamesArray(fieldNamesArray)));
        configuration.set(PROPERTY_OUTPUT_SOURCE_FIELD_NAMES_JSON, ConnectorSchemaUtils.fieldNamesToJson(fieldNamesArray));
    }

    public static String[] getOutputSourceFieldNamesArray(Configuration configuration) {
        return ConnectorSchemaUtils.fieldNamesFromJson(configuration.get(PROPERTY_OUTPUT_SOURCE_FIELD_NAMES_JSON, ""));
    }

    public static void setOutputTargetFieldNamesArray(Configuration configuration, String[] fieldNamesArray) {
        configuration.set(PROPERTY_OUTPUT_TARGET_FIELD_NAMES, ConnectorSchemaUtils.concatFieldNamesArray(ConnectorSchemaUtils.quoteFieldNamesArray(fieldNamesArray)));
        configuration.set(PROPERTY_OUTPUT_TARGET_FIELD_NAMES_JSON, ConnectorSchemaUtils.fieldNamesToJson(fieldNamesArray));
    }

    public static String[] getOutputTargetFieldNamesArray(Configuration configuration) {
        return ConnectorSchemaUtils.fieldNamesFromJson(configuration.get(PROPERTY_OUTPUT_TARGET_FIELD_NAMES_JSON, ""));
    }

    public static void setOutputTargetFieldCount(Configuration configuration, int fieldCount) {
        configuration.setInt(PROPERTY_OUTPUT_TARGET_FIELD_COUNT, fieldCount);
    }

    public static int getOutputTargetFieldCount(Configuration configuration) {
        return configuration.getInt(PROPERTY_OUTPUT_TARGET_FIELD_COUNT, 0);
    }

    public static void setOutputStageDatabase(Configuration configuration, String database) {
        if (database != null && database.trim().length() > 0) {
            configuration.set(PROPERTY_OUTPUT_STAGE_DATABASE, ConnectorSchemaUtils.unquoteFieldName(database.trim()));
        }
    }

    public static String getOutputStageDatabase(Configuration configuration) {
        return configuration.get(PROPERTY_OUTPUT_STAGE_DATABASE, "");
    }

    public static void setOutputStageDatabaseForTable(Configuration configuration, String databasefortable) {
        if (databasefortable != null && databasefortable.trim().length() > 0) {
            configuration.set(PROPERTY_OUTPUT_STAGE_DATABASE_FOR_TABLE, ConnectorSchemaUtils.unquoteFieldName(databasefortable.trim()));
        }
    }

    public static String getOutputStageDatabaseForTable(Configuration configuration) {
        return configuration.get(PROPERTY_OUTPUT_STAGE_DATABASE_FOR_TABLE, "");
    }

    public static void setOutputStageDatabaseForView(Configuration configuration, String databasefortableforview) {
        if (databasefortableforview != null && databasefortableforview.trim().length() > 0) {
            configuration.set(PROPERTY_OUTPUT_STAGE_DATABASE_FOR_VIEW, ConnectorSchemaUtils.unquoteFieldName(databasefortableforview.trim()));
        }
    }

    public static String getOutputStageDatabaseForView(Configuration configuration) {
        return configuration.get(PROPERTY_OUTPUT_STAGE_DATABASE_FOR_VIEW, "");
    }

    public static void setOutputStageTableName(Configuration configuration, String table) {
        if (table != null && !table.isEmpty()) {
            ConnectorSchemaParser parser = new ConnectorSchemaParser();
            parser.setDelimChar('.');
            List<String> tokens = parser.tokenize(table, 2, false);
            switch (tokens.size()) {
                case 1:
                    configuration.set(PROPERTY_OUTPUT_STAGE_TABLE_NAME, ConnectorSchemaUtils.unquoteFieldName((String) tokens.get(0)));
                    return;
                case 2:
                    configuration.set(PROPERTY_OUTPUT_STAGE_DATABASE, ConnectorSchemaUtils.unquoteFieldName((String) tokens.get(0)));
                    configuration.set(PROPERTY_OUTPUT_STAGE_TABLE_NAME, ConnectorSchemaUtils.unquoteFieldName((String) tokens.get(1)));
                    return;
                default:
                    return;
            }
        }
    }

    public static String getOutputStageTableName(Configuration configuration) {
        return configuration.get(PROPERTY_OUTPUT_STAGE_TABLE_NAME, "");
    }

    public static void setOutputErrorTableName(Configuration configuration, String table) {
        if (table != null && !table.isEmpty()) {
            configuration.set(PROPERTY_OUTPUT_ERROR_TABLE_NAME, table);
        }
    }

    public static String getOutputErrorTableName(Configuration configuration) {
        return configuration.get(PROPERTY_OUTPUT_ERROR_TABLE_NAME, "");
    }

    public static void setOutputStageTableKept(Configuration configuration, Boolean kept) {
        configuration.setBoolean(PROPERTY_OUTPUT_STAGE_TABLE_KEPT, kept.booleanValue());
    }

    public static Boolean getOutputStageTableKept(Configuration configuration) {
        return Boolean.valueOf(configuration.getBoolean(PROPERTY_OUTPUT_STAGE_TABLE_KEPT, VALUE_STAGE_TABLE_KEPT.booleanValue()));
    }

    public static void setOutputStageForced(Configuration configuration, boolean force) {
        configuration.setBoolean(PROPERTY_OUTPUT_STAGE_FORCED, force);
    }

    public static boolean getOutputStageForced(Configuration configuration) {
        return configuration.getBoolean(PROPERTY_OUTPUT_STAGE_FORCED, false);
    }

    public static void setOutputFastloadSocketHost(Configuration configuration, String fastloadSocketHost) {
        if (fastloadSocketHost != null && fastloadSocketHost.trim().length() > 0) {
            configuration.set(PROPERTY_OUTPUT_FASTLOAD_SOCKET_HOST, fastloadSocketHost.trim());
        }
    }

    public static String getOutputFastloadSocketHost(Configuration configuration) {
        return configuration.get(PROPERTY_OUTPUT_FASTLOAD_SOCKET_HOST, "localhost");
    }

    public static void setOutputFastloadSocketPort(Configuration configuration, Integer fastloadSocketPort) {
        configuration.setInt(PROPERTY_OUTPUT_FASTLOAD_SOCKET_PORT, fastloadSocketPort.intValue());
    }

    public static int getOutputFastloadSocketPort(Configuration configuration) {
        return configuration.getInt(PROPERTY_OUTPUT_FASTLOAD_SOCKET_PORT, 0);
    }

    public static void setOutputFastloadSocketTimeout(Configuration configuration, Long timeout) {
        configuration.setLong(PROPERTY_OUTPUT_FASTLOAD_SOCKET_TIMEOUT, timeout.longValue());
    }

    public static long getOutputFastloadSocketTimeout(Configuration configuration) {
        return (long) configuration.getInt(PROPERTY_OUTPUT_FASTLOAD_SOCKET_TIMEOUT, 480000);
    }

    public static void setCustomParseMode(Configuration configuration, boolean customParse) {
        configuration.setBoolean(PROPERTY_CUSTOM_PARSE, customParse);
    }

    public static boolean getCustomParseMode(Configuration configuration) {
        return configuration.getBoolean(PROPERTY_CUSTOM_PARSE, false);
    }

    public static void setDataDictionaryUseXViews(Configuration configuration, boolean useXViews) {
        configuration.setBoolean(PROPERTY_JOB_DATA_DICTIONARY_USE_XVIEWS, useXViews);
    }

    public static boolean getDataDictionaryUseXViews(Configuration configuration) {
        return configuration.getBoolean(PROPERTY_JOB_DATA_DICTIONARY_USE_XVIEWS, true);
    }

    public static void setOutputNumMappers(Configuration configuration, int numMappers) {
        configuration.setInt(PROPERTY_OUTPUT_NUMBER_MAPPERS, numMappers);
    }

    public static int getOutputNumMappers(Configuration configuration) {
        return configuration.getInt(PROPERTY_OUTPUT_NUMBER_MAPPERS, 2);
    }

    public static void setOutputNumReducers(Configuration configuration, int numMappers) {
        configuration.setInt(PROPERTY_OUTPUT_NUMBER_REDUCERS, numMappers);
    }

    public static int getOutputNumReducers(Configuration configuration) {
        return configuration.getInt(PROPERTY_OUTPUT_NUMBER_REDUCERS, 0);
    }

    public static void setOutputBatchSize(Configuration configuration, Integer batchSize) {
        configuration.setInt(PROPERTY_OUTPUT_BATCH_SIZE, batchSize.intValue());
    }

    public static int getOutputBatchSize(Configuration configuration) {
        return configuration.getInt(PROPERTY_OUTPUT_BATCH_SIZE, ErrorCode.UNKNOW_ERROR);
    }

    public static void setOutputQueryBand(Configuration configuration, String queryBandProperty) {
        configuration.set(PROPERTY_OUTPUT_QUERY_BAND, queryBandProperty);
    }

    public static String getOutputQueryBand(Configuration configuration) {
        return configuration.get(PROPERTY_OUTPUT_QUERY_BAND, "");
    }

    public static void setAvroSchemaFilePath(Configuration configuration, String filePath) {
        configuration.set(PROPERTY_JOB_AVRO_SCHEMA_FILE, filePath);
    }

    public static String getAvroSchemaFilePath(Configuration configuration) {
        return configuration.get(PROPERTY_JOB_AVRO_SCHEMA_FILE, "");
    }

    public static void setOutputTargetDatabase(Configuration configuration, String database) {
        configuration.set(PROPERTY_OUTPUT_TARGET_DATABASE, database);
    }

    public static void setOutputJobNumAmps(Configuration configuration, int numAmps) {
        configuration.setInt(PROPERTY_OUTPUT_JOB_NUMBER_AMPS, numAmps);
    }

    public static int getOutputJobNumAmps(Configuration configuration) {
        return configuration.getInt(PROPERTY_OUTPUT_JOB_NUMBER_AMPS, 2);
    }

    public static void setOutputJobStagingAreas(Configuration configuration, String stagingAreas) {
        if (stagingAreas != null && stagingAreas.length() > 0) {
            configuration.set(PROPERTY_OUTPUT_JOB_STAGING_AREAS, stagingAreas);
        }
    }

    public static String getOutputJobStagingAreas(Configuration configuration) {
        return configuration.get(PROPERTY_OUTPUT_JOB_STAGING_AREAS, "");
    }

    public static void setOutputJobFieldTypes(Configuration configuration, String[] typeValues) {
        if (typeValues != null) {
            configuration.setStrings(PROPERTY_OUTPUT_JOB_FIELD_TYPES, typeValues);
        }
    }

    public static int[] getOutputJobFieldTypes(Configuration configuration) {
        String[] typeValues = configuration.getStrings(PROPERTY_OUTPUT_JOB_FIELD_TYPES);
        if (typeValues == null) {
            return null;
        }
        int[] types = new int[typeValues.length];
        for (int i = 0; i < types.length; i++) {
            types[i] = Integer.valueOf(typeValues[i]).intValue();
        }
        return types;
    }

    public static void setOutputJobStageEnabled(Configuration configuration, boolean isEnabled) {
        configuration.setBoolean(PROPERTY_OUTPUT_JOB_STAGE_ENABLED, isEnabled);
    }

    public static boolean isOutputJobStageEnabled(Configuration configuration) {
        return configuration.getBoolean(PROPERTY_OUTPUT_JOB_STAGE_ENABLED, false);
    }

    public static void setOutputJobFastloadLSN(Configuration configuration, String lsnNumber) {
        if (lsnNumber != null && lsnNumber.length() > 0) {
            configuration.set(PROPERTY_OUTPUT_JOB_FASTLOAD_LSN, lsnNumber);
        }
    }

    public static String getOutputJobFastloadLSN(Configuration configuration) {
        return configuration.get(PROPERTY_OUTPUT_JOB_FASTLOAD_LSN, "");
    }

    public static void setOutputJobErrorTable1(Configuration configuration, String table) {
        configuration.set(PROPERTY_OUTPUT_JOB_ERROR_TABLE1, table);
    }

    public static String getOutputJobErrorTable1(Configuration configuration) {
        return configuration.get(PROPERTY_OUTPUT_JOB_ERROR_TABLE1, "");
    }

    public static void setOutputJobErrorTable2(Configuration configuration, String table) {
        configuration.set(PROPERTY_OUTPUT_JOB_ERROR_TABLE2, table);
    }

    public static String getOutputJobErrorTable2(Configuration configuration) {
        return configuration.get(PROPERTY_OUTPUT_JOB_ERROR_TABLE2, "");
    }

    public static void setOutputJobSourceTableDesc(Configuration configuration, String tableDescJson) {
        configuration.set(PROPERTY_OUTPUT_JOB_SOURCE_TABLE_DESC, tableDescJson);
    }

    public static String getOutputJobSourceTableDesc(Configuration configuration) {
        return configuration.get(PROPERTY_OUTPUT_JOB_SOURCE_TABLE_DESC, "");
    }

    public static void setOutputJobTargetTableDesc(Configuration configuration, String tableDescJson) {
        configuration.set(PROPERTY_OUTPUT_JOB_TARGET_TABLE_DESC, tableDescJson);
    }

    public static String getOutputJobTargetTableDesc(Configuration configuration) {
        return configuration.get(PROPERTY_OUTPUT_JOB_TARGET_TABLE_DESC, "");
    }

    public static void setOutputJobSourceFieldsDesc(Configuration configuration, String fieldsDescJson) {
        configuration.set(PROPERTY_OUTPUT_JOB_SOURCE_FIELDS_DESC, fieldsDescJson);
    }

    public static String getOutputJobSourceFieldsDesc(Configuration configuration) {
        return configuration.get(PROPERTY_OUTPUT_JOB_SOURCE_FIELDS_DESC, "");
    }

    public static void setOutputJobTargetFieldsDesc(Configuration configuration, String fieldsDescJson) {
        configuration.set(PROPERTY_OUTPUT_JOB_TARGET_FIELDS_DESC, fieldsDescJson);
    }

    public static String getOutputJobTargetFieldsDesc(Configuration configuration) {
        return configuration.get(PROPERTY_OUTPUT_JOB_TARGET_FIELDS_DESC, "");
    }
}