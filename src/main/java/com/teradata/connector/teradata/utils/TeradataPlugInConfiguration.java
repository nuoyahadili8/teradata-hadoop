package com.teradata.connector.teradata.utils;

import com.teradata.connector.common.exception.ConnectorException.ErrorCode;
import com.teradata.connector.common.utils.ConnectorConfiguration;
import com.teradata.connector.common.utils.ConnectorSchemaParser;
import com.teradata.connector.common.utils.ConnectorSchemaUtils;
import com.teradata.connector.teradata.db.TeradataConnection;
import java.util.List;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.JobContext;

/**
 * @author Administrator
 */
public class TeradataPlugInConfiguration {
    public static final int BATCH_SIZE_MAX = 13683;
    public static final String PREFIX_INPUT_STAGE_VIEW = "V";
    public static final String TDCH_INPUT_JDBC_DRIVER_CLASS = "tdch.input.teradata.jdbc.driver.class";
    public static final String TDCH_INPUT_JDBC_PASSWORD = "tdch.input.teradata.jdbc.password";
    public static final String TDCH_INPUT_JDBC_URL = "tdch.input.teradata.jdbc.url";
    public static final String TDCH_INPUT_JDBC_USER_NAME = "tdch.input.teradata.jdbc.user.name";
    public static final String TDCH_INPUT_TERADATA_ACCESS_LOCK = "tdch.input.teradata.access.lock";
    public static final String TDCH_INPUT_TERADATA_BATCH_SIZE = "tdch.input.teradata.batch.size";
    public static final String TDCH_INPUT_TERADATA_CONDITIONS = "tdch.input.teradata.conditions";
    public static final String TDCH_INPUT_TERADATA_DATABASE = "tdch.input.teradata.database";
    public static final String TDCH_INPUT_TERADATA_DATA_DICTIONARY_USE_XVIEW = "tdch.input.teradata.data.dictionary.use.xview";
    public static final String TDCH_INPUT_TERADATA_FASTEXPORT_COORDINATOR_SOCKET_BACKLOG = "tdch.input.teradata.fastexport.coordinator.socket.backlog";
    public static final String TDCH_INPUT_TERADATA_FASTEXPORT_COORDINATOR_SOCKET_HOST = "tdch.input.teradata.fastexport.coordinator.socket.host";
    public static final String TDCH_INPUT_TERADATA_FASTEXPORT_COORDINATOR_SOCKET_PORT = "tdch.input.teradata.fastexport.coordinator.socket.port";
    public static final String TDCH_INPUT_TERADATA_FASTEXPORT_COORDINATOR_SOCKET_TIMEOUT = "tdch.input.teradata.fastexport.coordinator.socket.timeout";
    public static final String TDCH_INPUT_TERADATA_FASTEXPORT_FASTFAIL = "tdch.input.teradata.fastexport.fastfail";
    public static final String TDCH_INPUT_TERADATA_FASTEXPORT_LSN = "tdch.input.teradata.fastexport.lsn";
    public static final String TDCH_INPUT_TERADATA_FIELD_NAMES = "tdch.input.teradata.field.names";
    public static final String TDCH_INPUT_TERADATA_FINAL_CONDITION = "tdch.input.teradata.final.condition";
    public static final String TDCH_INPUT_TERADATA_FINAL_DATABASE = "tdch.input.teradata.final.database";
    public static final String TDCH_INPUT_TERADATA_FINAL_QUERY = "tdch.input.teradata.final.query";
    public static final String TDCH_INPUT_TERADATA_FINAL_TABLE = "tdch.input.teradata.final.table";
    public static final String TDCH_INPUT_TERADATA_NUM_AMPS = "tdch.input.teradata.num.amps";
    public static final String TDCH_INPUT_TERADATA_NUM_PARTITIONS = "tdch.input.teradata.num.partitions";
    private static final Text TDCH_INPUT_TERADATA_PASSWORD = new Text("1f6a061e-86d1-4e93-95f4-85283c3c8b44");
    public static final String TDCH_INPUT_TERADATA_QUERY = "tdch.input.teradata.query";
    public static final String TDCH_INPUT_TERADATA_QUERY_BAND = "tdch.input.teradata.query.band";
    public static final String TDCH_INPUT_TERADATA_SPLIT_BY_COLUMN = "tdch.input.teradata.split.by.column";
    public static final String TDCH_INPUT_TERADATA_SPLIT_SQL = "tdch.input.teradata.split.sql";
    public static final String TDCH_INPUT_TERADATA_STAGE_AREAS = "tdch.input.teradata.stage.areas";
    public static final String TDCH_INPUT_TERADATA_STAGE_DATABASE = "tdch.input.teradata.stage.database";
    public static final String TDCH_INPUT_TERADATA_STAGE_DATABASE_FOR_TABLE = "tdch.input.teradata.stage.database.for.table";
    public static final String TDCH_INPUT_TERADATA_STAGE_DATABASE_FOR_VIEW = "tdch.input.teradata.stage.database.for.view";
    public static final String TDCH_INPUT_TERADATA_STAGE_TABLE_BLOCKSIZE = "tdch.input.teradata.stage.table.blocksize";
    public static final String TDCH_INPUT_TERADATA_STAGE_TABLE_ENABLED = "tdch.input.teradata.stage.table.enabled";
    public static final String TDCH_INPUT_TERADATA_STAGE_TABLE_FORCED = "tdch.input.teradata.stage.table.forced";
    public static final String TDCH_INPUT_TERADATA_STAGE_TABLE_NAME = "tdch.input.teradata.stage.table.name";
    public static final String TDCH_INPUT_TERADATA_TABLE = "tdch.input.teradata.table";
    public static final String TDCH_INPUT_TERADATA_TABLE_DESC = "tdch.input.teradata.table.desc";
    private static final Text TDCH_INPUT_TERADATA_USER_NAME = new Text("6d275825-a792-4416-9492-71e0aac48832");
    public static final String TDCH_OUTPUT_JDBC_DRIVER_CLASS = "tdch.output.teradata.jdbc.driver.class";
    public static final String TDCH_OUTPUT_JDBC_PASSWORD = "tdch.output.teradata.jdbc.password";
    public static final String TDCH_OUTPUT_JDBC_URL = "tdch.output.teradata.jdbc.url";
    public static final String TDCH_OUTPUT_JDBC_USER_NAME = "tdch.output.teradata.jdbc.user.name";
    public static final String TDCH_OUTPUT_TERADATA_BATCH_SIZE = "tdch.output.teradata.batch.size";
    public static final String TDCH_OUTPUT_TERADATA_BI_DISABLE_FAILOVER = "tdch.output.teradata.bi.disable.failover";
    public static final String TDCH_OUTPUT_TERADATA_DATABASE = "tdch.output.teradata.database";
    public static final String TDCH_OUTPUT_TERADATA_DATA_DICTIONARY_USE_XVIEW = "tdch.output.teradata.data.dictionary.use.xview";
    public static final String TDCH_OUTPUT_TERADATA_ERROR_TABLE1_NAME = "tdch.output.teradata.error.table1.name";
    public static final String TDCH_OUTPUT_TERADATA_ERROR_TABLE2_NAME = "tdch.output.teradata.error.table2.name";
    public static final String TDCH_OUTPUT_TERADATA_FASTLOAD_COORDINATOR_SOCKET_BACKLOG = "tdch.output.teradata.fastload.coordinator.socket.backlog";
    public static final String TDCH_OUTPUT_TERADATA_FASTLOAD_COORDINATOR_SOCKET_HOST = "tdch.output.teradata.fastload.coordinator.socket.host";
    public static final String TDCH_OUTPUT_TERADATA_FASTLOAD_COORDINATOR_SOCKET_PORT = "tdch.output.teradata.fastload.coordinator.socket.port";
    public static final String TDCH_OUTPUT_TERADATA_FASTLOAD_COORDINATOR_SOCKET_TIMEOUT = "tdch.output.teradata.fastload.coordinator.socket.timeout";
    public static final String TDCH_OUTPUT_TERADATA_FASTLOAD_ERROR_LIMIT = "tdch.output.teradata.fastload.error.limit";
    public static final String TDCH_OUTPUT_TERADATA_FASTLOAD_ERROR_TABLE_DATABASE = "tdch.output.teradata.fastload.error.table.database";
    public static final String TDCH_OUTPUT_TERADATA_FASTLOAD_ERROR_TABLE_NAME = "tdch.output.teradata.fastload.error.table.name";
    public static final String TDCH_OUTPUT_TERADATA_FASTLOAD_FASTFAIL = "tdch.output.teradata.fastload.fastfail";
    public static final String TDCH_OUTPUT_TERADATA_FASTLOAD_LSN = "tdch.output.teradata.fastload.lsn";
    public static final String TDCH_OUTPUT_TERADATA_FIELD_COUNT = "tdch.output.teradata.field.count";
    public static final String TDCH_OUTPUT_TERADATA_FIELD_NAMES = "tdch.output.teradata.field.names";
    public static final String TDCH_OUTPUT_TERADATA_FINAL_DATABASE = "tdch.output.teradata.final.database";
    public static final String TDCH_OUTPUT_TERADATA_FINAL_TABLE = "tdch.output.teradata.final.table";
    public static final String TDCH_OUTPUT_TERADATA_NUM_AMPS = "tdch.output.teradata.num.amps";
    private static final Text TDCH_OUTPUT_TERADATA_PASSWORD = new Text("1a516317-99cd-4fe8-af47-2304805c8176");
    public static final String TDCH_OUTPUT_TERADATA_QUERY_BAND = "tdch.output.teradata.query.band";
    public static final String TDCH_OUTPUT_TERADATA_STAGE_AREAS = "tdch.output.teradata.stage.areas";
    public static final String TDCH_OUTPUT_TERADATA_STAGE_DATABASE = "tdch.output.teradata.stage.databse";
    public static final String TDCH_OUTPUT_TERADATA_STAGE_DATABASE_FOR_TABLE = "tdch.output.teradata.stage.databse.for.table";
    public static final String TDCH_OUTPUT_TERADATA_STAGE_DATABASE_FOR_VIEW = "tdch.output.teradata.stage.databse.for.view";
    public static final String TDCH_OUTPUT_TERADATA_STAGE_TABLE_BLOCKSIZE = "tdch.output.teradata.stage.table.blocksize";
    public static final String TDCH_OUTPUT_TERADATA_STAGE_TABLE_ENABLED = "tdch.output.teradata.stage.table.enabled";
    public static final String TDCH_OUTPUT_TERADATA_STAGE_TABLE_FORCED = "tdch.output.teradata.stage.table.forced";
    public static final String TDCH_OUTPUT_TERADATA_STAGE_TABLE_KEPT = "tdch.output.teradata.stage.table.kept";
    public static final String TDCH_OUTPUT_TERADATA_STAGE_TABLE_NAME = "tdch.output.teradata.stage.table.name";
    public static final String TDCH_OUTPUT_TERADATA_TABLE = "tdch.output.teradata.table";
    public static final String TDCH_OUTPUT_TERADATA_TABLE_DESC = "tdch.output.teradata.table.desc";
    public static final String TDCH_OUTPUT_TERADATA_TRUNCATE_TABLE = "tdch.output.teradata.truncate";
    private static final Text TDCH_OUTPUT_TERADATA_USER_NAME = new Text("0d23d08c-24f5-4060-b3fe-edbca775dad2");
    public static final String TDCH_TERADATA_REPLACE_UNSUPPORTED_UNICODE_CHARS = "tdch.teradata.replace.unsupported.unicode.characters";
    public static final String TDCH_TERADATA_UNICODE_PASSTHROUGH = "tdch.teradata.unicode.passthrough";
    public static final int TERADATA_QUERY_BAND_MAX_LENGTH = 2046;
    public static final int TERADATA_QUERY_BAND_NAME_MAX_LENGTH = 128;
    public static final int TERADATA_QUERY_BAND_VALUE_MAX_LENGTH = 256;
    public static final String VALUE_BATCH_INSERT_METHOD = "batch.insert";
    protected static final int VALUE_INPUT_TERADATA_BATCH_SIZE = 10000;
    protected static final int VALUE_INPUT_TERADATA_FASTEXPORT_SOCKET_BACKLOG = 256;
    protected static final int VALUE_INPUT_TERADATA_FASTEXPORT_SOCKET_PORT = 0;
    protected static final int VALUE_INPUT_TERADATA_FASTEXPORT_SOCKET_TIMEOUT = 480000;
    public static final String VALUE_INTERNAL_FASTEXPORT_METHOD = "internal.fastexport";
    public static final String VALUE_INTERNAL_FASTLOAD_METHOD = "internal.fastload";
    protected static final int VALUE_NUMBER_AMPS_DEFAULT = 2;
    protected static final int VALUE_OUTPUT_TERADATA_BATCH_SIZE = 10000;
    protected static final int VALUE_OUTPUT_TERADATA_FASTLOAD_ERROR_LIMIT = 0;
    protected static final int VALUE_OUTPUT_TERADATA_FASTLOAD_SOCKET_BACKLOG = 256;
    protected static final int VALUE_OUTPUT_TERADATA_FASTLOAD_SOCKET_PORT = 0;
    protected static final int VALUE_OUTPUT_TERADATA_FASTLOAD_SOCKET_TIMEOUT = 480000;
    public static final String VALUE_SPLIT_BY_AMP_METHOD = "split.by.amp";
    public static final String VALUE_SPLIT_BY_HASH_METHOD = "split.by.hash";
    public static final String VALUE_SPLIT_BY_PARTITION_METHOD = "split.by.partition";
    public static final String VALUE_SPLIT_BY_VALUE_METHOD = "split.by.value";
    public static final String VALUE_TDCH_JDBC_DRIVER_CLASS_DEFAULT = "com.teradata.jdbc.TeraDriver";

    public static void setUnicodePassthrough(Configuration configuration, boolean enablePassthrough) {
        configuration.setBoolean(TDCH_TERADATA_UNICODE_PASSTHROUGH, enablePassthrough);
    }

    public static boolean getUnicodePassthrough(Configuration configuration) {
        return configuration.getBoolean(TDCH_TERADATA_UNICODE_PASSTHROUGH, false);
    }

    public static void setReplaceUnSupportedUnicodeChars(Configuration configuration, boolean replaceUnSupportedUnicodeChars) {
        configuration.setBoolean(TDCH_TERADATA_REPLACE_UNSUPPORTED_UNICODE_CHARS, replaceUnSupportedUnicodeChars);
    }

    public static boolean getReplaceUnSupportedUnicodeChars(Configuration configuration) {
        return configuration.getBoolean(TDCH_TERADATA_REPLACE_UNSUPPORTED_UNICODE_CHARS, false);
    }

    public static void setInputDataDictionaryUseXView(Configuration configuration, boolean useXView) {
        configuration.setBoolean(TDCH_INPUT_TERADATA_DATA_DICTIONARY_USE_XVIEW, useXView);
    }

    public static boolean getInputDataDictionaryUseXView(Configuration configuration) {
        return configuration.getBoolean(TDCH_INPUT_TERADATA_DATA_DICTIONARY_USE_XVIEW, false);
    }

    public static void setInputAccessLock(Configuration configuration, boolean accessLock) {
        configuration.setBoolean(TDCH_INPUT_TERADATA_ACCESS_LOCK, accessLock);
    }

    public static boolean getInputAccessLock(Configuration configuration) {
        return configuration.getBoolean(TDCH_INPUT_TERADATA_ACCESS_LOCK, false);
    }

    public static void setInputDatabase(Configuration configuration, String inputDatabase) {
        configuration.set(TDCH_INPUT_TERADATA_DATABASE, inputDatabase);
    }

    public static String getInputDatabase(Configuration configuration) {
        return configuration.get(TDCH_INPUT_TERADATA_DATABASE, "");
    }

    public static void setInputTable(Configuration configuration, String inputTable) {
        ConnectorSchemaParser parser = new ConnectorSchemaParser();
        parser.setDelimChar('.');
        List<String> tokens = parser.tokenize(inputTable, 2, false);
        switch (tokens.size()) {
            case 1:
                configuration.set(TDCH_INPUT_TERADATA_TABLE, (String) tokens.get(0));
                return;
            case 2:
                configuration.set(TDCH_INPUT_TERADATA_DATABASE, (String) tokens.get(0));
                configuration.set(TDCH_INPUT_TERADATA_TABLE, (String) tokens.get(1));
                return;
            default:
                return;
        }
    }

    public static String getInputTable(Configuration configuration) {
        return configuration.get(TDCH_INPUT_TERADATA_TABLE, "");
    }

    public static void setInputBatchSize(Configuration configuration, int batchSize) {
        configuration.setInt(TDCH_INPUT_TERADATA_BATCH_SIZE, batchSize);
    }

    public static int getInputBatchSize(Configuration configuration) {
        return configuration.getInt(TDCH_INPUT_TERADATA_BATCH_SIZE, ErrorCode.UNKNOW_ERROR);
    }

    public static void setInputNumPartitions(Configuration configuration, long numPartitions) {
        configuration.setLong(TDCH_INPUT_TERADATA_NUM_PARTITIONS, numPartitions);
    }

    public static long getInputNumPartitions(Configuration configuration) {
        return configuration.getLong(TDCH_INPUT_TERADATA_NUM_PARTITIONS, (long) ConnectorConfiguration.getNumMappers(configuration));
    }

    public static void setInputStageDatabase(Configuration configuration, String stageDatabse) {
        configuration.set(TDCH_INPUT_TERADATA_STAGE_DATABASE, stageDatabse);
    }

    public static String getInputStageDatabase(Configuration configuration) {
        return configuration.get(TDCH_INPUT_TERADATA_STAGE_DATABASE, "");
    }

    public static void setInputStageDatabaseForTable(Configuration configuration, String stageDatabseForTable) {
        configuration.set(TDCH_INPUT_TERADATA_STAGE_DATABASE_FOR_TABLE, stageDatabseForTable);
    }

    public static String getInputStageDatabaseForTable(Configuration configuration) {
        return configuration.get(TDCH_INPUT_TERADATA_STAGE_DATABASE_FOR_TABLE, "");
    }

    public static void setInputStageDatabaseForView(Configuration configuration, String stageDatabseForView) {
        configuration.set(TDCH_INPUT_TERADATA_STAGE_DATABASE_FOR_VIEW, stageDatabseForView);
    }

    public static String getInputStageDatabaseForView(Configuration configuration) {
        return configuration.get(TDCH_INPUT_TERADATA_STAGE_DATABASE_FOR_VIEW, "");
    }

    public static void setInputStageTableName(Configuration configuration, String stageTableName) {
        if (stageTableName != null && !stageTableName.isEmpty()) {
            ConnectorSchemaParser parser = new ConnectorSchemaParser();
            parser.setDelimChar('.');
            List<String> tokens = parser.tokenize(stageTableName, 2, false);
            switch (tokens.size()) {
                case 1:
                    configuration.set(TDCH_INPUT_TERADATA_STAGE_TABLE_NAME, ConnectorSchemaUtils.unquoteFieldName((String) tokens.get(0)));
                    return;
                case 2:
                    configuration.set(TDCH_OUTPUT_TERADATA_STAGE_DATABASE, ConnectorSchemaUtils.unquoteFieldName((String) tokens.get(0)));
                    configuration.set(TDCH_INPUT_TERADATA_STAGE_TABLE_NAME, ConnectorSchemaUtils.unquoteFieldName((String) tokens.get(1)));
                    return;
                default:
                    return;
            }
        }
    }

    public static String getInputStageTableName(Configuration configuration) {
        return configuration.get(TDCH_INPUT_TERADATA_STAGE_TABLE_NAME, "");
    }

    public static void setInputQuery(Configuration configuration, String inputQuery) {
        configuration.set(TDCH_INPUT_TERADATA_QUERY, inputQuery);
    }

    public static String getInputQuery(Configuration configuration) {
        return configuration.get(TDCH_INPUT_TERADATA_QUERY, "");
    }

    public static void setInputConditions(Configuration configuration, String conditions) {
        configuration.set(TDCH_INPUT_TERADATA_CONDITIONS, conditions);
    }

    public static String getInputConditions(Configuration configuration) {
        return configuration.get(TDCH_INPUT_TERADATA_CONDITIONS, "");
    }

    public static void setInputStageTableForced(Configuration configuration, boolean stageTableForced) {
        configuration.setBoolean(TDCH_INPUT_TERADATA_STAGE_TABLE_FORCED, stageTableForced);
    }

    public static boolean getInputStageTableForced(Configuration configuration) {
        return configuration.getBoolean(TDCH_INPUT_TERADATA_STAGE_TABLE_FORCED, false);
    }

    public static void setInputStageTableBlocksize(Configuration configuration, int stageTableBlocksize) {
        configuration.setInt(TDCH_INPUT_TERADATA_STAGE_TABLE_BLOCKSIZE, stageTableBlocksize);
    }

    public static int getInputStageTableBlocksize(Configuration configuration) {
        return configuration.getInt(TDCH_INPUT_TERADATA_STAGE_TABLE_BLOCKSIZE, TeradataConnection.DB_TABLE_BLOCKSIZE_MAX);
    }

    public static void setInputSplitByColumn(Configuration configuration, String splitByColumn) {
        configuration.set(TDCH_INPUT_TERADATA_SPLIT_BY_COLUMN, splitByColumn);
    }

    public static String getInputSplitByColumn(Configuration configuration) {
        return configuration.get(TDCH_INPUT_TERADATA_SPLIT_BY_COLUMN, "");
    }

    public static void setOutputDataDictionaryUseXView(Configuration configuration, boolean useXView) {
        configuration.setBoolean(TDCH_OUTPUT_TERADATA_DATA_DICTIONARY_USE_XVIEW, useXView);
    }

    public static boolean getOutputDataDictionaryUseXView(Configuration configuration) {
        return configuration.getBoolean(TDCH_OUTPUT_TERADATA_DATA_DICTIONARY_USE_XVIEW, false);
    }

    public static void setOutputDatabase(Configuration configuration, String outputDatabase) {
        configuration.set(TDCH_OUTPUT_TERADATA_DATABASE, outputDatabase);
    }

    public static String getOutputDatabase(Configuration configuration) {
        return configuration.get(TDCH_OUTPUT_TERADATA_DATABASE, "");
    }

    public static void setOutputTable(Configuration configuration, String outputTable) {
        ConnectorSchemaParser parser = new ConnectorSchemaParser();
        parser.setDelimChar('.');
        List<String> tokens = parser.tokenize(outputTable, 2, false);
        switch (tokens.size()) {
            case 1:
                configuration.set(TDCH_OUTPUT_TERADATA_TABLE, (String) tokens.get(0));
                return;
            case 2:
                configuration.set(TDCH_OUTPUT_TERADATA_DATABASE, (String) tokens.get(0));
                configuration.set(TDCH_OUTPUT_TERADATA_TABLE, (String) tokens.get(1));
                return;
            default:
                return;
        }
    }

    public static String getOutputTable(Configuration configuration) {
        return configuration.get(TDCH_OUTPUT_TERADATA_TABLE, "");
    }

    public static void setOutputStageDatabase(Configuration configuration, String stageDatabse) {
        configuration.set(TDCH_OUTPUT_TERADATA_STAGE_DATABASE, stageDatabse);
    }

    public static String getOutputStageDatabase(Configuration configuration) {
        return configuration.get(TDCH_OUTPUT_TERADATA_STAGE_DATABASE, "");
    }

    public static void setOutputStageDatabaseForTable(Configuration configuration, String stageDatabseForTable) {
        configuration.set(TDCH_OUTPUT_TERADATA_STAGE_DATABASE_FOR_TABLE, stageDatabseForTable);
    }

    public static String getOutputStageDatabaseForTable(Configuration configuration) {
        return configuration.get(TDCH_OUTPUT_TERADATA_STAGE_DATABASE_FOR_TABLE, "");
    }

    public static void setOutputStageDatabaseForView(Configuration configuration, String stageDatabseForView) {
        configuration.set(TDCH_OUTPUT_TERADATA_STAGE_DATABASE_FOR_VIEW, stageDatabseForView);
    }

    public static String getOutputStageDatabaseForView(Configuration configuration) {
        return configuration.get(TDCH_OUTPUT_TERADATA_STAGE_DATABASE_FOR_VIEW, "");
    }

    public static void setOutputStageTableName(Configuration configuration, String stageTableName) {
        if (stageTableName != null && !stageTableName.isEmpty()) {
            ConnectorSchemaParser parser = new ConnectorSchemaParser();
            parser.setDelimChar('.');
            List<String> tokens = parser.tokenize(stageTableName, 2, false);
            switch (tokens.size()) {
                case 1:
                    configuration.set(TDCH_OUTPUT_TERADATA_STAGE_TABLE_NAME, ConnectorSchemaUtils.unquoteFieldName((String) tokens.get(0)));
                    return;
                case 2:
                    configuration.set(TDCH_OUTPUT_TERADATA_STAGE_DATABASE, ConnectorSchemaUtils.unquoteFieldName((String) tokens.get(0)));
                    configuration.set(TDCH_OUTPUT_TERADATA_STAGE_TABLE_NAME, ConnectorSchemaUtils.unquoteFieldName((String) tokens.get(1)));
                    return;
                default:
                    return;
            }
        }
    }

    public static String getOutputStageTableName(Configuration configuration) {
        return configuration.get(TDCH_OUTPUT_TERADATA_STAGE_TABLE_NAME, "");
    }

    public static void setOutputBatchSize(Configuration configuration, int batchSize) {
        configuration.setInt(TDCH_OUTPUT_TERADATA_BATCH_SIZE, batchSize);
    }

    public static int getOutputBatchSize(Configuration configuration) {
        return configuration.getInt(TDCH_OUTPUT_TERADATA_BATCH_SIZE, ErrorCode.UNKNOW_ERROR);
    }

    public static void setOutputStageTableForced(Configuration configuration, boolean stageTableForced) {
        configuration.setBoolean(TDCH_OUTPUT_TERADATA_STAGE_TABLE_FORCED, stageTableForced);
    }

    public static boolean getOutputStageTableForced(Configuration configuration) {
        return configuration.getBoolean(TDCH_OUTPUT_TERADATA_STAGE_TABLE_FORCED, false);
    }

    public static void setOutputStageTableKept(Configuration configuration, boolean stageTableKept) {
        configuration.setBoolean(TDCH_OUTPUT_TERADATA_STAGE_TABLE_KEPT, stageTableKept);
    }

    public static boolean getOutputStageTableKept(Configuration configuration) {
        return configuration.getBoolean(TDCH_OUTPUT_TERADATA_STAGE_TABLE_KEPT, false);
    }

    public static void setOutputStageTableBlocksize(Configuration configuration, int stageTableBlocksize) {
        configuration.setInt(TDCH_OUTPUT_TERADATA_STAGE_TABLE_BLOCKSIZE, stageTableBlocksize);
    }

    public static int getOutputStageTableBlocksize(Configuration configuration) {
        return configuration.getInt(TDCH_OUTPUT_TERADATA_STAGE_TABLE_BLOCKSIZE, TeradataConnection.DB_TABLE_BLOCKSIZE_MAX);
    }

    public static void setOutputFieldCount(Configuration configuration, int fieldCount) {
        configuration.setInt(TDCH_OUTPUT_TERADATA_FIELD_COUNT, fieldCount);
    }

    public static int getOutputFieldCount(Configuration configuration) {
        return configuration.getInt(TDCH_OUTPUT_TERADATA_FIELD_COUNT, 0);
    }

    public static void setOutputQueryBand(Configuration configuration, String queryBand) {
        configuration.set(TDCH_OUTPUT_TERADATA_QUERY_BAND, queryBand);
    }

    public static String getOutputQueryBand(Configuration configuration) {
        return configuration.get(TDCH_OUTPUT_TERADATA_QUERY_BAND, "");
    }

    public static void setOutputTeradataTruncate(Configuration configuration, boolean truncateEnabled) {
        configuration.setBoolean(TDCH_OUTPUT_TERADATA_TRUNCATE_TABLE, truncateEnabled);
    }

    public static boolean getOutputTeradataTruncate(Configuration configuration) {
        return configuration.getBoolean(TDCH_OUTPUT_TERADATA_TRUNCATE_TABLE, false);
    }

    public static void setOutputBIDisableFailoverSupport(Configuration configuration, boolean disableFailover) {
        configuration.setBoolean(TDCH_OUTPUT_TERADATA_BI_DISABLE_FAILOVER, disableFailover);
    }

    public static boolean getOutputBIDisableFailoverSupport(Configuration configuration) {
        return configuration.getBoolean(TDCH_OUTPUT_TERADATA_BI_DISABLE_FAILOVER, false);
    }

    public static void setOutputErrorTable1Name(Configuration configuration, String errorTable1Name) {
        configuration.set(TDCH_OUTPUT_TERADATA_ERROR_TABLE1_NAME, errorTable1Name);
    }

    public static String getOutputErrorTable1Name(Configuration configuration) {
        return configuration.get(TDCH_OUTPUT_TERADATA_ERROR_TABLE1_NAME, "");
    }

    public static void setOutputFastloadSocketPort(Configuration configuration, int fastloadSocketPort) {
        configuration.setInt(TDCH_OUTPUT_TERADATA_FASTLOAD_COORDINATOR_SOCKET_PORT, fastloadSocketPort);
    }

    public static int getOutputFastloadSocketPort(Configuration configuration) {
        return configuration.getInt(TDCH_OUTPUT_TERADATA_FASTLOAD_COORDINATOR_SOCKET_PORT, 0);
    }

    public static void setOutputFastloadSocketTimeout(Configuration configuration, long fastloadSocketout) {
        configuration.setLong(TDCH_OUTPUT_TERADATA_FASTLOAD_COORDINATOR_SOCKET_TIMEOUT, fastloadSocketout);
    }

    public static long getOutputFastloadSocketTimeout(Configuration configuration) {
        return configuration.getLong(TDCH_OUTPUT_TERADATA_FASTLOAD_COORDINATOR_SOCKET_TIMEOUT, 480000);
    }

    public static void setOutputFastloadSocketHost(Configuration configuration, String fastloadSocketHost) {
        configuration.set(TDCH_OUTPUT_TERADATA_FASTLOAD_COORDINATOR_SOCKET_HOST, fastloadSocketHost);
    }

    public static String getOutputFastloadSocketHost(Configuration configuration) {
        return configuration.get(TDCH_OUTPUT_TERADATA_FASTLOAD_COORDINATOR_SOCKET_HOST, "");
    }

    public static void setOutputFastloadSocketBacklog(Configuration configuration, int fastloadSocketBacklog) {
        configuration.setInt(TDCH_OUTPUT_TERADATA_FASTLOAD_COORDINATOR_SOCKET_BACKLOG, fastloadSocketBacklog);
    }

    public static int getOutputFastloadSocketBacklog(Configuration configuration) {
        return configuration.getInt(TDCH_OUTPUT_TERADATA_FASTLOAD_COORDINATOR_SOCKET_BACKLOG, TERADATA_QUERY_BAND_VALUE_MAX_LENGTH);
    }

    public static void setOutputErrorTableName(Configuration configuration, String errorTableName) {
        configuration.set(TDCH_OUTPUT_TERADATA_FASTLOAD_ERROR_TABLE_NAME, errorTableName);
    }

    public static String getOutputErrorTableName(Configuration configuration) {
        return configuration.get(TDCH_OUTPUT_TERADATA_FASTLOAD_ERROR_TABLE_NAME, "");
    }

    public static void setOutputErrorTableDatabase(Configuration configuration, String errorTableDatabase) {
        configuration.set(TDCH_OUTPUT_TERADATA_FASTLOAD_ERROR_TABLE_DATABASE, errorTableDatabase);
    }

    public static String getOutputErrorTableDatabase(Configuration configuration) {
        return configuration.get(TDCH_OUTPUT_TERADATA_FASTLOAD_ERROR_TABLE_DATABASE, "");
    }

    public static void setOutputFastloadErrorLimit(Configuration configuration, long errLimit) {
        configuration.setLong(TDCH_OUTPUT_TERADATA_FASTLOAD_ERROR_LIMIT, errLimit);
    }

    public static long getOutputFastloadErrorLimit(Configuration configuration) {
        return configuration.getLong(TDCH_OUTPUT_TERADATA_FASTLOAD_ERROR_LIMIT, 0);
    }

    public static void setInputFastExportSocketPort(Configuration configuration, int port) {
        configuration.setInt(TDCH_INPUT_TERADATA_FASTEXPORT_COORDINATOR_SOCKET_PORT, port);
    }

    public static int getInputFastExportSocketPort(Configuration configuration) {
        return configuration.getInt(TDCH_INPUT_TERADATA_FASTEXPORT_COORDINATOR_SOCKET_PORT, 0);
    }

    public static void setInputFastExportSocketTimeout(Configuration configuration, long timeout) {
        configuration.setLong(TDCH_INPUT_TERADATA_FASTEXPORT_COORDINATOR_SOCKET_TIMEOUT, timeout);
    }

    public static long getInputFastExportSocketTimeout(Configuration configuration) {
        return configuration.getLong(TDCH_INPUT_TERADATA_FASTEXPORT_COORDINATOR_SOCKET_TIMEOUT, 480000);
    }

    public static void setInputFastExportSocketHost(Configuration configuration, String host) {
        configuration.set(TDCH_INPUT_TERADATA_FASTEXPORT_COORDINATOR_SOCKET_HOST, host);
    }

    public static String getInputFastExportSocketHost(Configuration configuration) {
        return configuration.get(TDCH_INPUT_TERADATA_FASTEXPORT_COORDINATOR_SOCKET_HOST, "");
    }

    public static void setInputFastExportSocketBacklog(Configuration configuration, int backlog) {
        configuration.setInt(TDCH_INPUT_TERADATA_FASTEXPORT_COORDINATOR_SOCKET_BACKLOG, backlog);
    }

    public static int getInputFastExportSocketBacklog(Configuration configuration) {
        return configuration.getInt(TDCH_INPUT_TERADATA_FASTEXPORT_COORDINATOR_SOCKET_BACKLOG, TERADATA_QUERY_BAND_VALUE_MAX_LENGTH);
    }

    public static void setInputJdbcDriverClass(Configuration configuration, String jdbcDriverClass) {
        configuration.set(TDCH_INPUT_JDBC_DRIVER_CLASS, jdbcDriverClass);
    }

    public static String getInputJdbcDriverClass(Configuration configuration) {
        return configuration.get(TDCH_INPUT_JDBC_DRIVER_CLASS, "com.teradata.jdbc.TeraDriver");
    }

    public static void setInputJdbcUrl(Configuration configuration, String jdbcUrl) {
        configuration.set(TDCH_INPUT_JDBC_URL, jdbcUrl);
    }

    public static String getInputJdbcUrl(Configuration configuration) {
        return configuration.get(TDCH_INPUT_JDBC_URL, "");
    }

    public static void setInputJdbcUserName(Configuration configuration, String jdbcUserName) {
        configuration.set(TDCH_INPUT_JDBC_USER_NAME, jdbcUserName);
    }

    public static String getInputJdbcUserName(Configuration configuration) {
        return configuration.get(TDCH_INPUT_JDBC_USER_NAME, "");
    }

    public static void setInputJdbcPassword(Configuration configuration, String jdbcPassword) {
        configuration.set(TDCH_INPUT_JDBC_PASSWORD, jdbcPassword);
    }

    public static String getInputJdbcPassword(Configuration configuration) {
        return configuration.get(TDCH_INPUT_JDBC_PASSWORD, "");
    }

    public static void setInputTeradataUserName(JobContext context, byte[] teradataUserName) {
        context.getCredentials().addSecretKey(TDCH_INPUT_TERADATA_USER_NAME, teradataUserName);
    }

    public static byte[] getInputTeradataUserName(JobContext context) {
        return context.getCredentials().getSecretKey(TDCH_INPUT_TERADATA_USER_NAME);
    }

    public static void setInputTeradataPassword(JobContext context, byte[] teradataPassword) {
        context.getCredentials().addSecretKey(TDCH_INPUT_TERADATA_PASSWORD, teradataPassword);
    }

    public static byte[] getInputTeradataPassword(JobContext context) {
        return context.getCredentials().getSecretKey(TDCH_INPUT_TERADATA_PASSWORD);
    }

    public static void setInputSplitSql(Configuration configuration, String splitSql) {
        configuration.set(TDCH_INPUT_TERADATA_SPLIT_SQL, splitSql);
    }

    public static String getInputSplitSql(Configuration configuration) {
        return configuration.get(TDCH_INPUT_TERADATA_SPLIT_SQL, "");
    }

    public static void setInputStageTableEnabled(Configuration configuration, boolean inputStageTableEnabled) {
        configuration.setBoolean(TDCH_INPUT_TERADATA_STAGE_TABLE_ENABLED, inputStageTableEnabled);
    }

    public static boolean getInputStageTableEnabled(Configuration configuration) {
        return configuration.getBoolean(TDCH_INPUT_TERADATA_STAGE_TABLE_ENABLED, false);
    }

    public static void setInputStageAreas(Configuration configuration, String stageTableName) {
        configuration.set(TDCH_INPUT_TERADATA_STAGE_AREAS, stageTableName);
    }

    public static String getInputStageAreas(Configuration configuration) {
        return configuration.get(TDCH_INPUT_TERADATA_STAGE_AREAS, "");
    }

    public static void setInputTableDesc(Configuration configuration, String inputTableDesc) {
        configuration.set(TDCH_INPUT_TERADATA_TABLE_DESC, inputTableDesc);
    }

    public static String getInputTableDesc(Configuration configuration) {
        return configuration.get(TDCH_INPUT_TERADATA_TABLE_DESC, "");
    }

    public static void setInputFinalTable(Configuration configuration, String finalTable) {
        configuration.set(TDCH_INPUT_TERADATA_FINAL_TABLE, finalTable);
    }

    public static String getInputFinalTable(Configuration configuration) {
        return configuration.get(TDCH_INPUT_TERADATA_FINAL_TABLE, "");
    }

    public static void setInputFinalDatabase(Configuration configuration, String finalDatabase) {
        configuration.set(TDCH_INPUT_TERADATA_FINAL_DATABASE, finalDatabase);
    }

    public static String getInputFinalDatabase(Configuration configuration) {
        return configuration.get(TDCH_INPUT_TERADATA_FINAL_DATABASE, "");
    }

    public static void setInputFinalConditions(Configuration configuration, String finalCondition) {
        configuration.set(TDCH_INPUT_TERADATA_FINAL_CONDITION, finalCondition);
    }

    public static String getInputFinalConditions(Configuration configuration) {
        return configuration.get(TDCH_INPUT_TERADATA_FINAL_CONDITION, "");
    }

    public static void setInputFinalQuery(Configuration configuration, String finalQuery) {
        configuration.set(TDCH_INPUT_TERADATA_FINAL_QUERY, finalQuery);
    }

    public static String getInputFinalQuery(Configuration configuration) {
        return configuration.get(TDCH_INPUT_TERADATA_FINAL_QUERY, "");
    }

    public static void setInputQueryBand(Configuration configuration, String inputQueryBand) {
        configuration.set(TDCH_INPUT_TERADATA_QUERY_BAND, inputQueryBand);
    }

    public static String getInputQueryBand(Configuration configuration) {
        return configuration.get(TDCH_INPUT_TERADATA_QUERY_BAND, "");
    }

    public static void setInputFieldNamesArray(Configuration configuration, String[] sourceFieldNamesArray) {
        configuration.set(TDCH_INPUT_TERADATA_FIELD_NAMES, ConnectorSchemaUtils.fieldNamesToJson(ConnectorSchemaUtils.unquoteFieldNamesArray(sourceFieldNamesArray)));
    }

    public static String[] getInputFieldNamesArray(Configuration configuration) {
        return ConnectorSchemaUtils.fieldNamesFromJson(configuration.get(TDCH_INPUT_TERADATA_FIELD_NAMES, ""));
    }

    public static void setInputNumAmps(Configuration configuration, int numAmps) {
        configuration.setInt(TDCH_INPUT_TERADATA_NUM_AMPS, numAmps);
    }

    public static int getInputNumAmps(Configuration configuration) {
        return configuration.getInt(TDCH_INPUT_TERADATA_NUM_AMPS, 2);
    }

    public static void setOutputStageAreas(Configuration configuration, String stageTableName) {
        configuration.set(TDCH_OUTPUT_TERADATA_STAGE_AREAS, stageTableName);
    }

    public static String getOutputStageAreas(Configuration configuration) {
        return configuration.get(TDCH_OUTPUT_TERADATA_STAGE_AREAS, "");
    }

    public static void setOutputStageEnabled(Configuration configuration, boolean stageEnabled) {
        configuration.setBoolean(TDCH_OUTPUT_TERADATA_STAGE_TABLE_ENABLED, stageEnabled);
    }

    public static boolean getOutputStageEnabled(Configuration configuration) {
        return configuration.getBoolean(TDCH_OUTPUT_TERADATA_STAGE_TABLE_ENABLED, false);
    }

    public static void setOutputFieldNamesArray(Configuration configuration, String[] targetFieldNamesArray) {
        configuration.set(TDCH_OUTPUT_TERADATA_FIELD_NAMES, ConnectorSchemaUtils.fieldNamesToJson(ConnectorSchemaUtils.unquoteFieldNamesArray(targetFieldNamesArray)));
    }

    public static String[] getOutputFieldNamesArray(Configuration configuration) {
        return ConnectorSchemaUtils.fieldNamesFromJson(configuration.get(TDCH_OUTPUT_TERADATA_FIELD_NAMES, ""));
    }

    public static void setOutputTableDesc(Configuration configuration, String outputTableDesc) {
        configuration.set(TDCH_OUTPUT_TERADATA_TABLE_DESC, outputTableDesc);
    }

    public static String getOutputTableDesc(Configuration configuration) {
        return configuration.get(TDCH_OUTPUT_TERADATA_TABLE_DESC, "");
    }

    public static void setOutputFinalTable(Configuration configuration, String finalTable) {
        configuration.set(TDCH_OUTPUT_TERADATA_FINAL_TABLE, finalTable);
    }

    public static String getOutputFinalTable(Configuration configuration) {
        return configuration.get(TDCH_OUTPUT_TERADATA_FINAL_TABLE, "");
    }

    public static void setOutputFinalDatabase(Configuration configuration, String finalDatabase) {
        configuration.set(TDCH_OUTPUT_TERADATA_FINAL_DATABASE, finalDatabase);
    }

    public static String getOutputFinalDatabase(Configuration configuration) {
        return configuration.get(TDCH_OUTPUT_TERADATA_FINAL_DATABASE, "");
    }

    public static void setOutputNumAmps(Configuration configuration, int numAmps) {
        configuration.setInt(TDCH_OUTPUT_TERADATA_NUM_AMPS, numAmps);
    }

    public static int getOutputNumAmps(Configuration configuration) {
        return configuration.getInt(TDCH_OUTPUT_TERADATA_NUM_AMPS, 2);
    }

    public static void setOutputJdbcDriverClass(Configuration configuration, String jdbcDriverClass) {
        configuration.set(TDCH_OUTPUT_JDBC_DRIVER_CLASS, jdbcDriverClass);
    }

    public static String getOutputJdbcDriverClass(Configuration configuration) {
        return configuration.get(TDCH_OUTPUT_JDBC_DRIVER_CLASS, "com.teradata.jdbc.TeraDriver");
    }

    public static void setOutputJdbcUrl(Configuration configuration, String jdbcUrl) {
        configuration.set(TDCH_OUTPUT_JDBC_URL, jdbcUrl);
    }

    public static String getOutputJdbcUrl(Configuration configuration) {
        return configuration.get(TDCH_OUTPUT_JDBC_URL, "");
    }

    public static void setOutputJdbcUserName(Configuration configuration, String jdbcUserName) {
        configuration.set(TDCH_OUTPUT_JDBC_USER_NAME, jdbcUserName);
    }

    public static String getOutputJdbcUserName(Configuration configuration) {
        return configuration.get(TDCH_OUTPUT_JDBC_USER_NAME, "");
    }

    public static void setOutputJdbcPassword(Configuration configuration, String jdbcPassword) {
        configuration.set(TDCH_OUTPUT_JDBC_PASSWORD, jdbcPassword);
    }

    public static String getOutputJdbcPassword(Configuration configuration) {
        return configuration.get(TDCH_OUTPUT_JDBC_PASSWORD, "");
    }

    public static void setOutputTeradataUserName(JobContext context, byte[] teradataUserName) {
        context.getCredentials().addSecretKey(TDCH_OUTPUT_TERADATA_USER_NAME, teradataUserName);
    }

    public static byte[] getOutputTeradataUserName(JobContext context) {
        return context.getCredentials().getSecretKey(TDCH_OUTPUT_TERADATA_USER_NAME);
    }

    public static void setOutputTeradataPassword(JobContext context, byte[] teradataPassword) {
        context.getCredentials().addSecretKey(TDCH_OUTPUT_TERADATA_PASSWORD, teradataPassword);
    }

    public static byte[] getOutputTeradataPassword(JobContext context) {
        return context.getCredentials().getSecretKey(TDCH_OUTPUT_TERADATA_PASSWORD);
    }

    public static void setOutputErrorTable2Name(Configuration configuration, String errorTable2Name) {
        configuration.set(TDCH_OUTPUT_TERADATA_ERROR_TABLE2_NAME, errorTable2Name);
    }

    public static String getOutputErrorTable2Name(Configuration configuration) {
        return configuration.get(TDCH_OUTPUT_TERADATA_ERROR_TABLE2_NAME, "");
    }

    public static void setOutputFastloadLsn(Configuration configuration, String fastloadLsnNumber) {
        configuration.set(TDCH_OUTPUT_TERADATA_FASTLOAD_LSN, fastloadLsnNumber);
    }

    public static String getOutputFastloadLsn(Configuration configuration) {
        return configuration.get(TDCH_OUTPUT_TERADATA_FASTLOAD_LSN, "");
    }

    public static void setOutputFastFail(Configuration configuration, boolean b) {
        configuration.setBoolean(TDCH_OUTPUT_TERADATA_FASTLOAD_FASTFAIL, b);
    }

    public static boolean getOutputFastFail(Configuration configuration) {
        return configuration.getBoolean(TDCH_OUTPUT_TERADATA_FASTLOAD_FASTFAIL, false);
    }

    public static void setInputFastExportLsn(Configuration configuration, String lsnNumber) {
        configuration.set(TDCH_INPUT_TERADATA_FASTEXPORT_LSN, lsnNumber);
    }

    public static String getInputFastExportLsn(Configuration configuration) {
        return configuration.get(TDCH_INPUT_TERADATA_FASTEXPORT_LSN, "");
    }

    public static void setInputFastFail(Configuration configuration, boolean b) {
        configuration.setBoolean(TDCH_INPUT_TERADATA_FASTEXPORT_FASTFAIL, b);
    }

    public static boolean getInputFastFail(Configuration configuration) {
        return configuration.getBoolean(TDCH_INPUT_TERADATA_FASTEXPORT_FASTFAIL, false);
    }
}