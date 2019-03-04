package com.teradata.connector.common.exception;

import java.io.*;
import java.util.*;
import java.lang.reflect.*;

import org.apache.hadoop.util.*;

import java.text.*;

public class ConnectorException extends IOException {
    private static final long serialVersionUID = 1L;
    protected static Map<Integer, String> messages;
    protected int code;
    protected String message;

    public static Map<Integer, String> loadMessages(final Class codeClass, final Class messageClass) {
        final Map<Integer, String> messages = new HashMap<Integer, String>();
        try {
            final Field[] fields = codeClass.getDeclaredFields();
            for (int i = 0; i < fields.length; ++i) {
                fields[i].setAccessible(true);
                if ((fields[i].getModifiers() & 0x8) == 0x8) {
                    final int code = (int) fields[i].get(null);
                    final Field field = messageClass.getField(fields[i].getName());
                    field.setAccessible(true);
                    final String message = (String) field.get(null);
                    messages.put(code, message);
                }
            }
        } catch (Exception ex) {
        }
        return messages;
    }

    public ConnectorException(final int code) {
        this.code = code;
        this.message = ConnectorException.messages.get(code);
    }

    public ConnectorException(final String errorMessage) {
        this.message = errorMessage;
        this.code = 1;
    }

    public ConnectorException(final String s, final Throwable e) {
        super(StringUtils.stringifyException(e));
        if (e instanceof ConnectorException) {
            this.code = ((ConnectorException) e).getCode();
            this.message = ((ConnectorException) e).getMessage();
        } else {
            this.code = 10000;
            this.message = StringUtils.stringifyException(e);
        }
    }

    public ConnectorException(final int code, final Object... args) {
        this.code = code;
        this.message = MessageFormat.format(ConnectorException.messages.get(code), args);
    }

    public int getCode() {
        return this.code;
    }

    public void setCode(final int code) {
        this.code = code;
    }

    @Override
    public String getMessage() {
        return this.message;
    }

    public void setMessage(final String message) {
        this.message = message;
    }

    static {
        (ConnectorException.messages = new HashMap<Integer, String>()).putAll(loadMessages(ErrorCode.class, ErrorMessage.class));
    }

    public static class ErrorCode {
        public static final int UNKNOW_ERROR = 10000;
        public static final int CONVERTER_CONSTRUCTOR_NOT_MATCHING = 11001;
        public static final int CONVERTER_STRING_TRUNCATE_ERROR = 11002;
        public static final int INPUT_ARGS_INVALID = 12001;
        public static final int INPUT_JOB_TYPE_UNSUPPORTED = 12002;
        public static final int INPUT_TARGET_PATH_MISSING = 12003;
        public static final int INPUT_TARGET_TABLE_NAME_MISSING = 12004;
        public static final int INPUT_SOURCE_TABLE_NAME_MISSING = 12005;
        public static final int INPUT_STAGE_TABLE_NAME_MISSING = 12006;
        public static final int INPUT_TABLE_QUERY_BOTH_MISSING = 12007;
        public static final int INPUT_TABLE_QUERY_BOTH_PRESENT = 12008;
        public static final int INPUT_SPLIT_METHOD_UNSUPPORTED = 12009;
        public static final int INPUT_METHOD_INVALID = 12010;
        public static final int INPUT_METHOD_SPLIT_BY_AMP_SUPPORTS_TABLE_ONLY = 12011;
        public static final int INPUT_SOURCE_TABLE_LENGTH_EXCEED_LIMIT = 12012;
        public static final int INPUT_STAGE_TABLE_LENGTH_EXCEED_LIMIT = 12013;
        public static final int INPUT_SPLIT_BY_AMP_DB_VERSION_UNSUPPORTED = 12014;
        public static final int INPUT_SPLIT_BY_AMP_VIEW_UNSUPPORTED = 12015;
        public static final int INPUT_FILE_FORMAT_UNSUPPORTED = 12016;
        public static final int INPUT_SOURCE_QUERY_ONLY_FOR_BY_PARTITION = 12017;
        public static final int INPUT_NUMBER_PARTITONS_IN_STAGING_PARAMETERS_INVALID = 12018;
        public static final int INPUT_NUMBER_MAPPERS_PARAMETERS_INVALID = 12019;
        public static final int INPUT_TABLE_IS_NOT_PPI_TABLE = 12020;
        public static final int INPUT_ARGS_MISSING = 12021;
        public static final int INPUT_TABLE_IS_EMPTY = 12022;
        public static final int OUTPUT_ARGS_INVALID = 13001;
        public static final int OUTPUT_JOB_TYPE_UNSUPPORTED = 13002;
        public static final int OUTPUT_METHOD_MISSING = 13003;
        public static final int OUTPUT_METHOD_INVALID = 13004;
        public static final int OUTPUT_SOURCE_PATH_MISSING = 13005;
        public static final int OUTPUT_SOURCE_TABLE_NAME_MISSING = 13006;
        public static final int OUTPUT_TARGET_TABLE_NAME_MISSING = 13007;
        public static final int OUTPUT_STAGE_TABLE_NAME_MISSING = 13008;
        public static final int OUTPUT_FIELD_NAME_COUNT_BOTH_MISSING = 13009;
        public static final int OUTPUT_FIELD_COUNT_MISMATCH = 13010;
        public static final int OUTPUT_RUN_PHASE_CONFUSED = 13011;
        public static final int OUTPUT_TARGET_TABLE_LENGTH_EXCEED_LIMIT = 13012;
        public static final int OUTPUT_TARGET_TABLE_LENGTH_EXCEED_INTERNAL_FASTLOAD_METHOD_LIMIT = 13013;
        public static final int OUTPUT_STAGE_TABLE_LENGTH_EXCEED_LIMIT = 13014;
        public static final int OUTPUT_TARGET_FIELD_NAMES_MISSING_PI = 13015;
        public static final int OUTPUT_FILE_FORMAT_UNSUPPORTED = 13016;
        public static final int OUTPUT_TARGET_TABLE_NAME_INVALID = 13017;
        public static final int OUTPUT_ERROR_TABLE_LENGTH_EXCEED_LIMIT = 13018;
        public static final int OUTPUT_NUMBER_MAPPERS_PARAMETERS_INVALID = 13019;
        public static final int OUTPUT_ARGS_MISSING = 13020;
        public static final int SCHEMA_COLUMN_DEFINITION_MISSING = 14001;
        public static final int SCHEMA_PARTITION_DEFINITION_MISSING = 14002;
        public static final int SCHEMA_PARTITION_UNSUPPORTED = 14003;
        public static final int FIELD_NAME_MISSING = 14004;
        public static final int FIELD_NAME_NOT_IN_SCHEMA = 14005;
        public static final int FIELD_DATATYPE_UNSUPPORTED = 14006;
        public static final int FIELD_DATATYPE_CONVERSION_UNSUPPORTED = 14007;
        public static final int FIELD_DATATYPE_CONVERSION_INCORRECT = 14008;
        public static final int SOURCE_TARGET_FIELD_COUNT_MISMATCH = 14009;
        public static final int SOURCE_FIELD_MISSING = 14010;
        public static final int TARGET_FIELD_MISSING = 14011;
        public static final int SCHEMA_DEFINITION_INVALID = 14012;
        public static final int COLUMN_LENGTH_OF_SOURCE_RECORD_SCHEMA_MISMATCH_LENGTH_OF_FIELD_NAMES = 14013;
        public static final int COLUMN_LENGTH_OF_TARGET_RECORD_SCHEMA_MISMATCH_LENGTH_OF_FIELD_NAMES = 14014;
        public static final int COLUMN_TYPE_OF_SOURCE_RECORD_SCHEMA_MISMATCH_FIELD_TYPE_IN_SCHEMA = 14015;
        public static final int COLUMN_TYPE_OF_TARGET_RECORD_SCHEMA_MISMATCH_FIELD_TYPE_IN_SCHEMA = 14016;
        public static final int COLUMN_LENGTH_OF_SOURCE_RECORD_SCHEMA_MISMATH_LENGTH_OF_TARGET_RECORD_SCHEMA = 14017;
        public static final int TARGET_RECORD_SCHEMA_IS_NULL = 14018;
        public static final int UDF_CONVERTER_SHOULD_NOT_APPEAR_IN_TARGET_RECORD_SCHEMA = 14019;
        public static final int PATH_NOT_A_FOLDER = 15001;
        public static final int PATH_INCREMENT_EXCEEDS_RANGE = 15002;
        public static final int OUPUT_STAGE_TABLE_NAME_INCLUDED_DATABASE_NAME_DIFFERS_WITH_STAGE_DATABASE_NAME = 15003;
        public static final int MALFORMED_UNICODE_ENCODING_STRING = 15004;
        public static final int STAGING_TABLES_MISSING = 15005;
        public static final int INVALID_JOB_CLIENT_OUTPUT_FILE_SCHEMA = 15006;
        public static final int JOB_CLIENT_OUTPUT_FILE_EXISTS = 15007;
        public static final int UNRECOGNIZED_INPUT_PARAMETERS = 15008;
        public static final int NUMBER_FORMAT_EXCEPTION = 15009;
        public static final int PARTITION_COLUMN_VALUE_NULL = 15010;
        public static final int PLUGEDIN_CONFIGURATION_INVALID = 15011;
        public static final int INDEX_OUT_OF_BOUNDARY = 15012;
        public static final int INVALID_VALUE_PROVIDED_UDF = 15013;
        public static final int UNRECOG_NODE_TAG = 15014;
        public static final int NEED_NODE_TAG_VALUE = 15015;
        public static final int PLUGIN_ALREADY_EXIST = 15016;
        public static final int PLUGIN_EXPECTED_TAG = 15017;
        public static final int REQUIRED_MISSING_TAG_VALUE = 15018;
        public static final int PLUGIN_FILE_NOT_FOUND = 15019;
        public static final int PLUGIN_NO_CHILDREN = 15020;
        public static final int INCORRECT_PLUGIN_CONF = 15021;
        public static final int CONNECTOR_PLUGIN_NOT_FOUND = 15022;
        public static final int CONNECTOR_PLUGIN_CONF_FILE_NOT_FOUND = 15023;
        public static final int CONNECTOR_PLUGIN_NOT_CONF_FILE = 15024;
        public static final int CONNECTOR_PLUGIN_NOT_EXPECTED_SOURCE_TYPE = 15025;
        public static final int CONNECTOR_PLUGIN_MISSING = 15026;
        public static final int CONNECTOR_CONVERTER_EXISTS = 15027;
        public static final int CONNECTOR_INVALID_OBJECT = 15028;
        public static final int PATH_PROVIDED_FOR_OTHER_JOB_TYPE = 15029;
        public static final int DATABASE_PROVIDED_FOR_OTHER_JOB_TYPE = 15030;
        public static final int TABLE_PROVIDED_FOR_OTHER_JOB_TYPE = 15031;
        public static final int TABLE_SCHEMA_PROVIDED_FOR_OTHER_JOB_TYPE = 15032;
        public static final int SEPARATOR_PROVIDED_FOR_OTHER_JOB_TYPE = 15033;
        public static final int NULL_STRING_PROVIDED_FOR_OTHER_JOB_TYPE = 15034;
        public static final int WALLET_UNSUPPORTED_OS = 15035;
        public static final int WALLET_UNSUPPORTED_ARCH = 15036;
        public static final int GET_RESOURCE_AS_STREAM_FAIL = 15037;
        public static final int GETPROPERTY_OS_NAME_FAIL = 15038;
        public static final int GETPROPERTY_OS_ARCH_FAIL = 15039;
        public static final int GETJRE_EXECUTABLE_FAIL = 15040;
        public static final int UNSUPPORTED_PROTOCOL = 15041;
        public static final int MAPLIBRARYNAME_FAIL = 15042;
        public static final int GETPROPERTY_JAVA_IO_TMPDIR_FAIL = 15043;
        public static final int GETPROPERTY_FILE_SEPARATOR_FAIL = 15044;
        public static final int VMID_FAIL = 15045;
        public static final int MAKE_PRIVATE_DIRECTORY_FAIL = 15046;
        public static final int GETPROPERTY_PATH_SEPARATOR_FAIL = 15047;
        public static final int NO_CONTAINERS_AVAILABLE_FOR_QUEUE = 15048;
        public static final int MIN_CONTAINERS_UNAVAILABLE_FOR_QUEUE = 15049;
        public static final int INVALID_RETRY_VALUE_SPECIFIED = 15050;
        public static final int INPUT_PRE_PROCESSOR_FAILED = 16001;
        public static final int OUTPUT_PRE_PROCESSOR_FAILED = 16002;
        public static final int INPUT_POST_PROCESSOR_FAILED = 16003;
        public static final int OUTPUT_POST_PROCESSOR_FAILED = 16004;
        public static final int JDBC_DRIVER_UNSUPPORTED = 20001;
        public static final int JDBC_DRIVER_CLASSNAME_MISSING = 20002;
        public static final int JDBC_URL_MISSING = 20003;
        public static final int JDBC_URL_INVALID = 20004;
        public static final int JDBC_USERNAME_MISSING = 20005;
        public static final int JDBC_PASSWORD_MISSING = 20006;
        public static final int JDBC_CONNECTION_TYPE_INVALID = 20007;
        public static final int TABLE_NOT_FOUND_IN_TERADATA = 20008;
        public static final int NOT_FOUND_PARTITION_IDS_IN_SOURCE_TABLE = 20009;
        public static final int DB_UNSUPPORTED = 200010;
        public static final int QUERY_BAND_PROPERTY_LENGTH_EXCEEDS_LIMIT = 21001;
        public static final int QUERY_BAND_NAME_PROPERTY_ALREADY_EXISTS = 21002;
        public static final int QUERY_BAND_NAME_LENGTH_EXCEEDS_LIMIT = 21003;
        public static final int QUERY_BAND_VALUE_LENGTH_EXCEEDS_LIMIT = 21004;
        public static final int SEMICOLON_NUMBER_DIFFERENT_WITH_EQUAL_NUMBER = 21005;
        public static final int FASTLOAD_INVALID_CMD = 22001;
        public static final int FASTLOAD_FAILED = 22002;
        public static final int FASTLOAD_TIMEOUT = 22003;
        public static final int FASTLOAD_CLUSTER_MAPPER_NOT_ENOUGH = 22004;
        public static final int FASTLOAD_CLUSTER_REDUCER_NOT_ENOUGH = 22005;
        public static final int FASTLOAD_DATABASE_AMP_NOT_ENOUGH = 22006;
        public static final int FASTLOAD_NO_AVAILABLE_PORT = 22007;
        public static final int FASTLOAD_NO_IPV4_INTERFACE = 22008;
        public static final int FASTLOAD_NO_NETWORK_INTERFACE = 22009;
        public static final int BATCH_INSERT_FAILED = 22010;
        public static final int BATCH_INSERT_FAILED_TASK_RESTART = 22011;
        public static final int FASTLOAD_EMPTY_LSN = 22012;
        public static final int FASTLOAD_ERRLIMIT_EXCEED = 22013;
        public static final int FASTLOAD_NO_TRACKER_REACHABLE = 22014;
        public static final int FASTLOAD_ERROR_PARSING_TRACKER_NAMES = 22015;
        public static final int FASTLOAD_STAGETABLE_TO_TARGETTABLE_FAILED = 22016;
        public static final int FASTEXPORT_INVALID_CMD = 22101;
        public static final int FASTEXPORT_FAILED = 22102;
        public static final int FASTEXPORT_TIMEOUT = 22103;
        public static final int FASTEXPORT_EMPTY_LSN = 22112;
        public static final int SPLIT_COLUMN_DATATYPE_UNSUPPORTED = 23001;
        public static final int SPLIT_COLUMN_MISSING = 23002;
        public static final int SPLIT_COLUMN_MIN_MAX_VALUE_EMPTY = 23003;
        public static final int SPLIT_COLUMN_MIN_MAX_VALUE_NULL = 23004;
        public static final int SPLIT_SQL_MISSING = 23005;
        public static final int FASTLOAD_LSN_FILE_ALREADY_EXISTS = 24001;
        public static final int FASTEXPORT_LSN_FILE_ALREADY_EXISTS = 24001;
        public static final int FASTEXPORT_PARAMS_FILE_ALREADY_EXISTS = 24002;
        public static final int OUTPUT_PATH_TABLE_BOTH_PRESENT = 30001;
        public static final int OUTPUT_PATH_TABLE_BOTH_MISSING = 30002;
        public static final int OUTPUT_COLUMN_SCHEMA_MISSING = 30003;
        public static final int OUTPUT_FIELD_NAMES_NOT_INCLUDE_PARTITION_COLUMNS = 30004;
        public static final int OUTPUT_TABLE_SCHEMA_BOTH_PRESENT = 30005;
        public static final int OUTPUT_TABLE_SCHEMA_CONTAIN_PARTITION_SCHEMA = 30006;
        public static final int HIVE_CONF_FILE_NOT_FOUND = 30007;
        public static final int HIVE_CONF_FILE_IS_DIR = 30008;
        public static final int INPUT_PATH_TABLE_BOTH_PRESENT = 31001;
        public static final int INPUT_PATH_TABLE_BOTH_MISSING = 31002;
        public static final int INPUT_COLUMN_SCHEMA_MISSING = 31003;
        public static final int INPUT_PARTITION_SCHEMA_MISSING = 31004;
        public static final int INPUT_TABLE_SCHEMA_BOTH_PRESENT = 310005;
        public static final int HIVE_TABLE_INVALID = 32001;
        public static final int HIVE_TABLE_INPUTFORMAT_UNSUPPORTED = 32003;
        public static final int HIVE_TABLE_OUTPUTFORMAT_UNSUPPORTED = 32004;
        public static final int HIVE_TABLE_SERDELIB_UNSUPPORTED = 32005;
        public static final int HIVE_COMMAND_NEED_RETRY = 32006;
        public static final int HIVE_THRIFT_EXCEPTION = 32007;
        public static final int HIVE_TABLE_EMPTY = 32008;
        public static final int HIVE_CURRENT_USER_DIRECTORY_NOT_EXISTS = 32009;
        public static final int HIVE_CONF_FILE_BAD_FORMAT = 32010;
        public static final int HIVE_PROVIDED_DIRECTORY_INVALID = 32011;
        public static final int HIVE_PROVIDED_PATH_NOT_EXISTED = 32012;
        public static final int HIVE_PROVIDED_FILTER_INVALID = 32013;
        public static final int HIVE_NOT_SUPPORT_OUTPUTFORMAT = 32014;
        public static final int FIELD_NAME_NOT_IN_HIVE_TABLE = 33001;
        public static final int INPUT_SOURCE_TABLE_MISSING = 40001;
        public static final int OUTPUT_TARGET_TABLE_MISSING = 40002;
        public static final int AVRO_SCHEMA_FILE_NOT_FOUND = 50001;
        public static final int AVRO_SCHEMA_FILE_IS_DIR = 50002;
        public static final int OUTPUT_AVRO_DUPLICATE_SCHEMA = 50003;
        public static final int INPUT_AVRO_DUPLICATE_SCHEMA = 500004;
        public static final int AVRO_NO_MAPPING_SCHEMA_FOUND = 50005;
        public static final int AVRO_FIELD_NAME_NOT_IN_SCHEMA = 50006;
        public static final int AVRO_UNION_TYPE_CAST_FAILED = 50007;
        public static final int HDFS_TEXTFILE_SOURCE_RECORD_AND_TABLE_SCHEMA_BOTH_REQUIRED = 50008;
        public static final int HDFS_TEXTFILE_FIELD_NAME_AND_TABLE_SCHEMA_BOTH_REUIRED = 50009;
        public static final int HDFS_INPUT_OBJECT_IS_NOT_TEXT = 50010;
        public static final int AVRO_SCHEMA_INVALID = 50011;
        public static final int AVRO_UNRECOG_TYPE = 50012;
        public static final int HDFS_INPUT_PATH_EMPTY = 50013;
        public static final int INVALID_IDATASTREAM_SOCKET_HOST = 60001;
        public static final int INVALID_IDATASTREAM_SOCKET_PORT = 60002;
        public static final int INVALID_IDATASTREAM_FIELD_NAMES_ARRAY = 60003;
        public static final int INVALID_IDATASTREAM_FIELD_TYPES_ARRAY = 60004;
        public static final int UNSUPPORTED_IDATASTREAM_FIELD_TYPE = 60005;
        public static final int IDATASTREAM_JOB_FAILED = 60006;
        public static final int IDATASTREAM_DATA_CONV_FAILED = 60007;
        public static final int OUTPUT_HIVE_PARTITION = 15099;
        public static final int HIVE_PARTITION_INPUT_PATH_EMPTY = 33002;
    }

    public static class ErrorMessage {
        public static final String UNKNOW_ERROR = "Unknow error.";
        public static final String INPUT_ARGS_MISSING = "Import tool parameters is not provided";
        public static final String INPUT_ARGS_INVALID = "Import tool parameters is invalid";
        public static final String INPUT_JOB_TYPE_UNSUPPORTED = "Import job type is invalid";
        public static final String INPUT_TARGET_PATH_MISSING = "Import target path is not provided";
        public static final String INPUT_TARGET_TABLE_NAME_MISSING = "Import target table name is not provided";
        public static final String INPUT_SOURCE_TABLE_NAME_MISSING = "Import source table name is not provided";
        public static final String INPUT_STAGE_TABLE_NAME_MISSING = "Import stage table name is not provided";
        public static final String INPUT_TABLE_QUERY_BOTH_MISSING = "Neither query nor table name is provided";
        public static final String INPUT_TABLE_QUERY_BOTH_PRESENT = "Either query or table name should be provided but not both";
        public static final String INPUT_SPLIT_METHOD_UNSUPPORTED = "Unsupported input split method is invoked";
        public static final String INPUT_METHOD_INVALID = "Import method was invalid, should be one of 'split.by.hash','split.by.value','split.by.partition','split.by.amp'";
        public static final String INPUT_METHOD_SPLIT_BY_AMP_SUPPORTS_TABLE_ONLY = "Import method split.by.amp supports table only";
        public static final String INPUT_SOURCE_TABLE_LENGTH_EXCEED_LIMIT = "The import source table name exceeds database length limit of {0}";
        public static final String INPUT_STAGE_TABLE_LENGTH_EXCEED_LIMIT = "The import stage table name length exceeds database length limit of {0}";
        public static final String INPUT_SPLIT_BY_AMP_DB_VERSION_UNSUPPORTED = "split.by.amp method supports Teradata 14.10 database and later versions";
        public static final String INPUT_SPLIT_BY_AMP_VIEW_UNSUPPORTED = "split.by.amp supports table only";
        public static final String INPUT_FILE_FORMAT_UNSUPPORTED = "Import file format is invalid";
        public static final String INPUT_SOURCE_QUERY_ONLY_FOR_BY_PARTITION = "Query-based import(-sourcequery) is only supported by \"split.by.partition\"";
        public static final String INPUT_NUMBER_PARTITONS_IN_STAGING_PARAMETERS_INVALID = "Invalid provided value for parameter 'numPartitionsInStaging', requires an integer greater than 0";
        public static final String INPUT_NUMBER_MAPPERS_PARAMETERS_INVALID = "Invalid provided value for parameter 'numMappers', require an integer greater than 0";
        public static final String INPUT_TABLE_IS_NOT_PPI_TABLE = "Input source table is not a ppi table";
        public static final String INPUT_TABLE_IS_EMPTY = "Input source table is empty";
        public static final String OUTPUT_ARGS_MISSING = "Export tool parameters is not provided";
        public static final String OUTPUT_ARGS_INVALID = "Export tool parameters is invalid";
        public static final String OUTPUT_JOB_TYPE_UNSUPPORTED = "Export job type is invalid";
        public static final String OUTPUT_METHOD_MISSING = "Export method is not provided";
        public static final String OUTPUT_METHOD_INVALID = "Export method is invalid, should be one of 'batch.insert','fastload','internal.fastload'";
        public static final String OUTPUT_SOURCE_PATH_MISSING = "Export source path is not provided";
        public static final String OUTPUT_SOURCE_TABLE_NAME_MISSING = "Export source table name is not provided";
        public static final String OUTPUT_TARGET_TABLE_NAME_MISSING = "Export target table name is not provided";
        public static final String OUTPUT_STAGE_TABLE_NAME_MISSING = "Export stage table name is not provided";
        public static final String OUTPUT_FIELD_NAME_COUNT_BOTH_MISSING = "Neither field count nor field names is provided";
        public static final String OUTPUT_FIELD_COUNT_MISMATCH = "Field count does not match output table column count";
        public static final String OUTPUT_RUN_PHASE_CONFUSED = "TeradataOutputFormat can only run in either mapper or reducer phase";
        public static final String OUTPUT_TARGET_TABLE_LENGTH_EXCEED_LIMIT = "The export target table name exceeds database length limit of {0}";
        public static final String OUTPUT_TARGET_TABLE_LENGTH_EXCEED_INTERNAL_FASTLOAD_METHOD_LIMIT = "Output target table name exceeds internal fastload method limit, please provide a stage table name";
        public static final String OUTPUT_STAGE_TABLE_LENGTH_EXCEED_LIMIT = "The export stage table name length exceeds database length limit of {0}";
        public static final String OUTPUT_TARGET_FIELD_NAMES_MISSING_PI = "Export target field names missing primary index for PI table";
        public static final String OUTPUT_FILE_FORMAT_UNSUPPORTED = "Export file format is invalid";
        public static final String OUTPUT_TARGET_TABLE_NAME_INVALID = "Export target table name is invalid";
        public static final String OUTPUT_ERROR_TABLE_LENGTH_EXCEED_LIMIT = "After concatenating error table extensions to user-supplied value, the error table name length exceeds database length limit of {0}";
        public static final String OUTPUT_NUMBER_MAPPERS_PARAMETERS_INVALID = "Invalid provided value for parameter 'numMappers', require an integer greater than 0";
        public static final String SCHEMA_DEFINITION_INVALID = "Schema definition is invalid";
        public static final String SCHEMA_COLUMN_DEFINITION_MISSING = "Column definition is not provided";
        public static final String SCHEMA_PARTITION_DEFINITION_MISSING = "Partition definition is not provided";
        public static final String SCHEMA_PARTITION_UNSUPPORTED = "Partition is not supported for this configuration";
        public static final String FIELD_NAME_MISSING = "Field name is empty or not provided";
        public static final String FIELD_NAME_NOT_IN_SCHEMA = "Field name is not found in schema";
        public static final String FIELD_DATATYPE_UNSUPPORTED = "{0} Field data type is not supported";
        public static final String FIELD_DATATYPE_CONVERSION_UNSUPPORTED = "Field data type is not convertible {0}";
        public static final String FIELD_DATATYPE_CONVERSION_INCORRECT = "Field data type conversion is incorrect";
        public static final String SOURCE_TARGET_FIELD_COUNT_MISMATCH = "Source and target field count are different";
        public static final String SOURCE_FIELD_MISSING = "Source fields are missing";
        public static final String TARGET_FIELD_MISSING = "Target fields are missing";
        public static final String SCHEMA_FIELD_COUNT_MISMATCH = "Source record schema and source schema mismatch";
        public static final String SCHEMA_DATA_TYPE_INCONSISTENT = "Data type inconsistent";
        public static final String COLUMN_LENGTH_OF_SOURCE_RECORD_SCHEMA_MISMATCH_LENGTH_OF_FIELD_NAMES = "column number defined in source record schema mismatch column number defined in source table names";
        public static final String COLUMN_LENGTH_OF_TARGET_RECORD_SCHEMA_MISMATCH_LENGTH_OF_FIELD_NAMES = "column number defined in target record schema mismatch column number defined in target field names";
        public static final String COLUMN_TYPE_OF_SOURCE_RECORD_SCHEMA_MISMATCH_FIELD_TYPE_IN_SCHEMA = "source record column type mismatch field type defined in schema";
        public static final String COLUMN_TYPE_OF_TARGET_RECORD_SCHEMA_MISMATCH_FIELD_TYPE_IN_SCHEMA = "target record column type mismatch field type defined in schema";
        public static final String COLUMN_LENGTH_OF_SOURCE_RECORD_SCHEMA_MISMATH_LENGTH_OF_TARGET_RECORD_SCHEMA = "source record schema and target record schema mismatch";
        public static final String TARGET_RECORD_SCHEMA_IS_NULL = "target record schema is null";
        public static final String UDF_CONVERTER_SHOULD_NOT_APPEAR_IN_TARGET_RECORD_SCHEMA = "user defined converters should not appear in target record schema";
        public static final String NUMBER_FORMAT_EXCEPTION = "Invalid number format";
        public static final String PATH_NOT_A_FOLDER = "Path is not a folder";
        public static final String PATH_INCREMENT_EXCEEDS_RANGE = "Path increment exceeds range";
        public static final String OUPUT_STAGE_TABLE_NAME_INCLUDED_DATABASE_NAME_DIFFERS_WITH_STAGE_DATABASE_NAME = "stage database name included in output stage table name not equals to input stage database name ";
        public static final String MALFORMED_UNICODE_ENCODING_STRING = "Malformed \\uxxxx encoding";
        public static final String STAGING_TABLES_MISSING = "No staging tables are found in the database";
        public static final String INVALID_JOB_CLIENT_OUTPUT_FILE_SCHEMA = "input URI schema is not supported. Only support hdfs:// and maprfs://";
        public static final String JOB_CLIENT_OUTPUT_FILE_EXISTS = "the output log file already exsits";
        public static final String PLUGEDIN_CONFIGURATION_INVALID = "job is invalid, please make sure all required plugedIn are provided";
        public static final String UNRECOGNIZED_INPUT_PARAMETERS = "unrecognized input parameter(s): {0}";
        public static final String PARTITION_COLUMN_VALUE_NULL = "partition columns cannot have NULL or empty value";
        public static final String INDEX_OUT_OF_BOUNDARY = "index outof boundary";
        public static final String INVALID_VALUE_PROVIDED_UDF = "Invalid value provided for UDF";
        public static final String UNRECOG_NODE_TAG = "unrecognized node tag: <{0}>";
        public static final String NEED_NODE_TAG_VALUE = "need a value for tag: <{0}>";
        public static final String PLUGIN_ALREADY_EXIST = "Connector plugin {0} already exists";
        public static final String PLUGIN_EXPECTED_TAG = "expected tag: <{0}>";
        public static final String REQUIRED_MISSING_TAG_VALUE = "missing required tag value";
        public static final String PLUGIN_FILE_NOT_FOUND = "connector plugins configuration file \"{0}\" was not found";
        public static final String PLUGIN_NO_CHILDREN = "<{0}> needs children nodes";
        public static final String INCORRECT_PLUGIN_CONF = "bad configuration for {0}";
        public static final String CONNECTOR_PLUGIN_NOT_FOUND = "plugin \"{0}\" not found";
        public static final String CONNECTOR_PLUGIN_CONF_FILE_NOT_FOUND = "plugin configuration file does not exist :{0}";
        public static final String CONNECTOR_PLUGIN_NOT_CONF_FILE = "plugin configuration file is not a file: {0}";
        public static final String CONNECTOR_PLUGIN_NOT_EXPECTED_SOURCE_TYPE = "unrecognized input source for plugin configuration file: {0}";
        public static final String CONNECTOR_PLUGIN_MISSING = "missing source or target plugin name";
        public static final String CONNECTOR_CONVERTER_EXISTS = "converter has already exsited";
        public static final String CONNECTOR_INVALID_OBJECT = "invalid object type \"{0}\"";
        public static final String NO_CONTAINERS_AVAILABLE_FOR_QUEUE = "The {0} queue with which this job was associated is full; resubmit job at a later time";
        public static final String MIN_CONTAINERS_UNAVAILABLE_FOR_QUEUE = "The minimum number of mappers ({0}) requested was unavailable in the {1} queue after {2} retries (retries submitted {3} seconds apart); resubmit job at a later time";
        public static final String INVALID_RETRY_VALUE_SPECIFIED = "A positive non-zero integer should be specified for the tdch.throttle.num.mappers.retry.* properties";
        public static final String PATH_PROVIDED_FOR_OTHER_JOB_TYPE = "user provided path for job types excepts Hive and HDFS";
        public static final String DATABASE_PROVIDED_FOR_OTHER_JOB_TYPE = "user provided database for job types excepts Hive and HCat";
        public static final String TABLE_PROVIDED_FOR_OTHER_JOB_TYPE = "user provided table for job types excepts Hive and HCat";
        public static final String TABLE_SCHEMA_PROVIDED_FOR_OTHER_JOB_TYPE = "user provided table schema for job types excepts Hive and HDFS";
        public static final String SEPARATOR_PROVIDED_FOR_OTHER_JOB_TYPE = "user provided separator for job types excepts Hive and HDFS";
        public static final String NULL_STRING_PROVIDED_FOR_OTHER_JOB_TYPE = "user provided null string for job types excepts Hive and HDFS";
        public static final String WALLET_UNSUPPORTED_OS = "The Teradata Connector for Hadoop does not support using Teradata Wallet on the {0} operating system.";
        public static final String WALLET_UNSUPPORTED_ARCH = "The Teradata Connector for Hadoop does not support using Teradata Wallet on the {0} architecture.";
        public static final String GET_RESOURCE_AS_STREAM_FAIL = "Cannot find the resource named {0}.";
        public static final String GETPROPERTY_OS_NAME_FAIL = "Cannot determine the operating system name.";
        public static final String GETPROPERTY_OS_ARCH_FAIL = "Cannot determine the architecture name.";
        public static final String GETJRE_EXECUTABLE_FAIL = "Cannot find the java executable.";
        public static final String UNSUPPORTED_PROTOCOL = "The class loader returned a URL having an unsupported protocol.";
        public static final String MAPLIBRARYNAME_FAIL = "Cannot map the \"{0}\" library name into a platform-specific string representing a native library.";
        public static final String GETPROPERTY_JAVA_IO_TMPDIR_FAIL = "Cannot determine the temporary directory name.";
        public static final String GETPROPERTY_FILE_SEPARATOR_FAIL = "Cannot determine the file separator.";
        public static final String VMID_FAIL = "Cannot get virtual machine unique identifier.";
        public static final String MAKE_PRIVATE_DIRECTORY_FAIL = "Cannot create directory named {0}.";
        public static final String GETPROPERTY_PATH_SEPARATOR_FAIL = "Cannot determine the path separator.";
        public static final String INPUT_PRE_PROCESSOR_FAILED = "Input PreProcessor fails";
        public static final String OUTPUT_PRE_PROCESSOR_FAILED = "Output PreProcessor fails";
        public static final String INPUT_POST_PROCESSOR_FAILED = "Input PostProcessor fails";
        public static final String OUTPUT_POST_PROCESSOR_FAILED = "Output PostProcessor fails";
        public static final String CONVERTER_CONSTRUCTOR_NOT_MATCHING = "can't find matching constructor in user defined data type converter";
        public static final String CONVERTER_STRING_TRUNCATE_ERROR = "String would be truncated";
        public static final String INVALID_INPUT_PARAMETERS = "invalid provided value for parameter %s, %s value is required for this parameter";
        public static final String DB_UNSUPPORTED = "Teradata database 13.0 and above is required";
        public static final String JDBC_DRIVER_UNSUPPORTED = "Teradata JDBC Driver 13.0 and above is required";
        public static final String JDBC_DRIVER_CLASSNAME_MISSING = "JDBC Driver class name is not provided";
        public static final String JDBC_URL_MISSING = "JDBC URL is not provided";
        public static final String JDBC_URL_INVALID = "JDBC URL is invalid";
        public static final String JDBC_USERNAME_MISSING = "JDBC username is not provided";
        public static final String JDBC_PASSWORD_MISSING = "JDBC password is not provided";
        public static final String JDBC_CONNECTION_TYPE_INVALID = "JDBC connection type is invalid";
        public static final String TABLE_NOT_FOUND_IN_TERADATA = "Table was not found, either it does not exist, or is not a table, or there might be a permission issue";
        public static final String NOT_FOUND_PARTITION_IDS_IN_SOURCE_TABLE = "not find partition IDs in source table";
        public static final String SEMICOLON_NUMBER_DIFFERENT_WITH_EQUAL_NUMBER = "Customer input query band property is invalid,number of semicolons should be same with number of equals";
        public static final String QUERY_BAND_PROPERTY_LENGTH_EXCEEDS_LIMIT = "Query band property exceeds 2048 characters length limit";
        public static final String QUERY_BAND_NAME_PROPERTY_ALREADY_EXISTS = "Provided same query band name twice in query band property";
        public static final String QUERY_BAND_NAME_LENGTH_EXCEEDS_LIMIT = "Query band name exceeds 128 characters length limit";
        public static final String QUERY_BAND_VALUE_LENGTH_EXCEEDS_LIMIT = "Query band value exceeds 256 characters length limit";
        public static final String FASTLOAD_EMPTY_LSN = "Fastload LSN is empty";
        public static final String FASTLOAD_INVALID_CMD = "Expected fastload transfer begin command, but got error";
        public static final String FASTLOAD_FAILED = "Fastload job failed";
        public static final String FASTLOAD_TIMEOUT = "Internal fast load socket server time out";
        public static final String FASTLOAD_CLUSTER_MAPPER_NOT_ENOUGH = "Cluster mapper resource is not enough to run export job with method internal.fastload";
        public static final String FASTLOAD_CLUSTER_REDUCER_NOT_ENOUGH = "Cluster reducer resource is not enough to run export job with method internal.fastload";
        public static final String FASTLOAD_DATABASE_AMP_NOT_ENOUGH = "Database AMP resource is not enough to run export job with method internal.fastload";
        public static final String FASTLOAD_NO_AVAILABLE_PORT = "Can not get an unused port";
        public static final String FASTLOAD_NO_IPV4_INTERFACE = "None of the local node's network interfaces have an IPv4 address assigned";
        public static final String FASTLOAD_NO_NETWORK_INTERFACE = "Network interface is not found";
        public static final String BATCH_INSERT_FAILED = "Batch insert job failed";
        public static final String BATCH_INSERT_FAILED_TASK_RESTART = "Batch insert failed due to task restart; batch insert supports failover when staging is enabled";
        public static final String FASTLOAD_ERRLIMIT_EXCEED = "Fastload job error number exceeded";
        public static final String FASTLOAD_NO_TRACKER_REACHABLE = "None of the local node's network interfaces can reach the cluster's job tracker/RM or task trackers/AMs";
        public static final String FASTLOAD_ERROR_PARSING_TRACKER_NAMES = "Failed to parse the names of the job tracker/RM and task trackers/AMs";
        public static final String FASTLOAD_STAGETABLE_TO_TARGETTABLE_FAILED = "Unable to insert data in the staging table into the target table";
        public static final String FASTEXPORT_EMPTY_LSN = "FastExport LSN is empty";
        public static final String FASTEXPORT_INVALID_CMD = "Expected FastExport transfer begin command, but got error";
        public static final String FASTEXPORT_FAILED = "FastExport job failed";
        public static final String FASTEXPORT_TIMEOUT = "Internal FastExport socket server time out";
        public static final String SPLIT_COLUMN_DATATYPE_UNSUPPORTED = "Split column's data type is not supported";
        public static final String SPLIT_COLUMN_MISSING = "Split column is not found";
        public static final String SPLIT_COLUMN_MIN_MAX_VALUE_EMPTY = "Split column does not have min and max values";
        public static final String SPLIT_COLUMN_MIN_MAX_VALUE_NULL = "Split column min or/and max values are null";
        public static final String SPLIT_SQL_MISSING = "Split SQL is missing";
        public static final String FASTLOAD_LSN_FILE_ALREADY_EXISTS = "fastload lsn file already exists";
        public static final String FASTEXPORT_LSN_FILE_ALREADY_EXISTS = "fastexport lsn file already exists";
        public static final String FASTEXPORT_PARAMS_FILE_ALREADY_EXISTS = "fastexport params file already exists";
        public static final String OUTPUT_PATH_TABLE_BOTH_PRESENT = "Either target path or target Hive table name should be provided, but not both";
        public static final String OUTPUT_PATH_TABLE_BOTH_MISSING = "Neither target path nor target Hive table name is provided";
        public static final String OUTPUT_COLUMN_SCHEMA_MISSING = "Import Hive table's column schema is missing";
        public static final String OUTPUT_FIELD_NAMES_NOT_INCLUDE_PARTITION_COLUMNS = "Target table field names not include partition columns";
        public static final String OUTPUT_TABLE_SCHEMA_BOTH_PRESENT = "Import target table schema should not be provided for exist target table";
        public static final String OUTPUT_TABLE_SCHEMA_CONTAIN_PARTITION_SCHEMA = "Target table schema should not contain partition schema";
        public static final String HIVE_CONF_FILE_NOT_FOUND = "hive-site.xml specified by -hiveconf is not found";
        public static final String HIVE_CONF_FILE_IS_DIR = "hive-site.xml specified by -hiveconf should not be a directory";
        public static final String INPUT_PATH_TABLE_BOTH_PRESENT = "Either source path or source Hive table name should be provided, but not both";
        public static final String INPUT_PATH_TABLE_BOTH_MISSING = "Neither source path nor source Hive table name is provided";
        public static final String INPUT_COLUMN_SCHEMA_MISSING = "Export source Hive table's column schema is missing";
        public static final String INPUT_PARTITION_SCHEMA_MISSING = "Export source Hive table's partition schema is missing";
        public static final String INPUT_TABLE_SCHEMA_BOTH_PRESENT = "Export source table schema should not be provided for exist source table";
        public static final String FIELD_NAME_NOT_IN_HIVE_TABLE = "Some field names are not in the hive table schema";
        public static final String HIVE_TABLE_INVALID = "Hive table is not found, either it does not exist, or there is a permission issue";
        public static final String HIVE_TABLE_INPUTFORMAT_UNSUPPORTED = "Hive table's InputFormat class is not supported";
        public static final String HIVE_TABLE_OUTPUTFORMAT_UNSUPPORTED = "Hive table's OutputFormat class is not supported";
        public static final String HIVE_TABLE_SERDELIB_UNSUPPORTED = "Hive table's SerdeLib class is not supported";
        public static final String HIVE_COMMAND_NEED_RETRY = "Hive command needs to be retried";
        public static final String HIVE_THRIFT_EXCEPTION = "Hive command operation on Thrift server failed";
        public static final String HIVE_TABLE_EMPTY = "Hive table is empty with no data";
        public static final String HIVE_CURRENT_USER_DIRECTORY_NOT_EXISTS = "Hive current user directory not exists";
        public static final String HIVE_CONF_FILE_BAD_FORMAT = "Bad Hive Configuration file format";
        public static final String HIVE_PROVIDED_DIRECTORY_INVALID = "Invalid directory provided";
        public static final String HIVE_PROVIDED_PATH_NOT_EXISTED = "Provided path not exists";
        public static final String HIVE_PROVIDED_FILTER_INVALID = "Invalid filter provided";
        public static final String HIVE_NOT_SUPPORT_OUTPUTFORMAT = "Current Hive output format for non-existed table is not supported by TDCH";
        public static final String INPUT_SOURCE_TABLE_MISSING = "Source HCatalog table name should be provided";
        public static final String OUTPUT_TARGET_TABLE_MISSING = "Target HCatalog table name should be provided";
        public static final String AVRO_SCHEMA_FILE_NOT_FOUND = "Avro schema file not found";
        public static final String AVRO_SCHEMA_FILE_IS_DIR = "Avro schema file should not be a directory";
        public static final String OUTPUT_AVRO_DUPLICATE_SCHEMA = "-avroschemafile and -sourcetableschema can not be provided at the same time";
        public static final String INPUT_AVRO_DUPLICATE_SCHEMA = "-avroschemafile and -targettableschema can not be provided at the same time";
        public static final String AVRO_NO_MAPPING_SCHEMA_FOUND = "no Avro schema is found for type mapping";
        public static final String AVRO_FIELD_NAME_NOT_IN_SCHEMA = "field name not in avro schema";
        public static final String AVRO_UNION_TYPE_CAST_FAILED = "cast failed for avro union type";
        public static final String HDFS_TEXTFILE_SOURCE_RECORD_AND_TABLE_SCHEMA_BOTH_REQUIRED = "for hdfs job, sourcerecordschema and sourcetableschema must be provided at same time";
        public static final String HDFS_TEXTFILE_FIELD_NAME_AND_TABLE_SCHEMA_BOTH_REUIRED = "Source field name is not available without source schema";
        public static final String HDFS_INPUT_OBJECT_IS_NOT_TEXT = "input writable object is not a text";
        public static final String AVRO_UNRECOG_TYPE = "unrecognized avro type";
        public static final String AVRO_SCHEMA_INVALID = "avro schema is invalid";
        public static final String HDFS_INPUT_PATH_EMPTY = "HDFS input source path is empty with no data";
        public static final String INVALID_IDATASTREAM_SOCKET_HOST = "Invalid IDataStream socket host value";
        public static final String INVALID_IDATASTREAM_SOCKET_PORT = "Invalid IDataStream socket port value";
        public static final String INVALID_IDATASTREAM_FIELD_NAMES_ARRAY = "Invalid IDataStream field names array";
        public static final String INVALID_IDATASTREAM_FIELD_TYPES_ARRAY = "Invalid IDataStream field types array";
        public static final String UNSUPPORTED_IDATASTREAM_FIELD_TYPE = "One or more of the types defined in the IDataStream schema are currently unsupported by the IDataStream plugin";
        public static final String IDATASTREAM_JOB_FAILED = "The IDataStream job failed";
        public static final String IDATASTREAM_DATA_CONV_FAILED = "Failure during data conversion";
        public static final String OUTPUT_HIVE_PARTITION = "-sourcepartitionpath can not be set in a task where jobType is non-hive tasks";
        public static final String HIVE_PARTITION_INPUT_PATH_EMPTY = "Hive partition  input source path not exists";
    }
}