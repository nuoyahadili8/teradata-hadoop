package com.teradata.connector.common.utils;

import org.apache.hadoop.conf.*;

import java.util.*;

public class ConnectorConfiguration {
    public static final String TDCH_PRE_JOB_HOOK = "tdch.pre.job.hook";
    public static final String TDCH_POST_JOB_HOOK = "tdch.post.job.hook";
    public static final String TDCH_PLUGIN_INPUT_FORMAT = "tdch.plugin.input.format";
    public static final String TDCH_PLUGIN_OUTPUT_FORMAT = "tdch.plugin.output.format";
    public static final String TDCH_INPUT_SERDE = "tdch.input.serde";
    public static final String TDCH_OUTPUT_SERDE = "tdch.output.serde";
    public static final String TDCH_PLUGIN_INPUT_PROCESSOR = "tdch.plugin.input.processor";
    public static final String TDCH_PLUGIN_OUTPUT_PROCESSOR = "tdch.plugin.output.processor";
    public static final String TDCH_DATA_CONVERTER = "tdch.plugin.data.converter";
    public static final String TDCH_PLUGIN_INPUT_SPLIT_CLASS = "tdch.plugin.input.split";
    public static final String TDCH_JOB_MAPPER = "tdch.job.mapper";
    public static final String TDCH_JOB_REDUCER = "tdch.job.reducer";
    public static final String TDCH_USE_COMBINED_INPUT_FORMAT = "tdch.use.combined.input.format";
    public static final String TDCH_USE_PARTITIONED_OUTPUT_FORMAT = "tdch.use.partitioned.output.format";
    public static final String TDCH_NUM_MAPPERS = "tdch.num.mappers";
    public static final String TDCH_NUM_REDUCERS = "tdch.num.reducers";
    public static final String TDCH_JOB_SUCCEEDED = "tdch.job.succeeded";
    public static final String TDCH_THROTTLE_NUM_MAPPERS = "tdch.throttle.num.mappers";
    public static final String TDCH_THROTTLE_NUM_MAPPERS_MIN_MAPPERS = "tdch.throttle.num.mappers.min.mappers";
    public static final String TDCH_THROTTLE_NUM_MAPPERS_RETRY_SECONDS = "tdch.throttle.num.mappers.retry.seconds";
    public static final String TDCH_THROTTLE_NUM_MAPPERS_RETRY_COUNT = "tdch.throttle.num.mappers.retry.count";
    public static final String TDCH_STRING_TRUNCATE = "tdch.string.truncate";
    public static final String TDCH_OUTPUT_PARTITION_COLUMN_NAMES = "tdch.output.partition.column.names";
    public static final String TDCH_INPUT_CONVERTER_RECORD_SCHEMA = "tdch.intput.converter.record.schema";
    public static final String TDCH_OUTPUT_CONVERTER_RECORD_SCHEMA = "tdch.output.converter.record.schema";
    public static final String TDCH_INTER_MAP_COMPRESSION = "mapreduce.map.output.compress";
    public static final String TDCH_INTER_MAP_COMPRESSION_CODEC = "mapreduce.map.output.compress.codec";
    public static final String TDCH_INPUT_COMPRESSION = "mapreduce.output.fileoutputformat.compress";
    public static final String TDCH_INPUT_COMPRESSION_FOR_PARQUET = "parquet.compression";
    public static final String TDCH_INPUT_COMPRESSION_TYPE = "mapreduce.output.fileoutputformat.compress.type";
    public static final String TDCH_INPUT_COMPRESSION_CODEC = "mapreduce.output.fileoutputformat.compress.codec";
    public static final String TDCH_OUTPUT_LOGGING_FLAG = "tdch.output.hdfs.logging";
    public static final String TDCH_OUTPUT_LOGGING_PATH = "tdch.output.hdfs.logging.path";
    public static final String TDCH_OUTPUT_LOGGING_DELIMITER = "tdch.output.hdfs.logging.delimiter";
    public static final String TDCH_OUTPUT_LOGGING_LIMIT = "tdch.output.hdfs.logging.limit";
    public static final String TDCH_OUTPUT_WRITE_PHASE_CLOSE = "tdch.output.write.phase.close";
    public static final String TDCH_PLUGIN_CONF_FILE = "tdch.job.plugin.configuration.file";
    public static final String TDCH_INPUT_DATE_FORMAT = "tdch.input.date.format";
    public static final String TDCH_OUTPUT_DATE_FORMAT = "tdch.output.date.format";
    public static final String TDCH_INPUT_TIMESTAMP_FORMAT = "tdch.input.timestamp.format";
    public static final String TDCH_OUTPUT_TIMESTAMP_FORMAT = "tdch.output.timestamp.format";
    public static final String TDCH_INPUT_TIME_FORMAT = "tdch.input.time.format";
    public static final String TDCH_OUTPUT_TIME_FORMAT = "tdch.output.time.format";
    public static final String TDCH_INPUT_TIMEZONE_ID = "tdch.input.timezone.id";
    public static final String TDCH_OUTPUT_TIMEZONE_ID = "tdch.output.timezone.id";
    public static final String VALUE_JOB_TYPE_HCAT = "hcat";
    public static final String VALUE_JOB_TYPE_HIVE = "hive";
    public static final String VALUE_JOB_TYPE_HDFS = "hdfs";
    public static final String VALUE_JOB_TYPE_TERADATA = "teradata";
    public static final String VALUE_FILE_FORMAT_TEXTFILE = "textfile";
    public static final String VALUE_FILE_FORMAT_SEQUENCEFILE = "sequencefile";
    public static final String VALUE_FILE_FORMAT_RCFILE = "rcfile";
    public static final String VALUE_FILE_FORMAT_AVROFILE = "avrofile";
    public static final String VALUE_FILE_FORMAT_ORCFILE = "orcfile";
    public static final String VALUE_FILE_FORMAT_ORC = "orc";
    public static final String VALUE_FILE_FORMAT_PARQUET = "parquet";
    public static final String VALUE_MAPRED_INPUT_DIR = "mapred.input.dir";
    public static final String VALUE_MAPRED_OUTPUT_DIR = "mapred.output.dir";
    public static final int VALUE_NUMBER_MAPPERS_DEFAULT = 2;
    public static final int VALUE_NUMBER_REDUCERS_DEFAULT = 0;
    public static final int VALUE_THROTTLE_MAPPERS_RETRY_SECONDS_DEFAULT = 5;
    public static final int VALUE_THROTTLE_MAPPERS_RETRY_COUNT_DEFAULT = 12;
    public static final int VALUE_CONNECTOR_DUG_OPTION_CLOSED = 0;
    public static final int VALUE_CONNECTOR_WRITE_PHASE_CLOSED = 1;
    public static final String TDCH_DEFAULT_INPUT_SPLIT = "com.teradata.connector.teradata.TeradataInputFormat$TeradataInputSplit";
    private static final int MAX_CONFIG_ITEM_LIMIT = 100;
    private static final String POST_FIX = ".";

    public static void setPreJobHook(final Configuration configuration, final String preJobHooks) {
        configuration.set("tdch.pre.job.hook", preJobHooks);
    }

    public static String getPreJobHook(final Configuration configuration) {
        return configuration.get("tdch.pre.job.hook");
    }

    public static void setPostJobHook(final Configuration configuration, final String postJobHooks) {
        configuration.set("tdch.post.job.hook", postJobHooks);
    }

    public static String getPostJobHook(final Configuration configuration) {
        return configuration.get("tdch.post.job.hook");
    }

    public static void setPlugInInputFormat(final Configuration configuration, final String plugInInputFormatClass) {
        configuration.set("tdch.plugin.input.format", plugInInputFormatClass);
    }

    public static String getPlugInInputFormat(final Configuration configuration) {
        return configuration.get("tdch.plugin.input.format", "");
    }

    public static void setPlugInOutputFormat(final Configuration configuration, final String plugInOutputFormatClass) {
        configuration.set("tdch.plugin.output.format", plugInOutputFormatClass);
    }

    public static String getPlugInOutputFormat(final Configuration configuration) {
        return configuration.get("tdch.plugin.output.format", "");
    }

    public static void setInputSerDe(final Configuration configuration, final String sourceSerDeClass) {
        configuration.set("tdch.input.serde", sourceSerDeClass);
    }

    public static String getInputSerDe(final Configuration configuration) {
        return configuration.get("tdch.input.serde", "");
    }

    public static void setOutputSerDe(final Configuration configuration, final String targetSerDeClass) {
        configuration.set("tdch.output.serde", targetSerDeClass);
    }

    public static String getOutputSerDe(final Configuration configuration) {
        return configuration.get("tdch.output.serde", "");
    }

    public static void setPlugInInputProcessor(final Configuration configuration, final String plugInInputProcessorClass) {
        configuration.set("tdch.plugin.input.processor", plugInInputProcessorClass);
    }

    public static String getPlugInInputProcessor(final Configuration configuration) {
        return configuration.get("tdch.plugin.input.processor", "");
    }

    public static void setPlugInOutputProcessor(final Configuration configuration, final String plugInOutputProcessorClass) {
        configuration.set("tdch.plugin.output.processor", plugInOutputProcessorClass);
    }

    public static String getPlugInOutputProcessor(final Configuration configuration) {
        return configuration.get("tdch.plugin.output.processor", "");
    }

    public static void setDataConverter(final Configuration configuration, final String plugedInDataConverter) {
        configuration.set("tdch.plugin.data.converter", plugedInDataConverter);
    }

    public static String getDataConverter(final Configuration configuration) {
        return configuration.get("tdch.plugin.data.converter", "");
    }

    public static void setPluginConf(final Configuration configuration, final String filePath) {
        configuration.set("tdch.job.plugin.configuration.file", filePath);
    }

    public static String getPluginConf(final Configuration configuration) {
        return configuration.get("tdch.job.plugin.configuration.file", "");
    }

    public static void setInputSplit(final Configuration configuration, final String inputClass) {
        configuration.set("tdch.plugin.input.split", inputClass);
    }

    public static String getInputSplit(final Configuration configuration) {
        return configuration.get("tdch.plugin.input.split", "com.teradata.connector.teradata.TeradataInputFormat$TeradataInputSplit");
    }

    public static void setJobMapper(final Configuration configuration, final String jobMapperClass) {
        configuration.set("tdch.job.mapper", jobMapperClass);
    }

    public static String getJobMapper(final Configuration configuration) {
        return configuration.get("tdch.job.mapper", "");
    }

    public static void setJobReducer(final Configuration configuration, final String jobReducerClass) {
        configuration.set("tdch.job.reducer", jobReducerClass);
    }

    public static String getJobReducer(final Configuration configuration) {
        return configuration.get("tdch.job.reducer", "");
    }

    public static void setUseCombinedInputFormat(final Configuration configuration, final boolean useCombinedInputFormat) {
        configuration.setBoolean("tdch.use.combined.input.format", useCombinedInputFormat);
    }

    public static boolean getUseCombinedInputFormat(final Configuration configuration) {
        return configuration.getBoolean("tdch.use.combined.input.format", true);
    }

    public static void setUsePartitionedOutputFormat(final Configuration configuration, final boolean value) {
        configuration.setBoolean("tdch.use.partitioned.output.format", value);
    }

    public static boolean getUsePartitionedOutputFormat(final Configuration configuration) {
        return configuration.getBoolean("tdch.use.partitioned.output.format", false);
    }

    public static void setNumMappers(final Configuration configuration, final int numMappers) {
        configuration.setInt("tdch.num.mappers", numMappers);
    }

    public static int getNumMappers(final Configuration configuration) {
        return configuration.getInt("tdch.num.mappers", 2);
    }

    public static void setCompressionInter(final Configuration configuration, final boolean value) {
        configuration.setBoolean("mapreduce.map.output.compress", value);
    }

    public static boolean getCompressionInter(final Configuration configuration) {
        return configuration.getBoolean("mapreduce.map.output.compress", false);
    }

    public static void setEnableHdfsLoggingFlag(final Configuration configuration, final boolean value) {
        configuration.setBoolean("tdch.output.hdfs.logging", value);
    }

    public static boolean getEnableHdfsLoggingFlag(final Configuration configuration) {
        return configuration.getBoolean("tdch.output.hdfs.logging", false);
    }

    public static void setLogDelimiter(final Configuration configuration, final String value) {
        configuration.set("tdch.output.hdfs.logging.delimiter", value);
    }

    public static String getLogDelimiter(final Configuration configuration) {
        return configuration.get("tdch.output.hdfs.logging.delimiter", "|");
    }

    public static void setLogRowsCount(final Configuration configuration, final int value) {
        configuration.setInt("tdch.output.hdfs.logging.limit", value);
    }

    public static int getLogRowsCount(final Configuration configuration) {
        return configuration.getInt("tdch.output.hdfs.logging.limit", 100);
    }

    public static void setHdfsLoggingPath(final Configuration configuration, final String path) {
        configuration.set("tdch.output.hdfs.logging.path", path);
    }

    public static String getHdfsLoggingPath(final Configuration configuration) {
        return configuration.get("tdch.output.hdfs.logging.path", "");
    }

    public static void setCompressionInterCodec(final Configuration configuration, final String interCodec) {
        configuration.set("mapreduce.map.output.compress.codec", interCodec);
    }

    public static String getCompressionInterCodec(final Configuration configuration) {
        return configuration.get("mapreduce.map.output.compress.codec", "NONE");
    }

    public static void setTargetCompressionForParquet(final Configuration configuration, final String type) {
        configuration.set("parquet.compression", type);
    }

    public static String getTargetCompressionForParquet(final Configuration configuration) {
        return configuration.get("parquet.compression", "NONE");
    }

    public static void setTargetCompression(final Configuration configuration, final boolean value) {
        configuration.setBoolean("mapreduce.output.fileoutputformat.compress", value);
    }

    public static boolean getTargetCompression(final Configuration configuration) {
        return configuration.getBoolean("mapreduce.output.fileoutputformat.compress", false);
    }

    public static void setTargetCompressionType(final Configuration configuration, final String type) {
        configuration.set("mapreduce.output.fileoutputformat.compress.type", type);
    }

    public static String getTargetCompressionType(final Configuration configuration) {
        return configuration.get("mapreduce.output.fileoutputformat.compress.type", "NONE");
    }

    public static void setTargetCompressionCodec(final Configuration configuration, final String codec) {
        configuration.set("mapreduce.output.fileoutputformat.compress.codec", codec);
    }

    public static String getTargetCompressionCodec(final Configuration configuration) {
        return configuration.get("mapreduce.output.fileoutputformat.compress.codec", "org.apache.hadoop.io.compress.GzipCodec");
    }

    public static void setThrottleNumMappers(final Configuration configuration, final boolean value) {
        configuration.setBoolean("tdch.throttle.num.mappers", value);
    }

    public static boolean getThrottleNumMappers(final Configuration configuration) {
        return configuration.getBoolean("tdch.throttle.num.mappers", false);
    }

    public static void setThrottleNumMappersMinMappers(final Configuration configuration, final int minMappers) {
        configuration.setInt("tdch.throttle.num.mappers.min.mappers", minMappers);
    }

    public static int getThrottleNumMappersMinMappers(final Configuration configuration) {
        return configuration.getInt("tdch.throttle.num.mappers.min.mappers", 0);
    }

    public static void setThrottleNumMappersRetrySeconds(final Configuration configuration, final int retrySeconds) {
        configuration.setInt("tdch.throttle.num.mappers.retry.seconds", retrySeconds);
    }

    public static int getThrottleNumMappersRetrySeconds(final Configuration configuration) {
        return configuration.getInt("tdch.throttle.num.mappers.retry.seconds", 5);
    }

    public static void setThrottleNumMappersRetryCount(final Configuration configuration, final int retryCount) {
        configuration.setInt("tdch.throttle.num.mappers.retry.count", retryCount);
    }

    public static int getThrottleNumMappersRetryCount(final Configuration configuration) {
        return configuration.getInt("tdch.throttle.num.mappers.retry.count", 12);
    }

    public static void setStringTruncate(final Configuration configuration, final boolean value) {
        configuration.setBoolean("tdch.string.truncate", value);
    }

    public static boolean getStringTruncate(final Configuration configuration) {
        return configuration.getBoolean("tdch.string.truncate", true);
    }

    public static void setNumReducers(final Configuration configuration, final int numReducers) {
        configuration.setInt("tdch.num.reducers", numReducers);
    }

    public static int getNumReducers(final Configuration configuration) {
        return configuration.getInt("tdch.num.reducers", 0);
    }

    public static void setJobSucceeded(final Configuration configuration, final boolean jobSucceeded) {
        configuration.setBoolean("tdch.job.succeeded", jobSucceeded);
    }

    public static boolean getJobSucceeded(final Configuration configuration) {
        return configuration.getBoolean("tdch.job.succeeded", true);
    }

    public static void setOutputPartitionColumnNames(final Configuration configuration, final String targetPartitionColumnNames) {
        configuration.set("tdch.output.partition.column.names", targetPartitionColumnNames);
    }

    public static String getOutputPartitionColumnNames(final Configuration configuration) {
        return configuration.get("tdch.output.partition.column.names", "");
    }

    public static void setInputConverterRecordSchema(final Configuration configuration, final String inputRecordSchema) {
        configuration.set("tdch.intput.converter.record.schema", inputRecordSchema);
    }

    public static String getInputConverterRecordSchema(final Configuration configuration) {
        return configuration.get("tdch.intput.converter.record.schema", "");
    }

    public static void setOutputConverterRecordSchema(final Configuration configuration, final String inputRecordSchema) {
        configuration.set("tdch.output.converter.record.schema", inputRecordSchema);
    }

    public static String getOutputConverterRecordSchema(final Configuration configuration) {
        return configuration.get("tdch.output.converter.record.schema", "");
    }

    public static int getDebugOption(final Configuration configuration) {
        return configuration.getInt("tdch.output.write.phase.close", 0);
    }

    public static void setDebugOption(final Configuration configuration, final int debugOption) {
        configuration.setInt("tdch.output.write.phase.close", debugOption);
    }

    public static String getInputDateFormat(final Configuration configuration) {
        return configuration.get("tdch.input.date.format", "");
    }

    public static void setInputDateFormat(final Configuration configuration, final String inputDateFormat) {
        configuration.set("tdch.input.date.format", inputDateFormat);
    }

    public static String getOutputDateFormat(final Configuration configuration) {
        return configuration.get("tdch.output.date.format", "");
    }

    public static void setOutputDateFormat(final Configuration configuration, final String outputDateFormat) {
        configuration.set("tdch.output.date.format", outputDateFormat);
    }

    public static String getInputTimestampFormat(final Configuration configuration) {
        return configuration.get("tdch.input.timestamp.format", "");
    }

    public static void setInputTimestampFormat(final Configuration configuration, final String inputDateFormat) {
        configuration.set("tdch.input.timestamp.format", inputDateFormat);
    }

    public static String getOutputTimestampFormat(final Configuration configuration) {
        return configuration.get("tdch.output.timestamp.format", "");
    }

    public static void setOutputTimestampFormat(final Configuration configuration, final String outputDateFormat) {
        configuration.set("tdch.output.timestamp.format", outputDateFormat);
    }

    public static String getInputTimeFormat(final Configuration configuration) {
        return configuration.get("tdch.input.time.format", "");
    }

    public static void setInputTimeFormat(final Configuration configuration, final String inputDateFormat) {
        configuration.set("tdch.input.time.format", inputDateFormat);
    }

    public static String getOutputTimeFormat(final Configuration configuration) {
        return configuration.get("tdch.output.time.format", "");
    }

    public static void setOutputTimeFormat(final Configuration configuration, final String outputDateFormat) {
        configuration.set("tdch.output.time.format", outputDateFormat);
    }

    public static String getInputTimezoneId(final Configuration configuration) {
        return configuration.get("tdch.input.timezone.id", "");
    }

    public static void setInputTimezoneId(final Configuration configuration, final String inputTimezoneId) {
        configuration.set("tdch.input.timezone.id", inputTimezoneId);
    }

    public static String getOutputTimezoneId(final Configuration configuration) {
        return configuration.get("tdch.output.timezone.id", "");
    }

    public static void setOutputTimezoneId(final Configuration configuration, final String outputTimezoneId) {
        configuration.set("tdch.output.timezone.id", outputTimezoneId);
    }

    public static List<String> getAllConfigurationItems(final Configuration config, final String name) {
        final List<String> list = new ArrayList<String>();
        for (int i = 1; i <= 100; ++i) {
            final String value = config.get(name + "." + i, "").trim();
            if (!value.isEmpty()) {
                list.add(value);
            }
        }
        return list;
    }

    public enum direction {
        input,
        output;
    }
}
