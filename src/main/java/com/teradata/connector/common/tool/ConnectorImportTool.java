package com.teradata.connector.common.tool;

import com.teradata.connector.common.ConnectorRecordSchema;
import com.teradata.connector.common.exception.ConnectorException;
import com.teradata.connector.common.utils.*;
import com.teradata.connector.hcat.utils.HCatPlugInConfiguration;
import com.teradata.connector.hdfs.utils.HdfsPlugInConfiguration;
import com.teradata.connector.hive.utils.HivePlugInConfiguration;
import com.teradata.connector.hive.utils.HiveUtils;
import com.teradata.connector.teradata.utils.TeradataPlugInConfiguration;
import com.teradata.connector.teradata.utils.TeradataSchemaUtils;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

/**
 * @author Administrator
 */
public class ConnectorImportTool extends Configured implements Tool
{
    private static Log logger;
    private String jobType;
    private String method;
    private String fileFormat;
    
    public ConnectorImportTool() {
        this.jobType = "hdfs";
        this.method = "split.by.hash";
        this.fileFormat = "textfile";
    }
    
    @Override
    public int run(final String[] args) throws Exception {
        final Job job = new Job(this.getConf());
        try {
            if (this.processArgs((JobContext)job, args) < 0) {
                this.printHelp();
                return 0;
            }
        }
        catch (ConnectorException e) {
            this.printHelp();
            throw e;
        }
        return ConnectorJobRunner.runJob(job);
    }
    
    public void printHelp() {
        System.out.println("hadoop jar teradata-hadoop-connector.jar");
        System.out.println("     com.teradata.connector.common.tool.ConnectorImportTool");
        System.out.println("     [-conf <conf file>] (optional)");
        System.out.println("     [-jobtype <job type>] (values: hdfs, hive, and hcat, default is hdfs)");
        System.out.println("     [-fileformat <file format>] (values: sequencefile, textfile, avrofile, orcfile (or orc), rcfile, and parquet, default is textfile)");
        System.out.println("     [-classname <classname>] (optional)");
        System.out.println("     [-url <url>] (optional)");
        System.out.println("     [-username <username>] (optional)");
        System.out.println("     [-password <password>] (optional)");
        System.out.println("     [-batchsize <batchsize>] (optional, default value is 10000)");
        System.out.println("     [-accesslock <true|false>] (optional, default value is false)");
        System.out.println("     [-queryband <queryband>] (optional)");
        System.out.println("     [-targetpaths <path>] (optional, applicable for hdfs and hive jobs)]");
        System.out.println("     [-sourcetable <tablename>] (optional, use -sourcetable or -sourcequery but not both)");
        System.out.println("     [-sourceconditions <conditions>] (optional, use with -sourcetable option)");
        System.out.println("     [-sourcefieldnames <fieldnames>] (optional, comma delimited format)");
        System.out.println("     [-sourcerecordschema <record schema>] (optional, comma delimited format)");
        System.out.println("     [-targetrecordschema <record schema>] (optional, comma delimited format)");
        System.out.println("     [-sourcequery <query>] (optional, use either -sourcetable or -sourcequery but not both)");
        System.out.println("     [-sourcecountquery <countquery>] (optional, use with -sourcequery option)");
        System.out.println("     [-targetdatabase <database>] (optional)");
        System.out.println("     [-targettable <table>] (optional)");
        System.out.println("     [-targetfieldnames <fields>] (optional, comma separated format");
        System.out.println("     [-targettableschema <schema>] (optional, comma separated format");
        System.out.println("     [-targetpartitionschema <schema>] (optional, comma separated format, used with -targettableschema only");
        System.out.println("     [-separator <separator>] (optional,used to separate fields in text)");
        System.out.println("     [-lineseparator <lineseparator>] (optional, used to separate different lines, only useful for hive and hcat job)");
        System.out.println("     [-enclosedby <enclosed-by-character> (optional, used to enclose text, only useful for hdfs job)]");
        System.out.println("     [-escapedby  <escaped-by-character> (optional, used to escape special characters, only useful for hdfs job)]");
        System.out.println("     [-nullstring <string>] (optional, a string to replace null value of string type)");
        System.out.println("     [-nullnonstring <string>] (optional, a string to replace null value of non-string type, only useful for hdfs job)");
        System.out.println("    [-method <method>] (optional import method, values: internal.fastexport, split.by.partition, split.by.hash, split.by.value and split.by.amp only for Teradata version 14.10.00.02, default is split.by.hash)");
        System.out.println("    [-nummappers <num>] (optional, default is 2)");
        System.out.println("    [-throttlemappers <true> (optional, overwrite nummappers value based on cluster resources)");
        System.out.println("    [-minmappers <num>] (optional, only overwrite nummappers value if/when minmappers are available)");
        System.out.println("    [-splitbycolumn <column name>] (optional for split.by.hash and split.by.value methods)");
        System.out.println("    [-forcestage <true>] (optional force to use stage table, default is false)");
        System.out.println("    [-stagetablename <table name>] (optional)");
        System.out.println("    [-stagedatabase <database>] (optional)");
        System.out.println("    [-stagedatabasefortable <database>] (optional)");
        System.out.println("    [-stagedatabaseforview <database>] (optional)");
        System.out.println("    [-numpartitionsinstaging <num>] (optional)");
        System.out.println("    [-fastexportsockethost <host>] (optional)");
        System.out.println("    [-fastexportsocketport <port>] (optional)");
        System.out.println("    [-fastexportsockettimeout <time(ms)>] (optional, default is 8 minutes)");
        System.out.println("    [-hiveconf <target path>] (optional, required for hive and hcat jobs launched on non-name nodes)]");
        System.out.println("    [-usexview <true|false>] (optional, default is true)");
        System.out.println("    [-targetcompression <I|O|B>] (optional, required for hive and hdfs jobs for enabling compression)]");
        System.out.println("    [-targetcompressiontype <BLOCK|RECORD|NONE>] (optional, required for hive and hdfs jobs for types of compression)]");
        System.out.println("    [-targetcompressioncodec <LZO|SNAPPY|BZIP2|GZIP>] (optional, required for hive and hdfs jobs for compression codecs selection)]");
        System.out.println("    [-avroschema <avro schema>] (optional, an inline Avro schema definition)");
        System.out.println("    [-avroschemafile <path>] (optional, a file path for Avro schema definition)");
        System.out.println("    [-debugoption <debug option value>] (optional, a debug option to close some stages of the whole hadoop job)");
        System.out.println("    [-sourcedateformat <date format>] (optional, a default date format for all converters that convert string to date)");
        System.out.println("    [-targetdateformat <date format>] (optional, a default date format for all converters that convert date  to string )");
        System.out.println("    [-sourcetimeformat <time format>] (optional, a default time format for all converters that convert string to time)");
        System.out.println("    [-targettimeformat <time format>] (optional, a default time format for all converters that convert time  to string )");
        System.out.println("    [-sourcetimestampformat <timestamp format>] (optional, a default timestamp format for all converters that convert string to timestamp)");
        System.out.println("    [-targettimestampformat <timestamp format>] (optional, a default timestamp format for all converters that convert timestamp  to string )");
        System.out.println("    [-sourcetimezoneid <timezone id>] (optional, a default input timezone id for all converters that convert timestamp to another timestamp");
        System.out.println("    [-targettimezoneid <timezone id>] (optional, a default output timezone id for all converters that convert timestamp to another timestamp");
        System.out.println("    [-stringtruncate <false>] (optional, do not automatically truncate strings and error)");
        System.out.println("    [-h|help] (optional)");
        System.out.println("");
    }
    
    private int processArgs(final JobContext context, final String[] args) throws ConnectorException {
        final Map<String, String> configurationMap = new HashMap<String, String>();
        int i = 0;
        final int length = args.length;
        if (length < 1) {
            throw new ConnectorException(12021);
        }
        while (i < length) {
            final String arg = args[i];
            if (arg == null || arg.isEmpty()) {
                return 0;
            }
            if (arg.charAt(0) != '-') {
                throw new ConnectorException(12001);
            }
            if (arg.equalsIgnoreCase("-h") || arg.equalsIgnoreCase("-help")) {
                return -1;
            }
            if (++i >= length) {
                throw new ConnectorException(12001);
            }
            final String value = args[i];
            if (value == null) {
                throw new ConnectorException(12001);
            }
            configurationMap.put(arg, value);
            ++i;
        }
        final Configuration configuration = context.getConfiguration();
        configuration.addResource("teradata-import-properties.xml");
        if (configurationMap.containsKey("-jobtype")) {
            this.jobType = configurationMap.remove("-jobtype").toLowerCase();
        }
        if (!this.jobType.equalsIgnoreCase("hcat") && !this.jobType.equalsIgnoreCase("hdfs") && !this.jobType.equalsIgnoreCase("hive")) {
            throw new ConnectorException(12002);
        }
        if (this.jobType.equals("hcat")) {
            ConnectorImportTool.logger.warn((Object)"The TDCH hcatalog plugin is deprecated, and will be removed in a future release");
        }
        if (configurationMap.containsKey("-fileformat")) {
            this.fileFormat = configurationMap.remove("-fileformat").toLowerCase();
            this.fileFormat = HadoopConfigurationUtils.getAliasFileFormatName(this.fileFormat);
        }
        if (!this.fileFormat.equalsIgnoreCase("textfile") && !this.fileFormat.equalsIgnoreCase("rcfile") && !this.fileFormat.equalsIgnoreCase("orcfile") && !this.fileFormat.equalsIgnoreCase("sequencefile") && !this.fileFormat.equalsIgnoreCase("avrofile") && !this.fileFormat.equalsIgnoreCase("parquet")) {
            throw new ConnectorException(12016);
        }
        if (configurationMap.containsKey("-method")) {
            this.method = configurationMap.remove("-method").toLowerCase();
        }
        if (!this.method.equalsIgnoreCase("split.by.partition") && !this.method.equalsIgnoreCase("split.by.hash") && !this.method.equalsIgnoreCase("split.by.value") && !this.method.equalsIgnoreCase("split.by.amp") && !this.method.equalsIgnoreCase("idata.stream") && !this.method.equalsIgnoreCase("internal.fastexport")) {
            throw new ConnectorException(12010);
        }
        if (configurationMap.containsKey("-nummappers")) {
            try {
                final int numMappers = Integer.parseInt(configurationMap.remove("-nummappers"));
                if (numMappers < 1) {
                    throw new ConnectorException(String.format("invalid provided value for parameter %s, %s value is required for this parameter", "-nummappers", " greater than zero "));
                }
                ConnectorConfiguration.setNumMappers(configuration, numMappers);
            }
            catch (NumberFormatException e) {
                ConnectorImportTool.logger.info((Object)String.format("invalid provided value for parameter %s, %s value is required for this parameter", "-nummappers", "integer"));
                throw new ConnectorException(String.format("invalid provided value for parameter %s, %s value is required for this parameter", "-nummappers", "integer"), e);
            }
        }
        if (configurationMap.containsKey("-targetcompression")) {
            final String targetcompression = configurationMap.remove("-targetcompression").toUpperCase();
            if (targetcompression.equals("I")) {
                ConnectorConfiguration.setCompressionInter(configuration, true);
                ConnectorConfiguration.setCompressionInterCodec(configuration, "org.apache.hadoop.io.compress.SnappyCodec");
            }
            else if (targetcompression.equals("O") || targetcompression.equals("B")) {
                String targetcompressiontype = "";
                String targetcompressioncodec = "";
                String targetoutputcodec = "";
                if (configurationMap.containsKey("-targetcompressiontype")) {
                    targetcompressiontype = configurationMap.remove("-targetcompressiontype").toUpperCase();
                }
                if (configurationMap.containsKey("-targetcompressioncodec")) {
                    targetcompressioncodec = configurationMap.remove("-targetcompressioncodec").toUpperCase();
                }
                if (targetcompressiontype.equals("") || targetcompressioncodec.equals("")) {
                    throw new ConnectorException(String.format("invalid provided value for parameter %s, %s value is required for this parameter", "-targetcompressiontype or -targetcompressioncodec", " is empty "));
                }
                if (targetcompression.equals("B")) {
                    ConnectorConfiguration.setCompressionInter(configuration, true);
                    ConnectorConfiguration.setCompressionInterCodec(configuration, "org.apache.hadoop.io.compress.SnappyCodec");
                }
                ConnectorConfiguration.setTargetCompression(configuration, true);
                ConnectorConfiguration.setTargetCompressionType(configuration, targetcompressiontype);
                if (targetcompressioncodec.equals("SNAPPY")) {
                    targetoutputcodec = "org.apache.hadoop.io.compress.SnappyCodec";
                }
                else if (targetcompressioncodec.equals("GZIP")) {
                    targetoutputcodec = "org.apache.hadoop.io.compress.GzipCodec";
                }
                else if (targetcompressioncodec.equals("LZO")) {
                    targetoutputcodec = "com.hadoop.compression.lzo.LzoCodec";
                }
                else if (targetcompressioncodec.equals("BZIP2")) {
                    targetoutputcodec = "org.apache.hadoop.io.compress.BZip2Codec";
                }
                ConnectorConfiguration.setTargetCompressionCodec(configuration, targetoutputcodec);
                if (this.fileFormat.equalsIgnoreCase("parquet") && (targetcompressioncodec.equals("SNAPPY") || targetcompressioncodec.equals("LZO") || targetcompressioncodec.equals("GZIP"))) {
                    ConnectorConfiguration.setTargetCompressionForParquet(configuration, targetcompressioncodec);
                }
            }
        }
        if (configurationMap.containsKey("-throttlemappers")) {
            ConnectorConfiguration.setThrottleNumMappers(configuration, Boolean.parseBoolean(configurationMap.remove("-throttlemappers")));
        }
        if (configurationMap.containsKey("-minmappers")) {
            try {
                final int minMappers = Integer.parseInt(configurationMap.remove("-minmappers"));
                if (minMappers < 1) {
                    throw new ConnectorException(String.format("invalid provided value for parameter %s, %s value is required for this parameter", "-minmappers", " greater than zero "));
                }
                ConnectorConfiguration.setThrottleNumMappersMinMappers(configuration, minMappers);
            }
            catch (NumberFormatException e) {
                throw new ConnectorException(String.format("invalid provided value for parameter %s, %s value is required for this parameter", "-minmappers", "integer"), e);
            }
        }
        int numReducers = 0;
        if (configurationMap.containsKey("-numreducers")) {
            try {
                numReducers = Integer.parseInt(configurationMap.remove("-numreducers"));
                ConnectorConfiguration.setNumReducers(configuration, numReducers);
            }
            catch (NumberFormatException e2) {
                ConnectorImportTool.logger.info((Object)String.format("invalid provided value for parameter %s, %s value is required for this parameter", "-numreducers", "integer"));
                throw new ConnectorException(String.format("invalid provided value for parameter %s, %s value is required for this parameter", "-numreducers", "integer"), e2);
            }
        }
        if (numReducers != 0) {
            ConnectorImportTool.logger.warn((Object)"You are trying calling reducer phase");
        }
        if (configurationMap.containsKey("-hiveconf")) {
            HivePlugInConfiguration.setOutputConfigureFile(configuration, configurationMap.remove("-hiveconf"));
        }
        if (configurationMap.containsKey("-classname")) {
            TeradataPlugInConfiguration.setInputJdbcDriverClass(configuration, configurationMap.remove("-classname"));
        }
        if (configurationMap.containsKey("-url")) {
            TeradataPlugInConfiguration.setInputJdbcUrl(configuration, configurationMap.remove("-url"));
        }
        if (configurationMap.containsKey("-username")) {
            TeradataPlugInConfiguration.setInputTeradataUserName(context, configurationMap.remove("-username").getBytes(StandardCharsets.UTF_8));
        }
        if (configurationMap.containsKey("-password")) {
            TeradataPlugInConfiguration.setInputTeradataPassword(context, configurationMap.remove("-password").getBytes(StandardCharsets.UTF_8));
        }
        if (configurationMap.containsKey("-accesslock")) {
            TeradataPlugInConfiguration.setInputAccessLock(configuration, Boolean.parseBoolean(configurationMap.remove("-accesslock")));
        }
        if (configurationMap.containsKey("-queryband")) {
            TeradataPlugInConfiguration.setInputQueryBand(configuration, configurationMap.remove("-queryband"));
        }
        if (configurationMap.containsKey("-sourcetable")) {
            TeradataPlugInConfiguration.setInputTable(configuration, configurationMap.remove("-sourcetable"));
        }
        if (configurationMap.containsKey("-sourceconditions")) {
            TeradataPlugInConfiguration.setInputConditions(configuration, configurationMap.remove("-sourceconditions"));
        }
        String sourceFieldNames = "";
        if (configurationMap.containsKey("-sourcefieldnames")) {
            sourceFieldNames = configurationMap.remove("-sourcefieldnames");
            final String[] sourceFieldNamesArray = ConnectorSchemaUtils.convertFieldNamesToArray(sourceFieldNames);
            TeradataPlugInConfiguration.setInputFieldNamesArray(configuration, sourceFieldNamesArray);
        }
        if (configurationMap.containsKey("-sourcequery")) {
            TeradataPlugInConfiguration.setInputQuery(configuration, configurationMap.remove("-sourcequery"));
            ConnectorImportTool.logger.warn((Object)("The method will be defaulted to \"split.by.partition\" as \"" + this.method + "\" is not supported for -sourcequery option. Please use -sourcetable option in case you want to specify \"" + this.method + "\" method."));
            this.method = "split.by.partition";
        }
        final String outputPluginName = this.jobType.equalsIgnoreCase("hcat") ? this.jobType : (this.jobType + "-" + this.fileFormat);
        final String inputPluginName = this.method.equalsIgnoreCase("idata.stream") ? this.method : ("teradata-" + this.method);
        ConnectorPluginUtils.configConnectorOutputPlugins(configuration, outputPluginName);
        ConnectorPluginUtils.configConnectorInputPlugins(configuration, inputPluginName);
        if (configurationMap.containsKey("-sourcecountquery")) {
            configurationMap.remove("-sourcecountquery");
        }
        if (configurationMap.containsKey("-targetdatabase")) {
            final String targetDatabase = configurationMap.remove("-targetdatabase");
            if (this.jobType.equalsIgnoreCase("hive")) {
                HivePlugInConfiguration.setOutputDatabase(configuration, targetDatabase);
            }
            else if (this.jobType.equalsIgnoreCase("hcat")) {
                HCatPlugInConfiguration.setOutputDatabase(configuration, targetDatabase);
            }
            else {
                ConnectorImportTool.logger.warn((Object)"user provided database for job types excepts Hive and HCat");
            }
        }
        if (configurationMap.containsKey("-targettable")) {
            final String targetTable = configurationMap.remove("-targettable");
            if (this.jobType.equalsIgnoreCase("hive")) {
                HivePlugInConfiguration.setOutputTable(configuration, targetTable);
            }
            else if (this.jobType.equalsIgnoreCase("hcat")) {
                HCatPlugInConfiguration.setOutputTable(configuration, targetTable);
            }
            else {
                ConnectorImportTool.logger.warn((Object)"user provided table for job types excepts Hive and HCat");
            }
        }
        if (configurationMap.containsKey("-targetpaths")) {
            final String targetPathString = configurationMap.remove("-targetpaths");
            if (this.jobType.equalsIgnoreCase("hive")) {
                HivePlugInConfiguration.setOutputPaths(configuration, targetPathString);
            }
            else if (this.jobType.equalsIgnoreCase("hdfs")) {
                HdfsPlugInConfiguration.setOutputPaths(configuration, targetPathString);
            }
            else {
                ConnectorImportTool.logger.warn((Object)"user provided path for job types excepts Hive and HDFS");
            }
        }
        String targetFieldNames = "";
        if (configurationMap.containsKey("-targetfieldnames")) {
            targetFieldNames = configurationMap.remove("-targetfieldnames");
            final String[] targetFieldNamesArray = ConnectorSchemaUtils.convertFieldNamesToArray(targetFieldNames);
            if (this.jobType.equalsIgnoreCase("hive")) {
                HivePlugInConfiguration.setOutputFieldNamesArray(configuration, targetFieldNamesArray);
            }
            else if (this.jobType.equalsIgnoreCase("hdfs")) {
                HdfsPlugInConfiguration.setOutputFieldNamesArray(configuration, targetFieldNamesArray);
            }
            else if (this.jobType.equalsIgnoreCase("hcat")) {
                HCatPlugInConfiguration.setOutputFieldNamesArray(configuration, targetFieldNamesArray);
            }
        }
        if (configurationMap.containsKey("-targettableschema")) {
            final String targetTableSchema = configurationMap.remove("-targettableschema");
            if (this.jobType.equalsIgnoreCase("hive")) {
                HivePlugInConfiguration.setOutputTableSchema(configuration, targetTableSchema);
            }
            else if (this.jobType.equalsIgnoreCase("hdfs")) {
                HdfsPlugInConfiguration.setOutputSchema(configuration, targetTableSchema);
            }
            else {
                ConnectorImportTool.logger.warn((Object)"user provided table schema for job types excepts Hive and HDFS");
            }
        }
        if (configurationMap.containsKey("-separator")) {
            final String separator = configurationMap.remove("-separator");
            if (this.jobType.equalsIgnoreCase("hive")) {
                HivePlugInConfiguration.setOutputSeparator(configuration, separator);
            }
            else if (this.jobType.equalsIgnoreCase("hdfs") && this.fileFormat.equalsIgnoreCase("textfile")) {
                HdfsPlugInConfiguration.setOutputSeparator(configuration, separator);
            }
            else {
                ConnectorImportTool.logger.warn((Object)"user provided separator for job types excepts Hive and HDFS");
            }
        }
        if (configurationMap.containsKey("-forcestage")) {
            TeradataPlugInConfiguration.setInputStageTableForced(configuration, Boolean.parseBoolean(configurationMap.remove("-forcestage")));
        }
        if (configurationMap.containsKey("-stagetablename")) {
            TeradataPlugInConfiguration.setInputStageTableName(configuration, configurationMap.remove("-stagetablename"));
        }
        if (configurationMap.containsKey("-stagedatabase")) {
            TeradataPlugInConfiguration.setInputStageDatabase(configuration, configurationMap.remove("-stagedatabase"));
        }
        if (configurationMap.containsKey("-stagedatabasefortable")) {
            TeradataPlugInConfiguration.setInputStageDatabaseForTable(configuration, configurationMap.remove("-stagedatabasefortable"));
        }
        if (configurationMap.containsKey("-stagedatabaseforview")) {
            TeradataPlugInConfiguration.setInputStageDatabaseForView(configuration, configurationMap.remove("-stagedatabaseforview"));
        }
        if (configurationMap.containsKey("-batchsize")) {
            try {
                TeradataPlugInConfiguration.setInputBatchSize(configuration, Integer.parseInt(configurationMap.remove("-batchsize")));
            }
            catch (NumberFormatException e3) {
                ConnectorImportTool.logger.info((Object)String.format("invalid provided value for parameter %s, %s value is required for this parameter", "-batchsize", "integer"));
                throw new ConnectorException(String.format("invalid provided value for parameter %s, %s value is required for this parameter", "-batchsize", "integer"), e3);
            }
        }
        if (configurationMap.containsKey("-splitbycolumn")) {
            TeradataPlugInConfiguration.setInputSplitByColumn(configuration, configurationMap.remove("-splitbycolumn"));
        }
        if (configurationMap.containsKey("-lineseparator")) {
            HivePlugInConfiguration.setOutputLineSeparator(configuration, configurationMap.remove("-lineseparator"));
        }
        if (configurationMap.containsKey("-nullstring")) {
            final String nullString = configurationMap.remove("-nullstring");
            if (this.jobType.equalsIgnoreCase("hive")) {
                HivePlugInConfiguration.setOutputNullString(configuration, nullString);
            }
            else if (this.jobType.equalsIgnoreCase("hdfs")) {
                HdfsPlugInConfiguration.setOutputNullString(configuration, nullString);
            }
            else {
                ConnectorImportTool.logger.warn((Object)"user provided null string for job types excepts Hive and HDFS");
            }
        }
        if (configurationMap.containsKey("-nullnonstring")) {
            HdfsPlugInConfiguration.setOutputNullNonString(configuration, configurationMap.remove("-nullnonstring"));
        }
        if (configurationMap.containsKey("-escapedby")) {
            HdfsPlugInConfiguration.setOutputEscapedBy(configuration, configurationMap.remove("-escapedby"));
        }
        if (configurationMap.containsKey("-enclosedby")) {
            HdfsPlugInConfiguration.setOutputEnclosedBy(configuration, configurationMap.remove("-enclosedby"));
        }
        if (configurationMap.containsKey("-jobclientoutput")) {
            HdfsAppender.addHDFSAppender(configurationMap.remove("-jobclientoutput"));
        }
        if (configurationMap.containsKey("-usexviews")) {
            TeradataPlugInConfiguration.setInputDataDictionaryUseXView(configuration, Boolean.parseBoolean(configurationMap.remove("-usexviews")));
        }
        if (configurationMap.containsKey("-avroschema")) {
            HdfsPlugInConfiguration.setOutputAvroSchema(configuration, configurationMap.remove("-avroschema"));
        }
        if (configurationMap.containsKey("-avroschemafile")) {
            HdfsPlugInConfiguration.setOutputAvroSchemaFile(configuration, configurationMap.remove("-avroschemafile"));
        }
        if (configurationMap.containsKey("-numpartitionsinstaging")) {
            try {
                TeradataPlugInConfiguration.setInputNumPartitions(configuration, Integer.parseInt(configurationMap.remove("-numpartitionsinstaging")));
            }
            catch (NumberFormatException e3) {
                ConnectorImportTool.logger.info((Object)String.format("invalid provided value for parameter %s, %s value is required for this parameter", "-numpartitionsinstaging", "long"));
                throw new ConnectorException(String.format("invalid provided value for parameter %s, %s value is required for this parameter", "-numpartitionsinstaging", "long"), e3);
            }
        }
        if (configurationMap.containsKey("-fastexportsockethost")) {
            TeradataPlugInConfiguration.setInputFastExportSocketHost(configuration, configurationMap.remove("-fastexportsockethost"));
        }
        if (configurationMap.containsKey("-fastexportsocketport")) {
            try {
                TeradataPlugInConfiguration.setInputFastExportSocketPort(configuration, Integer.parseInt(configurationMap.remove("-fastexportsocketport")));
            }
            catch (NumberFormatException e3) {
                ConnectorImportTool.logger.info((Object)String.format("invalid provided value for parameter %s, %s value is required for this parameter", "-fastexportsocketport", "integer"));
                throw new ConnectorException(String.format("invalid provided value for parameter %s, %s value is required for this parameter", "-fastexportsocketport", "integer"), e3);
            }
        }
        if (configurationMap.containsKey("-fastexportsockettimeout")) {
            try {
                TeradataPlugInConfiguration.setInputFastExportSocketTimeout(configuration, Long.parseLong(configurationMap.remove("-fastexportsockettimeout")));
            }
            catch (NumberFormatException e3) {
                ConnectorImportTool.logger.info((Object)String.format("invalid provided value for parameter %s, %s value is required for this parameter", "-fastexportsockettimeout", "long"));
                throw new ConnectorException(String.format("invalid provided value for parameter %s, %s value is required for this parameter", "-fastexportsockettimeout", "long"), e3);
            }
        }
        if (configurationMap.containsKey("-targetpartitionschema")) {
            ConnectorConfiguration.setUsePartitionedOutputFormat(configuration, true);
            final String targetPartitionSchema = configurationMap.remove("-targetpartitionschema");
            HivePlugInConfiguration.setOutputPartitionSchema(configuration, targetPartitionSchema);
            final List<String> targetPartitionList = ConnectorSchemaUtils.parseColumns(targetPartitionSchema);
            final List<String> targetPartitonColumnList = ConnectorSchemaUtils.parseColumnNames(targetPartitionList);
            if (!targetFieldNames.isEmpty()) {
                final List<String> targetFieldList = ConnectorSchemaUtils.parseColumns(targetFieldNames);
                final List<String> targetFieldColumnList = ConnectorSchemaUtils.parseColumnNames(targetFieldList);
                final int[] mapping = TeradataSchemaUtils.getColumnMapping(targetFieldColumnList, targetPartitonColumnList.toArray(new String[targetPartitonColumnList.size()]));
                final List<String> sourceFieldList = ConnectorSchemaUtils.parseColumns(sourceFieldNames);
                final List<String> sourceFieldColumnList = ConnectorSchemaUtils.parseColumnNames(sourceFieldList);
                final String[] targetPartitionArray = new String[targetPartitonColumnList.size()];
                for (int index = 0; index < mapping.length; ++index) {
                    targetPartitionArray[index] = sourceFieldColumnList.get(mapping[index]);
                }
                ConnectorConfiguration.setOutputPartitionColumnNames(configuration, ConnectorSchemaUtils.concatFieldNamesArray(targetPartitionArray));
            }
        }
        if (this.jobType.equalsIgnoreCase("hive") && HiveUtils.isHiveOutputTablePartitioned(configuration)) {
            ConnectorConfiguration.setUsePartitionedOutputFormat(configuration, true);
        }
        if (configurationMap.containsKey("-sourcerecordschema")) {
            final String sourceRecordSchema = configurationMap.remove("-sourcerecordschema");
            final ConnectorSchemaParser parser = new ConnectorSchemaParser();
            final List<String> schemaList = parser.tokenize(sourceRecordSchema);
            final List<String> newSchemaList = new ArrayList<String>();
            for (final String schema : schemaList) {
                newSchemaList.add(schema.trim());
            }
            final ConnectorRecordSchema recordSchema = new ConnectorRecordSchema(newSchemaList.size());
            int index2 = 0;
            for (final String schema2 : newSchemaList) {
                recordSchema.setFieldType(index2, ConnectorSchemaUtils.lookupDataTypeAndValidate(schema2));
                if (ConnectorSchemaUtils.lookupDataTypeAndValidate(schema2) == 1883) {
                    final int position = schema2.indexOf(40);
                    if (position == -1) {
                        throw new ConnectorException(15013);
                    }
                    recordSchema.setDataTypeConverter(index2, schema2.substring(0, position).trim());
                    final String[] parameters = ConnectorSchemaUtils.getUdfParameters(schema2);
                    final String[] newParameters = new String[parameters.length];
                    int pos = 0;
                    for (final String parameter : parameters) {
                        newParameters[pos++] = configuration.get(parameter.trim(), "");
                    }
                    recordSchema.setParameters(index2, newParameters);
                }
                ++index2;
            }
            ConnectorConfiguration.setInputConverterRecordSchema(configuration, ConnectorSchemaUtils.recordSchemaToString(ConnectorSchemaUtils.formalizeConnectorRecordSchema(recordSchema)));
            ConnectorImportTool.logger.info((Object)("source record schema is " + sourceRecordSchema));
        }
        if (configurationMap.containsKey("-targetrecordschema")) {
            final String targetRecordSchema = configurationMap.remove("-targetrecordschema");
            final ConnectorSchemaParser parser = new ConnectorSchemaParser();
            final List<String> schemaList = parser.tokenize(targetRecordSchema);
            final List<String> newSchemaList = new ArrayList<String>();
            for (final String schema : schemaList) {
                newSchemaList.add(schema.trim());
            }
            final ConnectorRecordSchema recordschema = new ConnectorRecordSchema(newSchemaList.size());
            int index2 = 0;
            for (final String schema2 : newSchemaList) {
                recordschema.setFieldType(index2, ConnectorSchemaUtils.lookupDataTypeAndValidate(schema2));
                if (ConnectorSchemaUtils.lookupDataTypeAndValidate(schema2) == 1883) {
                    throw new ConnectorException(14019);
                }
                ++index2;
            }
            ConnectorConfiguration.setOutputConverterRecordSchema(configuration, ConnectorSchemaUtils.recordSchemaToString(ConnectorSchemaUtils.formalizeConnectorRecordSchema(recordschema)));
            ConnectorImportTool.logger.info((Object)("target record schema is " + targetRecordSchema));
        }
        if (configurationMap.containsKey("-debugoption")) {
            final int debugOption = Integer.parseInt(configurationMap.remove("-debugoption"));
            ConnectorConfiguration.setDebugOption(configuration, debugOption);
        }
        if (configurationMap.containsKey("-sourcedateformat")) {
            ConnectorConfiguration.setInputDateFormat(configuration, configurationMap.remove("-sourcedateformat"));
        }
        if (configurationMap.containsKey("-sourcetimeformat")) {
            ConnectorConfiguration.setInputTimeFormat(configuration, configurationMap.remove("-sourcetimeformat"));
        }
        if (configurationMap.containsKey("-sourcetimestampformat")) {
            ConnectorConfiguration.setInputTimestampFormat(configuration, configurationMap.remove("-sourcetimestampformat"));
        }
        if (configurationMap.containsKey("-targetdateformat")) {
            ConnectorConfiguration.setOutputDateFormat(configuration, configurationMap.remove("-targetdateformat"));
        }
        if (configurationMap.containsKey("-targettimeformat")) {
            ConnectorConfiguration.setOutputTimeFormat(configuration, configurationMap.remove("-targettimeformat"));
        }
        if (configurationMap.containsKey("-targettimestampformat")) {
            ConnectorConfiguration.setOutputTimestampFormat(configuration, configurationMap.remove("-targettimestampformat"));
        }
        if (configurationMap.containsKey("-sourcetimezoneid")) {
            ConnectorConfiguration.setInputTimezoneId(configuration, configurationMap.remove("-sourcetimezoneid"));
        }
        if (configurationMap.containsKey("-targettimezoneid")) {
            ConnectorConfiguration.setOutputTimezoneId(configuration, configurationMap.remove("-targettimezoneid"));
        }
        if (configurationMap.containsKey("-stringtruncate")) {
            ConnectorConfiguration.setStringTruncate(configuration, Boolean.parseBoolean(configurationMap.remove("-stringtruncate")));
        }
        if (configurationMap.containsKey("-upt")) {
            TeradataPlugInConfiguration.setUnicodePassthrough(configuration, Boolean.parseBoolean(configurationMap.remove("-upt")));
        }
        if (!configurationMap.isEmpty()) {
            String unrecognizedParams = "";
            for (final String key : configurationMap.keySet()) {
                unrecognizedParams = unrecognizedParams + key + " ";
            }
            throw new ConnectorException(15008, new Object[] { unrecognizedParams });
        }
        return 0;
    }
    
    public static void main(final String[] args) {
        int res = 1;
        try {
            final long start = System.currentTimeMillis();
            ConnectorImportTool.logger.info((Object)("ConnectorImportTool starts at " + start));
            res = ToolRunner.run((Tool)new ConnectorImportTool(), args);
            final long end = System.currentTimeMillis();
            ConnectorImportTool.logger.info((Object)("ConnectorImportTool ends at " + end));
            ConnectorImportTool.logger.info((Object)("ConnectorImportTool time is " + (end - start) / 1000L + "s"));
        }
        catch (Throwable e) {
            ConnectorImportTool.logger.info((Object) ConnectorStringUtils.getExceptionStack(e));
            if (e instanceof ConnectorException) {
                res = ((ConnectorException)e).getCode();
            }
            else {
                res = 10000;
            }
        }
        finally {
            ConnectorImportTool.logger.info((Object)("job completed with exit code " + res));
            if (res > 255) {
                res /= 1000;
            }
            System.exit(res);
        }
    }
    
    static {
        ConnectorImportTool.logger = LogFactory.getLog((Class)ConnectorImportTool.class);
    }
}
