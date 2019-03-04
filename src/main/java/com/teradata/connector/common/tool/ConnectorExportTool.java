package com.teradata.connector.common.tool;

import org.apache.hadoop.mapreduce.*;
import com.teradata.connector.common.exception.*;
import com.teradata.connector.hive.utils.*;
import com.teradata.connector.teradata.utils.*;
import com.teradata.connector.hdfs.utils.*;
import com.teradata.connector.hcat.utils.*;
import com.teradata.connector.common.*;
import org.apache.hadoop.conf.*;

import java.util.*;

import org.apache.hadoop.util.*;
import com.teradata.connector.common.utils.*;
import org.apache.commons.logging.*;

public class ConnectorExportTool extends Configured implements Tool {
    private static Log logger;
    private String jobType;
    private String method;
    private String fileFormat;

    public ConnectorExportTool() {
        this.jobType = "hdfs";
        this.method = "batch.insert";
        this.fileFormat = "textfile";
    }

    @Override
    public int run(final String[] args) throws Exception {
        final Job job = new Job(this.getConf());
        try {
            if (this.processArgs((JobContext) job, args) < 0) {
                this.printHelp();
                return 0;
            }
        } catch (ConnectorException e) {
            this.printHelp();
            throw e;
        }
        return ConnectorJobRunner.runJob(job);
    }

    public void printHelp() {
        System.out.println("hadoop jar teradata-hadoop-connector.jar");
        System.out.println("    com.teradata.connector.common.tool.ConnectorExportTool");
        System.out.println("    [-conf <conf file>] (optional)");
        System.out.println("    [-jobtype <job type>] (values: hcat,hive and hdfs, default is hdfs)");
        System.out.println("    [-fileformat <file format>] (values: sequencefile, textfile, avrofile, orcfile (or orc), rcfile, and parquet, default is textfile)");
        System.out.println("    [-classname <jdbc classname>] (optional)");
        System.out.println("    [-url <url>] (optional)");
        System.out.println("    [-username <username>] (optional)");
        System.out.println("    [-password <password>] (optional)");
        System.out.println("    [-sourcepaths <source paths>] (optional, applicable for hdfs and hive jobs)]");
        System.out.println("    [-sourcedatabase <database>] (optional");
        System.out.println("    [-sourcetable <table>] (optional)");
        System.out.println("    [-sourcefieldnames <fields>] (optional, comma separated format");
        System.out.println("    [-sourcetableschema <schema>] (optional, comma separated format");
        System.out.println("    [-sourcepartitionschema <schema>] (optional, comma separated format, required with -sourcetableschema specified");
        System.out.println("    [-sourcerecordschema <record schema>] (optional, comma delimited format)");
        System.out.println("    [-targetrecordschema <record schema>] (optional, comma delimited format)");
        System.out.println("    [-targettable <table>] (required");
        System.out.println("    [-targetfieldnames <fields>] (optional, comma separated format, use -targetfieldnames or -targetfieldcount but not both");
        System.out.println("    [-targetfieldcount <count>] (optional, must be a number, use -targetfieldnames or -targetfieldcount but not both)");
        System.out.println("    [-separator <separator>] (optional, field separator)");
        System.out.println("    [-lineseparator <lineseparator>] (optional, used to separate different lines, only useful for hive and hcat job)");
        System.out.println("    [-enclosedby <enclosed-by-character> (optinal, used to enclose text, only useful for hdfs job)]");
        System.out.println("    [-escapedby <escaped-by-character> (optional, used to escape special characters, only useful for hdfs job)]");
        System.out.println("    [-nullstring <string>] (optional, a string to replace null value of string type)");
        System.out.println("    [-nullnonstring <string>] (optional, a string to replace null value of non-string type, only useful for hdfs job)");
        System.out.println("    [-queryband <queryband>] (optional)");
        System.out.println("    [-method <method>] (optional should be one of batch.insert, multiple.fastload and internal.                              fastload, default is batch.insert");
        System.out.println("    [-batchsize <size>] (optional, default is 10000)");
        System.out.println("    [-nummappers <num>] (optional, to limit number of mapper tasks, use in map phase only)");
        System.out.println("    [-throttlemappers <true>] (optional, overwrite nummappers value based on cluster resources)");
        System.out.println("    [-minmappers <num>] (optional, only overwrite nummappers value if/when minmappers are available)");
        System.out.println("    [-numreducers <num>] (optional, to limit number of reducer tasks, use in reduce phase only)");
        System.out.println("    [-fastloadsockethost <host>] (optional)");
        System.out.println("    [-fastloadsocketport <port>] (optional)");
        System.out.println("    [-fastloadsockettimeout <time(ms)>] (optional, default is 8 minutes)");
        System.out.println("    [-forcestage <true>] (optional, force to use stage table, default is false");
        System.out.println("    [-stagetablename <table name>] (optional)");
        System.out.println("    [-stagedatabase <database>] (optional)");
        System.out.println("    [-errortablename <table name>] (optional)");
        System.out.println("    [-errortabledatabase <database name>] (optional, database where error tables reside)");
        System.out.println("    [-errorlimit <num records>] (optional, acceptable number of records in error table)");
        System.out.println("    [-keepstagetable <true>] (optional, whether to keep stage table when export job fails during           exporting data from stage table to target table, default is dropping stage table)");
        System.out.println("    [-hiveconf <path>] (optional, required for hive and hcat jobs launched on non-name nodes)]");
        System.out.println("    [-usexviews <true|false>] (optional, default is false)");
        System.out.println("    [-avroschema <avro schema>] (optional, an inline Avro schema definition)");
        System.out.println("    [-avroschemafile <path>] (optional, a file path for Avro schema definition)");
        System.out.println("    [-debugoption <debug option value>] (optional, a debug option to close some stages of the whole hadoop job)");
        System.out.println("    [-sourcedateformat <date format>] (optional, a default date format for all converters that convert string to date)");
        System.out.println("    [-targetdateformat <date format>] (optional, a default date format for all converters that convert date  to string )");
        System.out.println("    [-sourcetimeformat <time format>] (optional, a time date format for all converters that convert string to time)");
        System.out.println("    [-targettimeformat <time format>] (optional, a time date format for all converters that convert time  to string )");
        System.out.println("    [-sourcetimestampformat <timestamp format>] (optional, a timestamp date format for all converters that convert string to timestamp)");
        System.out.println("    [-targettimestampformat <timestamp format>] (optional, a timestamp date format for all converters that convert timestamp  to string )");
        System.out.println("    [-sourcetimezoneid <timezone id>] (optional, a default input timezone id for all converters that convert timestamp to another timestamp");
        System.out.println("    [-targettimezoneid <timezone id>] (optional, a default output timezone id for all converters that convert timestamp to another timestamp");
        System.out.println("    [-stringtruncate <false>] (optional, do not automatically truncate strings and error)");
        System.out.println("    [-sourcepartitionpath <string>] (optional, the partition of hive-table )");
        System.out.println("    [-h|help] (optional)");
        System.out.println("");
    }

    private int processArgs(final JobContext context, final String[] args) throws ConnectorException {
        final Map<String, String> configurationMap = new HashMap<String, String>();
        int i = 0;
        final int length = args.length;
        if (length < 1) {
            throw new ConnectorException(13020);
        }
        while (i < length) {
            final String arg = args[i];
            if (arg == null || arg.isEmpty()) {
                return 0;
            }
            if (arg.charAt(0) != '-') {
                throw new ConnectorException(13001);
            }
            if (arg.equalsIgnoreCase("-h") || arg.equalsIgnoreCase("-help")) {
                return -1;
            }
            if (++i >= length) {
                throw new ConnectorException(13001);
            }
            final String value = args[i];
            if (value == null) {
                throw new ConnectorException(13001);
            }
            configurationMap.put(arg, value);
            ++i;
        }
        final Configuration configuration = context.getConfiguration();
        configuration.addResource("teradata-export-properties.xml");
        if (configurationMap.containsKey("-jobtype")) {
            this.jobType = configurationMap.remove("-jobtype").toLowerCase();
        }
        if (!this.jobType.equalsIgnoreCase("hcat") && !this.jobType.equalsIgnoreCase("hdfs") && !this.jobType.equalsIgnoreCase("hive")) {
            throw new ConnectorException(13002);
        }
        if (this.jobType.equals("hcat")) {
            ConnectorExportTool.logger.warn((Object) "The TDCH hcatalog plugin is deprecated, and will be removed in a future release");
        }
        if (configurationMap.containsKey("-fileformat")) {
            this.fileFormat = configurationMap.remove("-fileformat").toLowerCase();
            this.fileFormat = HadoopConfigurationUtils.getAliasFileFormatName(this.fileFormat);
            if (this.fileFormat.equals("parquet")) {
                HivePlugInConfiguration.setReadSupportClass(configuration);
            }
        }
        if (!this.fileFormat.equalsIgnoreCase("textfile") && !this.fileFormat.equalsIgnoreCase("rcfile") && !this.fileFormat.equalsIgnoreCase("orcfile") && !this.fileFormat.equalsIgnoreCase("sequencefile") && !this.fileFormat.equalsIgnoreCase("avrofile") && !this.fileFormat.equalsIgnoreCase("parquet")) {
            throw new ConnectorException(13016);
        }
        if (configurationMap.containsKey("-method")) {
            this.method = configurationMap.remove("-method").toLowerCase();
        }
        if (!this.method.equalsIgnoreCase("batch.insert") && !this.method.equalsIgnoreCase("internal.fastload") && !this.method.equalsIgnoreCase("idata.stream")) {
            throw new ConnectorException(13004);
        }
        final String inputPluginName = this.jobType.equalsIgnoreCase("hcat") ? this.jobType : (this.jobType + "-" + this.fileFormat);
        final String outputPluginName = this.method.equalsIgnoreCase("idata.stream") ? this.method : ("teradata-" + this.method);
        ConnectorPluginUtils.configConnectorInputPlugins(configuration, inputPluginName);
        ConnectorPluginUtils.configConnectorOutputPlugins(configuration, outputPluginName);
        if (configurationMap.containsKey("-hiveconf")) {
            HivePlugInConfiguration.setInputConfigureFile(configuration, configurationMap.remove("-hiveconf"));
        }
        if (configurationMap.containsKey("-classname")) {
            TeradataPlugInConfiguration.setOutputJdbcDriverClass(configuration, configurationMap.remove("-classname"));
        }
        if (configurationMap.containsKey("-url")) {
            TeradataPlugInConfiguration.setOutputJdbcUrl(configuration, configurationMap.remove("-url"));
        }
        if (configurationMap.containsKey("-username")) {
            TeradataPlugInConfiguration.setOutputTeradataUserName(context, configurationMap.remove("-username").getBytes(StandardCharsets.UTF_8));
        }
        if (configurationMap.containsKey("-password")) {
            TeradataPlugInConfiguration.setOutputTeradataPassword(context, configurationMap.remove("-password").getBytes(StandardCharsets.UTF_8));
        }
        if (configurationMap.containsKey("-sourcepaths")) {
            final String sourcePaths = configurationMap.remove("-sourcepaths");
            if (this.jobType.equalsIgnoreCase("hive")) {
                HivePlugInConfiguration.setInputPaths(configuration, sourcePaths);
            } else if (this.jobType.equalsIgnoreCase("hdfs")) {
                HdfsPlugInConfiguration.setInputPaths(configuration, sourcePaths);
            } else {
                ConnectorExportTool.logger.warn((Object) "user provided path for job types excepts Hive and HDFS");
            }
        }
        if (configurationMap.containsKey("-sourcedatabase")) {
            final String sourceDatabase = configurationMap.remove("-sourcedatabase");
            if (this.jobType.equalsIgnoreCase("hive")) {
                HivePlugInConfiguration.setInputDatabase(configuration, sourceDatabase);
            } else if (this.jobType.equalsIgnoreCase("hcat")) {
                HCatPlugInConfiguration.setInputDatabase(configuration, sourceDatabase);
            } else {
                ConnectorExportTool.logger.warn((Object) "user provided database for job types excepts Hive and HCat");
            }
        }
        if (configurationMap.containsKey("-sourcetable")) {
            final String sourceTable = configurationMap.remove("-sourcetable");
            if (this.jobType.equalsIgnoreCase("hive")) {
                HivePlugInConfiguration.setInputTable(configuration, sourceTable);
            } else if (this.jobType.equalsIgnoreCase("hcat")) {
                HCatPlugInConfiguration.setInputTable(configuration, sourceTable);
            } else {
                ConnectorExportTool.logger.warn((Object) "user provided table for job types excepts Hive and HCat");
            }
        }
        if (configurationMap.containsKey("-sourcefieldnames")) {
            final String sourceFieldNames = configurationMap.remove("-sourcefieldnames");
            final String[] sourceFieldNamesArray = sourceFieldNames.split(",");
            int index = 0;
            for (final String fieldName : sourceFieldNamesArray) {
                sourceFieldNamesArray[index++] = fieldName.trim();
            }
            if (this.jobType.equalsIgnoreCase("hive")) {
                HivePlugInConfiguration.setInputFieldNamesArray(configuration, sourceFieldNamesArray);
            } else if (this.jobType.equalsIgnoreCase("hdfs")) {
                HdfsPlugInConfiguration.setInputFieldNamesArray(configuration, sourceFieldNamesArray);
            } else if (this.jobType.equalsIgnoreCase("hcat")) {
                HCatPlugInConfiguration.setInputFieldNamesArray(configuration, sourceFieldNamesArray);
            }
        }

        /**
         * add hive partition by anliang
         */
        if (configurationMap.containsKey("-sourcepartitionpath")) {
            if (!this.jobType.equals("hive")) {
                throw new ConnectorException(15099);
            }
            configuration.set("sourcepartitionpath", configurationMap.remove("-sourcepartitionpath").toLowerCase());
        }

        if (configurationMap.containsKey("-sourcetableschema")) {
            final String sourceTableSchema = configurationMap.remove("-sourcetableschema");
            if (this.jobType.equalsIgnoreCase("hive")) {
                HivePlugInConfiguration.setInputTableSchema(configuration, sourceTableSchema);
            } else if (this.jobType.equalsIgnoreCase("hdfs")) {
                HdfsPlugInConfiguration.setInputSchema(configuration, sourceTableSchema);
            } else {
                ConnectorExportTool.logger.warn((Object) "user provided table schema for job types excepts Hive and HDFS");
            }
        }
        if (configurationMap.containsKey("-targettable")) {
            TeradataPlugInConfiguration.setOutputTable(configuration, configurationMap.remove("-targettable"));
        }
        if (configurationMap.containsKey("-targetfieldnames")) {
            final String targetFieldNames = configurationMap.remove("-targetfieldnames");
            String[] targetfieldNamesArray = ConnectorSchemaUtils.convertFieldNamesToArray(targetFieldNames);
            targetfieldNamesArray = ConnectorSchemaUtils.unquoteFieldNamesArray(targetfieldNamesArray);
            TeradataPlugInConfiguration.setOutputFieldNamesArray(configuration, targetfieldNamesArray);
        }
        if (configurationMap.containsKey("-targetfieldcount")) {
            try {
                TeradataPlugInConfiguration.setOutputFieldCount(configuration, Integer.parseInt(configurationMap.remove("-targetfieldcount")));
            } catch (NumberFormatException e) {
                ConnectorExportTool.logger.info((Object) String.format("invalid provided value for parameter %s, %s value is required for this parameter", "-targetfieldcount", "integer"));
                throw new ConnectorException(String.format("invalid provided value for parameter %s, %s value is required for this parameter", "-targetfieldcount", "integer"), e);
            }
        }
        if (configurationMap.containsKey("-separator")) {
            final String separator = configurationMap.remove("-separator");
            if (this.jobType.equalsIgnoreCase("hive")) {
                HivePlugInConfiguration.setInputSeparator(configuration, separator);
            } else if (this.jobType.equalsIgnoreCase("hdfs") && this.fileFormat.equalsIgnoreCase("textfile")) {
                HdfsPlugInConfiguration.setInputSeparator(configuration, separator);
            } else {
                ConnectorExportTool.logger.warn((Object) "user provided separator for job types excepts Hive and HDFS");
            }
        }
        if (configurationMap.containsKey("-lineseparator")) {
            HivePlugInConfiguration.setInputLineSeparator(configuration, configurationMap.remove("-lineseparator"));
        }
        if (configurationMap.containsKey("-nullstring")) {
            final String nullString = configurationMap.remove("-nullstring");
            if (this.jobType.equalsIgnoreCase("hive")) {
                HivePlugInConfiguration.setInputNullString(configuration, nullString);
            } else if (this.jobType.equalsIgnoreCase("hdfs")) {
                HdfsPlugInConfiguration.setInputNullString(configuration, nullString);
            } else {
                ConnectorExportTool.logger.warn((Object) "user provided null string for job types excepts Hive and HDFS");
            }
        }
        if (configurationMap.containsKey("-nullnonstring")) {
            HdfsPlugInConfiguration.setInputNullNonString(configuration, configurationMap.remove("-nullnonstring"));
        }
        if (configurationMap.containsKey("-escapedby")) {
            HdfsPlugInConfiguration.setInputEscapedBy(configuration, configurationMap.remove("-escapedby"));
        }
        if (configurationMap.containsKey("-enclosedby")) {
            HdfsPlugInConfiguration.setInputEnclosedBy(configuration, configurationMap.remove("-enclosedby"));
        }
        if (configurationMap.containsKey("-queryband")) {
            TeradataPlugInConfiguration.setOutputQueryBand(configuration, configurationMap.remove("-queryband"));
        }
        if (configurationMap.containsKey("-batchsize")) {
            try {
                TeradataPlugInConfiguration.setOutputBatchSize(configuration, Integer.parseInt(configurationMap.remove("-batchsize")));
            } catch (NumberFormatException e) {
                ConnectorExportTool.logger.info((Object) String.format("invalid provided value for parameter %s, %s value is required for this parameter", "-batchsize", "integer"));
                throw new ConnectorException(String.format("invalid provided value for parameter %s, %s value is required for this parameter", "-batchsize", "integer"), e);
            }
        }
        if (configurationMap.containsKey("-nummappers")) {
            try {
                final int numMappers = Integer.parseInt(configurationMap.remove("-nummappers"));
                if (numMappers < 1) {
                    throw new ConnectorException(String.format("invalid provided value for parameter %s, %s value is required for this parameter", "-nummappers", " greater than zero "));
                }
                ConnectorConfiguration.setNumMappers(configuration, numMappers);
            } catch (NumberFormatException e) {
                ConnectorExportTool.logger.info((Object) String.format("invalid provided value for parameter %s, %s value is required for this parameter", "-nummappers", "integer"));
                throw new ConnectorException(String.format("invalid provided value for parameter %s, %s value is required for this parameter", "-nummappers", "integer"), e);
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
            } catch (NumberFormatException e) {
                throw new ConnectorException(String.format("invalid provided value for parameter %s, %s value is required for this parameter", "-minmappers", "integer"), e);
            }
        }
        int numReducers = 0;
        if (configurationMap.containsKey("-numreducers")) {
            try {
                numReducers = Integer.parseInt(configurationMap.remove("-numreducers"));
                ConnectorConfiguration.setNumReducers(configuration, numReducers);
            } catch (NumberFormatException e2) {
                ConnectorExportTool.logger.info((Object) String.format("invalid provided value for parameter %s, %s value is required for this parameter", "-numreducers", "integer"));
                throw new ConnectorException(String.format("invalid provided value for parameter %s, %s value is required for this parameter", "-numreducers", "integer"), e2);
            }
        }
        if (numReducers != 0) {
            ConnectorExportTool.logger.warn((Object) "You are trying calling reducer phase");
        }
        if (configurationMap.containsKey("-fastloadsockethost")) {
            TeradataPlugInConfiguration.setOutputFastloadSocketHost(configuration, configurationMap.remove("-fastloadsockethost"));
        }
        if (configurationMap.containsKey("-fastloadsocketport")) {
            try {
                TeradataPlugInConfiguration.setOutputFastloadSocketPort(configuration, Integer.parseInt(configurationMap.remove("-fastloadsocketport")));
            } catch (NumberFormatException e2) {
                ConnectorExportTool.logger.info((Object) String.format("invalid provided value for parameter %s, %s value is required for this parameter", "-fastloadsocketport", "integer"));
                throw new ConnectorException(String.format("invalid provided value for parameter %s, %s value is required for this parameter", "-fastloadsocketport", "integer"), e2);
            }
        }
        if (configurationMap.containsKey("-fastloadsockettimeout")) {
            try {
                TeradataPlugInConfiguration.setOutputFastloadSocketTimeout(configuration, Long.parseLong(configurationMap.remove("-fastloadsockettimeout")));
            } catch (NumberFormatException e2) {
                ConnectorExportTool.logger.info((Object) String.format("invalid provided value for parameter %s, %s value is required for this parameter", "-fastloadsockettimeout", "long"));
                throw new ConnectorException(String.format("invalid provided value for parameter %s, %s value is required for this parameter", "-fastloadsockettimeout", "long"), e2);
            }
        }
        if (configurationMap.containsKey("-forcestage")) {
            TeradataPlugInConfiguration.setOutputStageTableForced(configuration, Boolean.parseBoolean(configurationMap.remove("-forcestage")));
        }
        if (configurationMap.containsKey("-stagetablename")) {
            TeradataPlugInConfiguration.setOutputStageTableName(configuration, configurationMap.remove("-stagetablename"));
        }
        if (configurationMap.containsKey("-stagedatabase")) {
            TeradataPlugInConfiguration.setOutputStageDatabase(configuration, configurationMap.remove("-stagedatabase"));
        }
        if (configurationMap.containsKey("-errortablename")) {
            TeradataPlugInConfiguration.setOutputErrorTableName(configuration, configurationMap.remove("-errortablename"));
        }
        if (configurationMap.containsKey("-errortabledatabase")) {
            TeradataPlugInConfiguration.setOutputErrorTableDatabase(configuration, configurationMap.remove("-errortabledatabase"));
        }
        if (configurationMap.containsKey("-errorlimit")) {
            try {
                TeradataPlugInConfiguration.setOutputFastloadErrorLimit(configuration, Long.parseLong(configurationMap.remove("-errorlimit")));
            } catch (NumberFormatException e2) {
                ConnectorExportTool.logger.info((Object) String.format("invalid provided value for parameter %s, %s value is required for this parameter", "-errorlimit", "long"));
                throw new ConnectorException(String.format("invalid provided value for parameter %s, %s value is required for this parameter", "-errorlimit", "long"), e2);
            }
        }
        if (configurationMap.containsKey("-keepstagetable")) {
            TeradataPlugInConfiguration.setOutputStageTableKept(configuration, Boolean.parseBoolean(configurationMap.remove("-keepstagetable")));
        }
        if (configurationMap.containsKey("-jobclientoutput")) {
            HdfsAppender.addHDFSAppender(configurationMap.remove("-jobclientoutput"));
        }
        if (configurationMap.containsKey("-usexviews")) {
            TeradataPlugInConfiguration.setOutputDataDictionaryUseXView(configuration, Boolean.parseBoolean(configurationMap.remove("-usexviews")));
        }
        if (configurationMap.containsKey("-avroschema")) {
            HdfsPlugInConfiguration.setInputAvroSchema(configuration, configurationMap.remove("-avroschema"));
        }
        if (configurationMap.containsKey("-avroschemafile")) {
            HdfsPlugInConfiguration.setInputAvroSchemaFile(configuration, configurationMap.remove("-avroschemafile"));
        }
        if (configurationMap.containsKey("-sourcepartitionschema")) {
            HivePlugInConfiguration.setInputPartitionSchema(configuration, configurationMap.remove("-sourcepartitionschema"));
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
            ConnectorExportTool.logger.info((Object) ("source record schema is " + sourceRecordSchema));
        }
        if (configurationMap.containsKey("-targetrecordschema")) {
            final String targetRecordSchema = configurationMap.remove("-targetrecordschema");
            final ConnectorSchemaParser parser = new ConnectorSchemaParser();
            final List<String> schemaList = parser.tokenize(targetRecordSchema);
            final List<String> newSchemaList = new ArrayList<String>();
            for (final String schema : schemaList) {
                newSchemaList.add(schema.trim());
            }
            final ConnectorRecordSchema recordSchema = new ConnectorRecordSchema(newSchemaList.size());
            int index2 = 0;
            for (final String schema2 : newSchemaList) {
                recordSchema.setFieldType(index2, ConnectorSchemaUtils.lookupDataTypeAndValidate(schema2));
                if (ConnectorSchemaUtils.lookupDataTypeAndValidate(schema2) == 1883) {
                    throw new ConnectorException(14019);
                }
                ++index2;
            }
            ConnectorConfiguration.setOutputConverterRecordSchema(configuration, ConnectorSchemaUtils.recordSchemaToString(ConnectorSchemaUtils.formalizeConnectorRecordSchema(recordSchema)));
            ConnectorExportTool.logger.info((Object) ("target record schema is " + targetRecordSchema));
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
            throw new ConnectorException(15008, new Object[]{unrecognizedParams});
        }
        return 0;
    }

    public static void main(final String[] args) {
        int res = 1;
        try {
            final long start = System.currentTimeMillis();
            ConnectorExportTool.logger.info((Object) ("ConnectorExportTool starts at " + start));
            res = ToolRunner.run((Tool) new ConnectorExportTool(), args);
            final long end = System.currentTimeMillis();
            ConnectorExportTool.logger.info((Object) ("ConnectorExportTool ends at " + end));
            ConnectorExportTool.logger.info((Object) ("ConnectorExportTool time is " + (end - start) / 1000L + "s"));
        } catch (Throwable e) {
            ConnectorExportTool.logger.info((Object) ConnectorStringUtils.getExceptionStack(e));
            if (e instanceof ConnectorException) {
                res = ((ConnectorException) e).getCode();
            } else {
                res = 10000;
            }
        } finally {
            ConnectorExportTool.logger.info((Object) ("job completed with exit code " + res));
            if (res > 255) {
                res /= 1000;
            }
            System.exit(res);
        }
    }

    static {
        ConnectorExportTool.logger = LogFactory.getLog((Class) ConnectorExportTool.class);
    }
}
