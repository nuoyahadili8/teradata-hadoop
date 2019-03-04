package com.teradata.connector.common.tool;

import com.teradata.connector.common.exception.ConnectorException;
import com.teradata.connector.common.exception.ConnectorException.ErrorCode;
import com.teradata.connector.common.exception.ConnectorException.ErrorMessage;
import com.teradata.connector.common.tdwallet.WalletCaller;
import com.teradata.connector.common.utils.ConnectorConfiguration;
import com.teradata.connector.common.utils.ConnectorPluginUtils;
import com.teradata.connector.common.utils.ConnectorSchemaUtils;
import com.teradata.connector.common.utils.HadoopConfigurationUtils;
import com.teradata.connector.common.utils.StandardCharsets;
import com.teradata.connector.hcat.utils.HCatPlugInConfiguration;
import com.teradata.connector.hdfs.utils.HdfsPlugInConfiguration;
import com.teradata.connector.hive.utils.HivePlugInConfiguration;
import com.teradata.connector.hive.utils.HiveUtils;
import com.teradata.connector.teradata.utils.TeradataPlugInConfiguration;
import com.teradata.connector.teradata.utils.TeradataSchemaUtils;
import com.teradata.hadoop.db.TeradataConfiguration;
import java.util.List;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.JobContext;

/**
 * @author Administrator
 */
public class ConfigurationMappingUtils {
    private static final byte[] TRIGGER_WORD;
    private static final int TRIGGER_WORD_LENGTH;

    public static void importConfigurationMapping(final Configuration configuration) throws ConnectorException {
        final String jobType = TeradataConfiguration.getInputJobType(configuration);
        if (!jobType.equalsIgnoreCase("hcat") && !jobType.equalsIgnoreCase("hdfs") && !jobType.equalsIgnoreCase("hive")) {
            throw new ConnectorException(12002);
        }
        String fileFormat = TeradataConfiguration.getInputFileFormat(configuration);
        fileFormat = HadoopConfigurationUtils.getAliasFileFormatName(fileFormat);
        if (!fileFormat.equalsIgnoreCase("textfile") && !fileFormat.equalsIgnoreCase("rcfile") && !fileFormat.equalsIgnoreCase("orcfile") && !fileFormat.equalsIgnoreCase("sequencefile") && !fileFormat.equalsIgnoreCase("avrofile")) {
            throw new ConnectorException(12016);
        }
        String method = TeradataConfiguration.getInputMethod(configuration);
        if (!method.equalsIgnoreCase("split.by.partition") && !method.equalsIgnoreCase("split.by.hash") && !method.equalsIgnoreCase("split.by.value") && !method.equalsIgnoreCase("split.by.amp") && !method.equalsIgnoreCase("internal.fastexport")) {
            throw new ConnectorException(12010);
        }
        try {
            final int numMappers = TeradataConfiguration.getInputNumMappers(configuration);
            if (numMappers < 0) {
                throw new ConnectorException(String.format("invalid provided value for parameter %s, %s value is required for this parameter", "-nummappers", " a value equals or more than zero "));
            }
            ConnectorConfiguration.setNumMappers(configuration, numMappers);
        } catch (NumberFormatException e) {
            throw new ConnectorException(String.format("invalid provided value for parameter %s, %s value is required for this parameter", "-nummappers", "integer"), e);
        }
        final String hiveconf = TeradataConfiguration.getHiveConfigureFile(configuration);
        if (hiveconf != null && !hiveconf.isEmpty()) {
            HivePlugInConfiguration.setOutputConfigureFile(configuration, hiveconf);
        }
        final String classname = TeradataConfiguration.getJDBCDriverClass(configuration);
        if (classname != null && !classname.isEmpty()) {
            TeradataPlugInConfiguration.setInputJdbcDriverClass(configuration, classname);
        }
        final String url = TeradataConfiguration.getJDBCURL(configuration);
        if (url != null && !url.isEmpty()) {
            TeradataPlugInConfiguration.setInputJdbcUrl(configuration, url);
        }
        final String username = TeradataConfiguration.getJDBCUsername(configuration);
        if (username != null && !username.isEmpty()) {
            TeradataPlugInConfiguration.setInputJdbcUserName(configuration, username);
        }
        final String password = TeradataConfiguration.getJDBCPassword(configuration);
        if (password != null && !password.isEmpty()) {
            TeradataPlugInConfiguration.setInputJdbcPassword(configuration, password);
        }
        final boolean accesslock = TeradataConfiguration.getInputAccessLock(configuration);
        TeradataPlugInConfiguration.setInputAccessLock(configuration, accesslock);
        final String queryband = TeradataConfiguration.getInputQueryBand(configuration);
        if (queryband != null && !queryband.isEmpty()) {
            TeradataPlugInConfiguration.setInputQueryBand(configuration, queryband);
        }
        final String sourcetable = TeradataConfiguration.getInputSourceTable(configuration);
        if (sourcetable != null && !sourcetable.isEmpty()) {
            TeradataPlugInConfiguration.setInputTable(configuration, sourcetable);
        }
        final String sourcedatabase = TeradataConfiguration.getInputSourceDatabase(configuration);
        if (sourcedatabase != null && !sourcedatabase.isEmpty()) {
            TeradataPlugInConfiguration.setInputDatabase(configuration, sourcedatabase);
        }
        final String sourceconditions = TeradataConfiguration.getInputSourceConditions(configuration);
        if (sourceconditions != null && !sourceconditions.isEmpty()) {
            TeradataPlugInConfiguration.setInputConditions(configuration, sourceconditions);
        }
        final String sourcefieldnames = TeradataConfiguration.getInputSourceFieldNames(configuration);
        if (sourcefieldnames != null && !sourcefieldnames.isEmpty()) {
            final String[] fieldNamesArray = ConnectorSchemaUtils.convertFieldNamesToArray(sourcefieldnames);
            TeradataPlugInConfiguration.setInputFieldNamesArray(configuration, fieldNamesArray);
        }
        final String sourcequery = TeradataConfiguration.getInputSourceQuery(configuration);
        if (sourcequery != null && !sourcequery.isEmpty()) {
            TeradataPlugInConfiguration.setInputQuery(configuration, sourcequery);
            method = "split.by.partition";
        }
        final String outputPluginName = jobType.equalsIgnoreCase("hcat") ? jobType : (jobType + "-" + fileFormat);
        ConnectorPluginUtils.configConnectorOutputPlugins(configuration, outputPluginName);
        ConnectorPluginUtils.configConnectorInputPlugins(configuration, "teradata-" + method);
        String targetdatabase = "";
        targetdatabase = TeradataConfiguration.getInputTargetDatabase(configuration);
        if (targetdatabase != null && !targetdatabase.isEmpty()) {
            if (jobType.equalsIgnoreCase("hive")) {
                HivePlugInConfiguration.setOutputDatabase(configuration, targetdatabase);
            } else if (jobType.equalsIgnoreCase("hcat")) {
                HCatPlugInConfiguration.setOutputDatabase(configuration, targetdatabase);
            }
        }
        String targettable = "";
        targettable = TeradataConfiguration.getInputTargetTable(configuration);
        if (targettable != null && !targettable.isEmpty()) {
            if (jobType.equalsIgnoreCase("hive")) {
                HivePlugInConfiguration.setOutputTable(configuration, targettable);
            } else if (jobType.equalsIgnoreCase("hcat")) {
                HCatPlugInConfiguration.setOutputTable(configuration, targettable);
            }
        }
        final String dir = configuration.get("mapred.output.dir", "");
        String targetpaths = TeradataConfiguration.getInputTargetPaths(configuration);
        if (targetpaths.isEmpty() && !dir.isEmpty()) {
            targetpaths = dir;
        }
        if (targetpaths != null && !targetpaths.isEmpty()) {
            if (jobType.equalsIgnoreCase("hive")) {
                HivePlugInConfiguration.setOutputPaths(configuration, targetpaths);
            } else if (jobType.equalsIgnoreCase("hdfs")) {
                HdfsPlugInConfiguration.setOutputPaths(configuration, targetpaths);
            }
        }
        String targetfieldnames = "";
        targetfieldnames = TeradataConfiguration.getInputTargetFieldNames(configuration);
        if (targetfieldnames != null && !targetfieldnames.isEmpty()) {
            final String[] fieldNamesArray2 = ConnectorSchemaUtils.convertFieldNamesToArray(targetfieldnames);
            if (jobType.equalsIgnoreCase("hive")) {
                HivePlugInConfiguration.setOutputFieldNamesArray(configuration, fieldNamesArray2);
            } else if (jobType.equalsIgnoreCase("hdfs")) {
                HdfsPlugInConfiguration.setOutputFieldNamesArray(configuration, fieldNamesArray2);
            } else if (jobType.equalsIgnoreCase("hcat")) {
                HCatPlugInConfiguration.setOutputFieldNamesArray(configuration, fieldNamesArray2);
            }
        }
        String targettableschema = "";
        targettableschema = TeradataConfiguration.getInputTargetTableSchema(configuration);
        if (targettableschema != null && !targettableschema.isEmpty()) {
            if (jobType.equalsIgnoreCase("hive")) {
                HivePlugInConfiguration.setOutputTableSchema(configuration, targettableschema);
            } else if (jobType.equalsIgnoreCase("hdfs") && fileFormat.equalsIgnoreCase("avrofile")) {
                HdfsPlugInConfiguration.setOutputSchema(configuration, targettableschema);
            }
        }
        final String separator = TeradataConfiguration.getInputSeparator(configuration);
        if (jobType.equalsIgnoreCase("hive")) {
            HivePlugInConfiguration.setOutputSeparator(configuration, separator);
        } else if (jobType.equalsIgnoreCase("hdfs") && fileFormat.equalsIgnoreCase("textfile")) {
            HdfsPlugInConfiguration.setOutputSeparator(configuration, separator);
        }
        final boolean forcestage = TeradataConfiguration.getInputStageForced(configuration);
        TeradataPlugInConfiguration.setInputStageTableForced(configuration, forcestage);
        final String stagetablename = TeradataConfiguration.getInputStageTableName(configuration);
        if (stagetablename != null && !stagetablename.isEmpty()) {
            TeradataPlugInConfiguration.setInputStageTableName(configuration, stagetablename);
        }
        final String stagedatabase = TeradataConfiguration.getInputStageDatabase(configuration);
        if (stagedatabase != null && !stagedatabase.isEmpty()) {
            TeradataPlugInConfiguration.setInputStageDatabase(configuration, stagedatabase);
        }
        final String stagedatabasefortable = TeradataConfiguration.getInputStageDatabaseForTable(configuration);
        if (stagedatabasefortable != null && !stagedatabasefortable.isEmpty()) {
            TeradataPlugInConfiguration.setInputStageDatabaseForTable(configuration, stagedatabasefortable);
        }
        final String stagedatabaseforview = TeradataConfiguration.getInputStageDatabaseForView(configuration);
        if (stagedatabaseforview != null && !stagedatabaseforview.isEmpty()) {
            TeradataPlugInConfiguration.setInputStageDatabaseForView(configuration, stagedatabaseforview);
        }
        final int batchsize = TeradataConfiguration.getInputBatchSize(configuration);
        TeradataPlugInConfiguration.setInputBatchSize(configuration, batchsize);
        final String splitbycolumn = TeradataConfiguration.getInputSplitByColumn(configuration);
        if (splitbycolumn != null && !splitbycolumn.isEmpty()) {
            TeradataPlugInConfiguration.setInputSplitByColumn(configuration, splitbycolumn);
        }
        final String lineseparator = TeradataConfiguration.getInputLineSeparator(configuration);
        HivePlugInConfiguration.setOutputLineSeparator(configuration, lineseparator);
        final String nullstring = TeradataConfiguration.getNullString(configuration);
        if (nullstring != null && !nullstring.isEmpty()) {
            if (jobType.equalsIgnoreCase("hive")) {
                HivePlugInConfiguration.setOutputNullString(configuration, nullstring);
            } else if (jobType.equalsIgnoreCase("hdfs")) {
                HdfsPlugInConfiguration.setOutputNullString(configuration, nullstring);
            }
        }
        final String nullnonstring = TeradataConfiguration.getNullNonString(configuration);
        if (nullnonstring != null && !nullnonstring.isEmpty()) {
            HdfsPlugInConfiguration.setOutputNullNonString(configuration, nullnonstring);
        }
        final String escapedby = TeradataConfiguration.getInputEscapedByString(configuration);
        if (escapedby != null && !escapedby.isEmpty()) {
            HdfsPlugInConfiguration.setOutputEscapedBy(configuration, escapedby);
        }
        final String enclosedby = TeradataConfiguration.getInputEnclosedByString(configuration);
        if (enclosedby != null && !enclosedby.isEmpty()) {
            HdfsPlugInConfiguration.setOutputEnclosedBy(configuration, enclosedby);
        }
        final boolean usexviews = TeradataConfiguration.getDataDictionaryUseXViews(configuration);
        TeradataPlugInConfiguration.setInputDataDictionaryUseXView(configuration, usexviews);
        final String avroschemafile = TeradataConfiguration.getAvroSchemaFilePath(configuration);
        if (avroschemafile != null && !avroschemafile.isEmpty()) {
            HdfsPlugInConfiguration.setOutputAvroSchemaFile(configuration, avroschemafile);
        }
        final long numpartitionsinstaging = TeradataConfiguration.getInputNumPartitionsInStaging(configuration);
        TeradataPlugInConfiguration.setInputNumPartitions(configuration, numpartitionsinstaging);
        final String fastexportsockethost = TeradataConfiguration.getInputFastExportSocketHost(configuration);
        if (fastexportsockethost != null && !fastexportsockethost.isEmpty()) {
            TeradataPlugInConfiguration.setInputFastExportSocketHost(configuration, fastexportsockethost);
        }
        final int fastexportsocketport = TeradataConfiguration.getInputFastExportSocketPort(configuration);
        TeradataPlugInConfiguration.setInputFastExportSocketPort(configuration, fastexportsocketport);
        final long fastexportsockettimeout = TeradataConfiguration.getInputFastExportSocketTimeout(configuration);
        TeradataPlugInConfiguration.setInputFastExportSocketTimeout(configuration, fastexportsockettimeout);
        final String targetpartitionschema = TeradataConfiguration.getInputTargetPartitionSchema(configuration);
        if (targetpartitionschema != null && !targetpartitionschema.isEmpty()) {
            ConnectorConfiguration.setUsePartitionedOutputFormat(configuration, true);
            HivePlugInConfiguration.setOutputPartitionSchema(configuration, targetpartitionschema);
            final List<String> targetPartitionList = ConnectorSchemaUtils.parseColumns(targetpartitionschema);
            final List<String> targetPartitonColNmList = ConnectorSchemaUtils.parseColumnNames(targetPartitionList);
            if (!targetfieldnames.isEmpty()) {
                final List<String> targetFieldList = ConnectorSchemaUtils.parseColumns(targetfieldnames);
                final List<String> targetFieldColNmList = ConnectorSchemaUtils.parseColumnNames(targetFieldList);
                final int[] mapping = TeradataSchemaUtils.getColumnMapping(targetFieldColNmList, targetPartitonColNmList.toArray(new String[targetPartitonColNmList.size()]));
                final List<String> sourceFieldList = ConnectorSchemaUtils.parseColumns(sourcefieldnames);
                final List<String> sourceFieldColNmList = ConnectorSchemaUtils.parseColumnNames(sourceFieldList);
                final String[] partitionArray = new String[targetPartitonColNmList.size()];
                for (int index = 0; index < mapping.length; ++index) {
                    partitionArray[index] = sourceFieldColNmList.get(mapping[index]);
                }
                ConnectorConfiguration.setOutputPartitionColumnNames(configuration, ConnectorSchemaUtils.concatFieldNamesArray(partitionArray));
            }
        }
        if (jobType.equalsIgnoreCase("hive") && HiveUtils.isHiveOutputTablePartitioned(configuration)) {
            ConnectorConfiguration.setUsePartitionedOutputFormat(configuration, true);
        }
    }

    public static void exportConfigurationMapping(final Configuration configuration) throws ConnectorException {
        final String jobType = TeradataConfiguration.getOutputJobType(configuration);
        if (!jobType.equalsIgnoreCase("hcat") && !jobType.equalsIgnoreCase("hdfs") && !jobType.equalsIgnoreCase("hive")) {
            throw new ConnectorException(13002);
        }
        String fileFormat = TeradataConfiguration.getOutputFileFormat(configuration);
        fileFormat = HadoopConfigurationUtils.getAliasFileFormatName(fileFormat);
        if (!fileFormat.equalsIgnoreCase("textfile") && !fileFormat.equalsIgnoreCase("rcfile") && !fileFormat.equalsIgnoreCase("orcfile") && !fileFormat.equalsIgnoreCase("sequencefile") && !fileFormat.equalsIgnoreCase("avrofile")) {
            throw new ConnectorException(12016);
        }
        final String method = TeradataConfiguration.getOutputMethod(configuration);
        if (!method.equalsIgnoreCase("batch.insert") && !method.equalsIgnoreCase("internal.fastload")) {
            throw new ConnectorException(13004);
        }
        final String inputPluginName = jobType.equalsIgnoreCase("hcat") ? jobType : (jobType + "-" + fileFormat);
        ConnectorPluginUtils.configConnectorInputPlugins(configuration, inputPluginName);
        ConnectorPluginUtils.configConnectorOutputPlugins(configuration, "teradata-" + method);
        final String hiveconf = TeradataConfiguration.getHiveConfigureFile(configuration);
        if (hiveconf != null && !hiveconf.isEmpty()) {
            HivePlugInConfiguration.setInputConfigureFile(configuration, hiveconf);
        }
        final String classname = TeradataConfiguration.getJDBCDriverClass(configuration);
        if (classname != null && !classname.isEmpty()) {
            TeradataPlugInConfiguration.setOutputJdbcDriverClass(configuration, classname);
        }
        final String url = TeradataConfiguration.getJDBCURL(configuration);
        if (url != null && !url.isEmpty()) {
            TeradataPlugInConfiguration.setOutputJdbcUrl(configuration, url);
        }
        final String username = TeradataConfiguration.getJDBCUsername(configuration);
        if (username != null && !username.isEmpty()) {
            TeradataPlugInConfiguration.setOutputJdbcUserName(configuration, username);
        }
        final String password = TeradataConfiguration.getJDBCPassword(configuration);
        if (password != null && !password.isEmpty()) {
            TeradataPlugInConfiguration.setOutputJdbcPassword(configuration, password);
        }
        final String dir = configuration.get("mapred.input.dir", "");
        String sourcepaths = TeradataConfiguration.getOutputSourcePaths(configuration);
        if (sourcepaths.isEmpty() && !dir.isEmpty()) {
            sourcepaths = dir;
        }
        if (sourcepaths != null && !sourcepaths.isEmpty()) {
            if (jobType.equalsIgnoreCase("hive")) {
                HivePlugInConfiguration.setInputPaths(configuration, sourcepaths);
            } else if (jobType.equalsIgnoreCase("hdfs")) {
                HdfsPlugInConfiguration.setInputPaths(configuration, sourcepaths);
            }
        }
        final String sourcedatabase = TeradataConfiguration.getOutputSourceDatabase(configuration);
        if (sourcedatabase != null && !sourcedatabase.isEmpty()) {
            if (jobType.equalsIgnoreCase("hive")) {
                HivePlugInConfiguration.setInputDatabase(configuration, sourcedatabase);
            } else if (jobType.equalsIgnoreCase("hcat")) {
                HCatPlugInConfiguration.setInputDatabase(configuration, sourcedatabase);
            }
        }
        final String sourcetable = TeradataConfiguration.getOutputSourceTable(configuration);
        if (sourcetable != null && !sourcetable.isEmpty()) {
            if (jobType.equalsIgnoreCase("hive")) {
                HivePlugInConfiguration.setInputTable(configuration, sourcetable);
            } else if (jobType.equalsIgnoreCase("hcat")) {
                HCatPlugInConfiguration.setInputTable(configuration, sourcetable);
            }
        }
        final String sourcefieldnames = TeradataConfiguration.getOutputSourceFieldNames(configuration);
        if (sourcefieldnames != null && !sourcefieldnames.isEmpty()) {
            final String[] sourceFieldNamesArray = sourcefieldnames.split(",");
            int index = 0;
            for (final String fieldName : sourceFieldNamesArray) {
                sourceFieldNamesArray[index++] = fieldName.trim();
            }
            if (jobType.equalsIgnoreCase("hive")) {
                HivePlugInConfiguration.setInputFieldNamesArray(configuration, sourceFieldNamesArray);
            } else if (jobType.equalsIgnoreCase("hdfs")) {
                HdfsPlugInConfiguration.setInputFieldNamesArray(configuration, sourceFieldNamesArray);
            } else if (jobType.equalsIgnoreCase("hcat")) {
                HCatPlugInConfiguration.setInputFieldNamesArray(configuration, sourceFieldNamesArray);
            }
        }
        final String sourcetableschema = TeradataConfiguration.getOutputSourceTableSchema(configuration);
        if (sourcetableschema != null && !sourcetableschema.isEmpty()) {
            if (jobType.equalsIgnoreCase("hive")) {
                HivePlugInConfiguration.setInputTableSchema(configuration, sourcetableschema);
            } else if (jobType.equalsIgnoreCase("hdfs")) {
                HdfsPlugInConfiguration.setInputSchema(configuration, sourcetableschema);
            }
        }
        final String targettable = TeradataConfiguration.getOutputTargetTable(configuration);
        if (targettable != null && !targettable.isEmpty()) {
            TeradataPlugInConfiguration.setOutputTable(configuration, targettable);
        }
        final String targetfieldnames = TeradataConfiguration.getOutputTargetFieldNames(configuration);
        if (targetfieldnames != null && !targetfieldnames.isEmpty()) {
            String[] fieldNamesArray = ConnectorSchemaUtils.convertFieldNamesToArray(targetfieldnames);
            fieldNamesArray = ConnectorSchemaUtils.unquoteFieldNamesArray(fieldNamesArray);
            TeradataPlugInConfiguration.setOutputFieldNamesArray(configuration, fieldNamesArray);
        }
        final int targetfieldcount = TeradataConfiguration.getOutputTargetFieldCount(configuration);
        TeradataPlugInConfiguration.setOutputFieldCount(configuration, targetfieldcount);
        final String separator = TeradataConfiguration.getOutputSeparator(configuration);
        if (jobType.equalsIgnoreCase("hive")) {
            HivePlugInConfiguration.setInputSeparator(configuration, separator);
        } else if (jobType.equalsIgnoreCase("hdfs") && fileFormat.equalsIgnoreCase("textfile")) {
            HdfsPlugInConfiguration.setInputSeparator(configuration, separator);
        }
        final String lineseparator = TeradataConfiguration.getOutputLineSeparator(configuration);
        HivePlugInConfiguration.setInputLineSeparator(configuration, lineseparator);
        final String nullstring = TeradataConfiguration.getNullString(configuration);
        if (nullstring != null && !nullstring.isEmpty()) {
            if (jobType.equalsIgnoreCase("hive")) {
                HivePlugInConfiguration.setInputNullString(configuration, nullstring);
            } else if (jobType.equalsIgnoreCase("hdfs")) {
                HdfsPlugInConfiguration.setInputNullString(configuration, nullstring);
            }
        }
        final String nullnonstring = TeradataConfiguration.getNullNonString(configuration);
        if (nullnonstring != null && !nullnonstring.isEmpty()) {
            HdfsPlugInConfiguration.setInputNullNonString(configuration, nullnonstring);
        }
        final String escapedby = TeradataConfiguration.getOutputEscapedByString(configuration);
        if (escapedby != null && !escapedby.isEmpty()) {
            HdfsPlugInConfiguration.setInputEscapedBy(configuration, escapedby);
        }
        final String enclosedby = TeradataConfiguration.getOutputEnclosedByString(configuration);
        if (enclosedby != null && !enclosedby.isEmpty()) {
            HdfsPlugInConfiguration.setInputEnclosedBy(configuration, enclosedby);
        }
        final String queryband = TeradataConfiguration.getOutputQueryBand(configuration);
        if (queryband != null && !queryband.isEmpty()) {
            TeradataPlugInConfiguration.setOutputQueryBand(configuration, queryband);
        }
        final int batchsize = TeradataConfiguration.getOutputBatchSize(configuration);
        TeradataPlugInConfiguration.setOutputBatchSize(configuration, batchsize);
        final int numMappers = TeradataConfiguration.getOutputNumMappers(configuration);
        ConnectorConfiguration.setNumMappers(configuration, numMappers);
        final int numReducers = TeradataConfiguration.getOutputNumReducers(configuration);
        ConnectorConfiguration.setNumReducers(configuration, numReducers);
        if (numMappers != 0 && numReducers != 0) {
            throw new ConnectorException(13011);
        }
        final String fastloadsockethost = TeradataConfiguration.getOutputFastloadSocketHost(configuration);
        if (fastloadsockethost != null && !fastloadsockethost.isEmpty()) {
            TeradataPlugInConfiguration.setOutputFastloadSocketHost(configuration, fastloadsockethost);
        }
        final int fastloadsocketport = TeradataConfiguration.getOutputFastloadSocketPort(configuration);
        TeradataPlugInConfiguration.setOutputFastloadSocketPort(configuration, fastloadsocketport);
        final long fastloadsockettimeout = TeradataConfiguration.getOutputFastloadSocketTimeout(configuration);
        TeradataPlugInConfiguration.setOutputFastloadSocketTimeout(configuration, fastloadsockettimeout);
        final boolean forcestage = TeradataConfiguration.getOutputStageForced(configuration);
        TeradataPlugInConfiguration.setOutputStageTableForced(configuration, forcestage);
        final String stagetablename = TeradataConfiguration.getOutputStageTableName(configuration);
        if (stagetablename != null && !stagetablename.isEmpty()) {
            TeradataPlugInConfiguration.setOutputStageTableName(configuration, stagetablename);
        }
        final String stagedatabase = TeradataConfiguration.getOutputStageDatabase(configuration);
        if (stagedatabase != null && !stagedatabase.isEmpty()) {
            TeradataPlugInConfiguration.setOutputStageDatabase(configuration, stagedatabase);
        }
        final String stagedatabasefortable = TeradataConfiguration.getOutputStageDatabaseForTable(configuration);
        if (stagedatabasefortable != null && !stagedatabasefortable.isEmpty()) {
            TeradataPlugInConfiguration.setOutputStageDatabaseForTable(configuration, stagedatabasefortable);
        }
        final String stagedatabaseforview = TeradataConfiguration.getOutputStageDatabaseForView(configuration);
        if (stagedatabaseforview != null && !stagedatabaseforview.isEmpty()) {
            TeradataPlugInConfiguration.setOutputStageDatabaseForView(configuration, stagedatabaseforview);
        }
        final String errortablename = TeradataConfiguration.getOutputErrorTableName(configuration);
        if (errortablename != null && !errortablename.isEmpty()) {
            TeradataPlugInConfiguration.setOutputErrorTableName(configuration, errortablename);
        }
        final boolean keepstagetable = TeradataConfiguration.getOutputStageTableKept(configuration);
        TeradataPlugInConfiguration.setOutputStageTableKept(configuration, keepstagetable);
        final boolean usexviews = TeradataConfiguration.getDataDictionaryUseXViews(configuration);
        TeradataPlugInConfiguration.setOutputDataDictionaryUseXView(configuration, usexviews);
        final String avroschemafile = TeradataConfiguration.getAvroSchemaFilePath(configuration);
        if (avroschemafile != null && !avroschemafile.isEmpty()) {
            HdfsPlugInConfiguration.setInputAvroSchemaFile(configuration, avroschemafile);
        }
        final String sourcepartitionschema = TeradataConfiguration.getOutputSourcePartitionSchema(configuration);
        if (sourcepartitionschema != null && !sourcepartitionschema.isEmpty()) {
            HivePlugInConfiguration.setInputPartitionSchema(configuration, sourcepartitionschema);
        }
    }

    public static void associateCredentialsForOozieJavaAction(final JobContext context) {
        if (System.getenv("HADOOP_TOKEN_FILE_LOCATION") != null) {
            context.getConfiguration().set("mapreduce.job.credentials.binary", System.getenv("HADOOP_TOKEN_FILE_LOCATION"));
            context.getConfiguration().set("tez.credentials.path", System.getenv("HADOOP_TOKEN_FILE_LOCATION"));
        }
    }

    public static void loadOozieJavaActionConf(final JobContext context) {
        if (System.getProperty("oozie.action.conf.xml") != null) {
            context.getConfiguration().addResource(new Path("file:///", System.getProperty("oozie.action.conf.xml")));
        }
    }

    public static void hideCredentials(final JobContext context) {
        final Configuration configuration = context.getConfiguration();
        final String inputUserName = TeradataPlugInConfiguration.getInputJdbcUserName(configuration);
        if (null != inputUserName && 0 != inputUserName.length()) {
            TeradataPlugInConfiguration.setInputTeradataUserName(context, inputUserName.getBytes(StandardCharsets.UTF_8));
            TeradataPlugInConfiguration.setInputJdbcUserName(configuration, "");
        }
        final String inputPassword = TeradataPlugInConfiguration.getInputJdbcPassword(configuration);
        if (null != inputPassword && 0 != inputPassword.length()) {
            TeradataPlugInConfiguration.setInputTeradataPassword(context, inputPassword.getBytes(StandardCharsets.UTF_8));
            TeradataPlugInConfiguration.setInputJdbcPassword(configuration, "");
        }
        final String outputUserName = TeradataPlugInConfiguration.getOutputJdbcUserName(configuration);
        if (null != outputUserName && 0 != outputUserName.length()) {
            TeradataPlugInConfiguration.setOutputTeradataUserName(context, outputUserName.getBytes(StandardCharsets.UTF_8));
            TeradataPlugInConfiguration.setOutputJdbcUserName(configuration, "");
        }
        final String outputPassword = TeradataPlugInConfiguration.getOutputJdbcPassword(configuration);
        if (null != outputPassword && 0 != outputPassword.length()) {
            TeradataPlugInConfiguration.setOutputTeradataPassword(context, outputPassword.getBytes(StandardCharsets.UTF_8));
            TeradataPlugInConfiguration.setOutputJdbcPassword(configuration, "");
        }
        final String username = TeradataConfiguration.getJDBCUsername(configuration);
        if (null != username && 0 != username.length()) {
            TeradataConfiguration.setJDBCUsername(configuration, "");
        }
        final String password = TeradataConfiguration.getJDBCPassword(configuration);
        if (null != password && 0 != password.length()) {
            TeradataConfiguration.setJDBCPassword(configuration, "");
        }
    }

    public static void performWalletSubstitutions(final JobContext context) throws ConnectorException {
        if (containsWalletKeyword(TeradataPlugInConfiguration.getInputTeradataUserName(context)) || containsWalletKeyword(TeradataPlugInConfiguration.getInputTeradataPassword(context)) || containsWalletKeyword(TeradataPlugInConfiguration.getOutputTeradataUserName(context)) || containsWalletKeyword(TeradataPlugInConfiguration.getOutputTeradataPassword(context))) {
            WalletCaller.performSubstitutions(context);
        }
    }

    private static boolean containsWalletKeyword(final byte[] credential) {
        if (null == ConfigurationMappingUtils.TRIGGER_WORD || null == credential) {
            return false;
        }
        final int last_start_position = credential.length - ConfigurationMappingUtils.TRIGGER_WORD_LENGTH;
        int start_position = 0;
        Label_0023:
        while (start_position <= last_start_position) {
            for (int offset = 0; offset < ConfigurationMappingUtils.TRIGGER_WORD_LENGTH; ++offset) {
                if (credential[start_position + offset] != ConfigurationMappingUtils.TRIGGER_WORD[offset]) {
                    ++start_position;
                    continue Label_0023;
                }
            }
            return true;
        }
        return false;
    }

    static {
        TRIGGER_WORD = "$tdwallet".getBytes(StandardCharsets.UTF_8);
        TRIGGER_WORD_LENGTH = ConfigurationMappingUtils.TRIGGER_WORD.length;
    }
}
