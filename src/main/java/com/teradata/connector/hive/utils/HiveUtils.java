package com.teradata.connector.hive.utils;

import com.teradata.connector.common.exception.ConnectorException;
import com.teradata.connector.common.utils.*;
import com.teradata.connector.hive.HiveAvroFileOutputFormat;
import com.teradata.connector.hive.HiveORCFileOutputFormat;
import com.teradata.connector.hive.HiveParquetFileOutputFormat;
import com.teradata.connector.hive.HiveRCFileOutputFormat;
import com.teradata.connector.hive.HiveSequenceFileOutputFormat;
import com.teradata.connector.hive.HiveTextFileOutputFormat;

import java.io.*;
import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Set;
import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.parsers.ParserConfigurationException;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.cli.CliSessionState;
import org.apache.hadoop.hive.common.FileUtils;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.metastore.HiveMetaStoreClient;
import org.apache.hadoop.hive.metastore.TableType;
import org.apache.hadoop.hive.metastore.api.*;
import org.apache.hadoop.hive.metastore.api.InvalidObjectException;
import org.apache.hadoop.hive.ql.CommandNeedRetryException;
import org.apache.hadoop.hive.ql.Driver;
import org.apache.hadoop.hive.ql.processors.CommandProcessorResponse;
import org.apache.hadoop.hive.ql.session.SessionState;
import org.apache.hadoop.hive.shims.HadoopShims;
import org.apache.hadoop.hive.shims.ShimLoader;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.thrift.TException;
import org.w3c.dom.Document;
import org.w3c.dom.Element;
import org.w3c.dom.Node;
import org.w3c.dom.NodeList;
import org.xml.sax.SAXException;


/**
 * @author Administrator
 */
public class HiveUtils {
    private static Log logger = LogFactory.getLog((Class) HiveUtils.class);;
    public static final String LIST_COLUMNS = "columns";
    public static final String DEFAULT_DATABASE = "default";
    public static final String LIST_COLUMN_TYPES = "columns.types";
    public static final Long RANDOMSEED = 999999L;;
    protected static final String SUPPORTED_TEXTFILE_INPUTFORMAT = "org.apache.hadoop.mapred.TextInputFormat";
    protected static final String SUPPORTED_TEXTFILE_OUTPUTFORMAT = "org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat";
    protected static final String SUPPORTED_SEQUENCEFILE_INPUTFORMAT = "org.apache.hadoop.mapred.SequenceFileInputFormat";
    protected static final String SUPPORTED_SEQUENCEFILE_OUTPUTFORMAT = "org.apache.hadoop.hive.ql.io.HiveSequenceFileOutputFormat";
    protected static final String SUPPORTED_RCFILE_INPUTFORMAT = "org.apache.hadoop.hive.ql.io.RCFileInputFormat";
    protected static final String SUPPORTED_RCFILE_OUTPUTFORMAT = "org.apache.hadoop.hive.ql.io.RCFileOutputFormat";
    protected static final String SUPPORTED_ORCFILE_INPUTFORMAT = "org.apache.hadoop.hive.ql.io.orc.OrcInputFormat";
    protected static final String SUPPORTED_ORCFILE_OUTPUTFORMAT = "org.apache.hadoop.hive.ql.io.orc.OrcOutputFormat";
    protected static final String SUPPORTED_PARQUET_INPUTFORMAT = "org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat";
    protected static final String SUPPORTED_PARQUET_OUTPUTFORMAT = "org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat";
    protected static final String SUPPORTED_AVRO_INPUTFORMAT = "org.apache.hadoop.hive.ql.io.avro.AvroContainerInputFormat";
    protected static final String SUPPORTED_AVRO_OUTPUTFORMAT = "org.apache.hadoop.hive.ql.io.avro.AvroContainerOutputFormat";
    protected static final String SUPPORTED_LAZY_SIMPLE_SERDE = "org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe";
    protected static final String SUPPORTED_COLUMNAR_SERDE = "org.apache.hadoop.hive.serde2.columnar.ColumnarSerDe";
    protected static final String SUPPORTED_LB_COLUMNAR_SERDE = "org.apache.hadoop.hive.serde2.columnar.LazyBinaryColumnarSerDe";
    protected static final String SUPPORTED_ORC_SERDE = "org.apache.hadoop.hive.ql.io.orc.OrcSerde";
    protected static final String SUPPORTED_PARQUET_SERDE = "org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe";
    protected static final String SUPPORTED_AVRO_SERDE = "org.apache.hadoop.hive.serde2.avro.AvroSerDe";
    protected static final String SLASH = "/";
    protected static final String COMMA = ",";
    protected static final String EQUALS = "=";
    protected static final String QUOTE = "\"";
    protected static final char ESCAPE_CHAR = '\\';
    protected static final char SPACE_CHAR = ' ';
    protected static final String USAGE_VALUES = "values";
    protected static final String USAGE_REGEX = "regex";
    protected static final String HIVE_FIRST_FILE_NAME = "00000";
    protected static final Text HIVE_DEFAULT_NULL_SEQUENCE = new Text("\\N");;
    protected static final byte HIVE_DEFAULT_ESCAPE_CHAR = 92;
    public static final byte[] DefaultSeparators = new byte[]{1, 2, 3};;

    public static void loadHiveConf(final Configuration configuration, final ConnectorConfiguration.direction direction) throws ConnectorException {
        try {
            String hiveConfFile;
            if (direction == ConnectorConfiguration.direction.input) {
                hiveConfFile = HivePlugInConfiguration.getInputConfigureFile(configuration);
            } else {
                hiveConfFile = HivePlugInConfiguration.getOutputConfigureFile(configuration);
            }
            if (hiveConfFile.equals("")) {
                return;
            }
            final Path hivePath = new Path(hiveConfFile);
            final FileSystem fs = hivePath.getFileSystem(configuration);
            HiveUtils.logger.info((Object) ("Using hive-site.xml: " + fs.makeQualified(hivePath)));
            if (!fs.exists(hivePath)) {
                throw new ConnectorException(30007);
            }
            if (!fs.isFile(hivePath)) {
                throw new ConnectorException(30008);
            }
            final InputStream in = new BufferedInputStream((InputStream) fs.open(hivePath));
            readConfFromInputStream(in, configuration);
            in.close();
        } catch (ParserConfigurationException e) {
            HiveUtils.logger.error((Object) ConnectorStringUtils.getExceptionStack(e));
        } catch (SAXException e2) {
            HiveUtils.logger.error((Object) ConnectorStringUtils.getExceptionStack(e2));
        } catch (IOException e3) {
            throw new ConnectorException(e3.getMessage(), e3);
        }
    }

    private static final void readConfFromInputStream(final InputStream in, final Configuration configuration) throws ParserConfigurationException, SAXException, IOException {
        final DocumentBuilder docbulider = DocumentBuilderFactory.newInstance().newDocumentBuilder();
        Document confDoc = null;
        Element rootElement = null;
        confDoc = docbulider.parse(in);
        rootElement = confDoc.getDocumentElement();
        if (!"configuration".equals(rootElement.getTagName())) {
            throw new ConnectorException(32010);
        }
        final NodeList parameters = rootElement.getChildNodes();
        final Set<String> finalParams = new HashSet<String>();
        for (int i = 0; i < parameters.getLength(); ++i) {
            final Node propNode = parameters.item(i);
            if (propNode instanceof Element) {
                final Element prop = (Element) propNode;
                if (prop.getTagName().equals("configuration")) {
                    throw new ConnectorException(32010);
                }
                if (!prop.getTagName().equals("property")) {
                    HiveUtils.logger.warn((Object) "expected <property> tag");
                }
                final NodeList childNodes = prop.getChildNodes();
                String attr = null;
                String value = null;
                boolean finalParam = false;
                for (int j = 0; j < childNodes.getLength(); ++j) {
                    final Node node = childNodes.item(j);
                    if (node instanceof Element) {
                        final Element field = (Element) node;
                        if (field.getTagName().equals("name") && field.hasChildNodes()) {
                            attr = ((org.w3c.dom.Text) field.getFirstChild()).getData().trim();
                        }
                        if (field.getTagName().equals("value") && field.hasChildNodes()) {
                            value = ((org.w3c.dom.Text) field.getFirstChild()).getData();
                        }
                        if (field.getTagName().equals("final") && field.hasChildNodes()) {
                            finalParam = ((org.w3c.dom.Text) field.getFirstChild()).getData().equals("true");
                        }
                    }
                }
                if (attr != null && value != null) {
                    if (!finalParams.contains(attr)) {
                        configuration.set(attr, value);
                    } else if (!value.equals(configuration.get(attr))) {
                        HiveUtils.logger.warn((Object) ("attempt to override final parameter: " + attr + ":  From " + configuration.get(attr) + " to " + value));
                    }
                    if (finalParam) {
                        finalParams.add(attr);
                    }
                }
            }
        }
    }

    public static final String listToString(final List<String> list) {
        final StringBuilder buff = new StringBuilder();
        for (final String item : list) {
            buff.append(item.trim()).append(",");
        }
        final int len = buff.length() - 1;
        buff.delete(len, len);
        return buff.toString();
    }

    public static void loadDataintoHiveTable(final Configuration configuration, final String databaseName, final String tableName, final HiveConf hiveConf, final HiveMetaStoreClient client) throws ConnectorException {
        CliSessionState ss = new CliSessionState(hiveConf);
        HiveUtils.logger.info((Object) "load data into hive table");
        ss.in = System.in;
        try {
            ss.out = new PrintStream(System.out, true, "UTF-8");
            ss.err = new PrintStream(System.err, true, "UTF-8");
        } catch (UnsupportedEncodingException e) {
            throw new ConnectorException(e.getMessage(), e);
        }
        SessionState.start((SessionState) ss);
        Driver driver = new Driver(hiveConf);
        try {
            String outputPath = HivePlugInConfiguration.getOutputPaths(configuration);
            final Path output = new Path(outputPath);
            final FileSystem fs = FileSystem.get(output.toUri(), configuration);
            if (fs.isFile(output)) {
                final int lastSlashPosition = outputPath.lastIndexOf("/");
                outputPath = outputPath.substring(0, lastSlashPosition + 1);
            } else {
                for (final FileStatus stat : fs.listStatus(output)) {
                    final Path filePath = stat.getPath();
                    final String name = filePath.getName();
                    if (name.startsWith("_") || name.startsWith(".") || stat.getLen() == 0L) {
                        fs.delete(filePath, true);
                    }
                }
            }
            String overwriteString = "";
            if (HivePlugInConfiguration.getOutputOverwrite(configuration)) {
                overwriteString = "overwrite ";
            }
            String cmd;
            if (!databaseName.isEmpty() && !databaseName.equalsIgnoreCase("default")) {
                cmd = "Load data inpath '" + outputPath + "' " + overwriteString + "into table " + databaseName + "." + tableName;
            } else {
                cmd = "Load data inpath '" + outputPath + "' " + overwriteString + "into table " + tableName;
            }
            HiveUtils.logger.debug((Object) ("the hql statement is:" + cmd));
            if (fs.isDirectory(output) && fs.listStatus(output).length != 0) {
                final CommandProcessorResponse code = driver.run(cmd);
                fs.delete(output, true);
                if (code.getResponseCode() != 0) {
                    throw new ConnectorException(code.getErrorMessage());
                }
            }
        } catch (ConnectorException e2) {
            throw e2;
        } catch (IOException e3) {
            throw new ConnectorException(e3.getMessage(), e3);
        } catch (CommandNeedRetryException e4) {
            throw new ConnectorException(32006);
        } finally {
            if (ss != null) {
                ss.close();
                ss = null;
            }
            if (driver != null) {
                driver.close();
                driver = null;
            }
        }
    }

    public static void addPartitionsToHiveTable(final Configuration configuration, final String databaseName, final String tableName, final HiveConf hiveConf, final HiveMetaStoreClient client) throws ConnectorException {
        CliSessionState ss = new CliSessionState(hiveConf);
        HiveUtils.logger.info((Object) "add partition to hive table");
        ss.in = System.in;
        try {
            ss.out = new PrintStream(System.out, true, "UTF-8");
            ss.err = new PrintStream(System.err, true, "UTF-8");
        } catch (UnsupportedEncodingException e) {
            throw new ConnectorException(e.getMessage(), e);
        }
        SessionState.start((SessionState) ss);
        Driver driver = new Driver(hiveConf);
        String outputPath = HivePlugInConfiguration.getOutputPaths(configuration);
        try {
            if (outputPath.contains(",")) {
                final String tempOutputPath = outputPath.split(",")[0];
                final int firstEquals = tempOutputPath.indexOf("=");
                final int firstValidateSlash = tempOutputPath.substring(0, firstEquals).lastIndexOf("/");
                outputPath = outputPath.substring(0, firstValidateSlash);
            }
            final FileSystem fs = FileSystem.get(new Path(outputPath).toUri(), configuration);
            final Path tempPath = new Path(outputPath);
            outputPath = tempPath.makeQualified(fs).toString();
            final List<Path> sourcePaths = HadoopConfigurationUtils.getAllFilePaths(configuration, outputPath);
            final Set<String> partitionValues = new HashSet<String>();
            for (int pathCount = sourcePaths.size(), i = 0; i < pathCount; ++i) {
                final Path path = sourcePaths.get(i);
                final String parentString = path.toString().substring(0, path.toString().lastIndexOf(47));
                partitionValues.add(parentString.substring(outputPath.length() + 1));
            }
            final String tableLocation = client.getTable(databaseName, tableName).getSd().getLocation();
            for (final String partitionValue : partitionValues) {
                final String sourcePath = outputPath + "/" + partitionValue;
                final String targetPath = tableLocation + "/" + partitionValue;
                final String targetFolder = targetPath.substring(0, targetPath.lastIndexOf("/"));
                if (!targetFolder.equals(tableLocation)) {
                    fs.mkdirs(new Path(targetFolder));
                }
                Partition oldPartition = null;
                try {
                    oldPartition = client.getPartition(databaseName, tableName, partitionValue);
                } catch (NoSuchObjectException ex) {
                }
                if (HivePlugInConfiguration.getOutputOverwrite(configuration) && oldPartition != null) {
                    client.dropPartition(databaseName, tableName, partitionValue, true);
                    oldPartition = null;
                }
                if (oldPartition != null) {
                    final Path sourcePartitionPath = new Path(sourcePath);
                    final FileStatus[] listStatus;
                    final FileStatus[] fileStatus = listStatus = fs.listStatus(sourcePartitionPath);
                    for (final FileStatus file : listStatus) {
                        final String newPathString = file.getPath().getName();
                        if (fs.exists(new Path(targetPath, newPathString))) {
                            final String newPartitionFileNameString = getFileNameUnderHiveNamingConvention(targetPath, fs);
                            final Boolean result = fs.rename(new Path(sourcePath, newPathString), new Path(targetPath, newPartitionFileNameString));
                            if (!result) {
                                HiveUtils.logger.warn((Object) ("Rename " + sourcePath + "/" + newPathString + " to " + targetPath + "/" + newPartitionFileNameString + "failed."));
                            }
                        } else {
                            final Boolean result2 = fs.rename(new Path(sourcePath, newPathString), new Path(targetPath, newPathString));
                            if (!result2) {
                                HiveUtils.logger.warn((Object) ("Rename " + sourcePath + "/" + newPathString + " to " + targetPath + "/" + newPathString + "failed."));
                            }
                        }
                    }
                } else {
                    final boolean result3 = fs.rename(new Path(sourcePath), new Path(targetPath));
                    if (!result3) {
                        HiveUtils.logger.warn((Object) ("Rename " + sourcePath + " to " + targetPath + "failed."));
                    }
                    String newPartitionValue = partitionValue.replaceAll("=", "='");
                    newPartitionValue = newPartitionValue.replaceAll("/", "',");
                    newPartitionValue += "'";
                    newPartitionValue = FileUtils.unescapePathName(newPartitionValue);
                    String cmd;
                    if (!databaseName.isEmpty() && !databaseName.equalsIgnoreCase("default")) {
                        driver.run("USE " + databaseName + "").getResponseCode();
                        cmd = "ALTER TABLE " + tableName + " ADD PARTITION ( " + newPartitionValue + ") location '" + tableLocation + "/" + partitionValue + "'";
                    } else {
                        cmd = "ALTER TABLE " + tableName + " ADD PARTITION ( " + newPartitionValue + ") location '" + tableLocation + "/" + partitionValue + "'";
                    }
                    HiveUtils.logger.debug((Object) ("the hql statement is:" + cmd));
                    driver.run(cmd).getResponseCode();
                }
            }
            fs.delete(new Path(outputPath), true);
        } catch (IOException e2) {
            throw new ConnectorException(e2.getMessage(), e2);
        } catch (CommandNeedRetryException e5) {
            throw new ConnectorException(32006);
        } catch (MetaException e3) {
            throw new ConnectorException(e3.getMessage(), (Throwable) e3);
        } catch (NoSuchObjectException e6) {
            throw new ConnectorException(32001);
        } catch (UnknownTableException e4) {
            throw new ConnectorException(e4.getMessage(), (Throwable) e4);
        } catch (TException e7) {
            throw new ConnectorException(32007);
        } finally {
            if (ss != null) {
                ss.close();
                ss = null;
            }
            if (driver != null) {
                driver.close();
                driver = null;
            }
        }
    }

    public static void checkHiveTableProperties(final Table table) throws ConnectorException {
        final StorageDescriptor sd = table.getSd();
        final String inputFormat = sd.getInputFormat();
        final String outputFormat = sd.getOutputFormat();
        final String serdeLib = sd.getSerdeInfo().getSerializationLib();
        if (!"org.apache.hadoop.mapred.TextInputFormat".equalsIgnoreCase(inputFormat) && !"org.apache.hadoop.mapred.SequenceFileInputFormat".equalsIgnoreCase(inputFormat) && !"org.apache.hadoop.hive.ql.io.orc.OrcInputFormat".equalsIgnoreCase(inputFormat) && !"org.apache.hadoop.hive.ql.io.RCFileInputFormat".equalsIgnoreCase(inputFormat) && !"org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat".equalsIgnoreCase(inputFormat) && !"org.apache.hadoop.hive.ql.io.avro.AvroContainerInputFormat".equalsIgnoreCase(inputFormat)) {
            throw new ConnectorException(32003);
        }
        if (!"org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat".equalsIgnoreCase(outputFormat) && !"org.apache.hadoop.hive.ql.io.HiveSequenceFileOutputFormat".equalsIgnoreCase(outputFormat) && !"org.apache.hadoop.hive.ql.io.orc.OrcOutputFormat".equalsIgnoreCase(outputFormat) && !"org.apache.hadoop.hive.ql.io.RCFileOutputFormat".equalsIgnoreCase(outputFormat) && !"org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat".equalsIgnoreCase(outputFormat) && !"org.apache.hadoop.hive.ql.io.avro.AvroContainerOutputFormat".equalsIgnoreCase(outputFormat)) {
            throw new ConnectorException(32004);
        }
        if (!"org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe".equalsIgnoreCase(serdeLib) && !"org.apache.hadoop.hive.ql.io.orc.OrcSerde".equalsIgnoreCase(serdeLib) && !"org.apache.hadoop.hive.serde2.columnar.ColumnarSerDe".equalsIgnoreCase(serdeLib) && !"org.apache.hadoop.hive.serde2.columnar.LazyBinaryColumnarSerDe".equalsIgnoreCase(serdeLib) && !"org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe".equalsIgnoreCase(serdeLib) && !"org.apache.hadoop.hive.serde2.avro.AvroSerDe".equalsIgnoreCase(serdeLib)) {
            throw new ConnectorException(32005);
        }
    }

    public static String setHiveSchemaToOutputHiveTable(final Configuration configuration, final String databaseName, final String tableName, final HiveMetaStoreClient client) throws ConnectorException {
        String path = null;
        try {
            final Table hiveTable = client.getTable(databaseName, tableName);
            final StorageDescriptor storageDescriptor = hiveTable.getSd();
            checkHiveTableProperties(hiveTable);
            final List<FieldSchema> fieldSchemas = (List<FieldSchema>) storageDescriptor.getCols();
            String hiveTableSchema = "";
            int colNum = fieldSchemas.size() - 1;
            int i = 0;
            for (final FieldSchema field : fieldSchemas) {
                hiveTableSchema = hiveTableSchema + field.getName() + " " + field.getType();
                if (i < colNum) {
                    hiveTableSchema += ",";
                }
                ++i;
            }
            final List<FieldSchema> partitioFields = (List<FieldSchema>) hiveTable.getPartitionKeys();
            String partitionSchema = "";
            i = 0;
            colNum = partitioFields.size() - 1;
            for (final FieldSchema field2 : partitioFields) {
                partitionSchema = partitionSchema + field2.getName() + " " + field2.getType();
                if (i < colNum) {
                    partitionSchema += ",";
                }
                ++i;
            }
            if (hiveTable.getParameters().containsKey("avro.schema.url")) {
                final String avroURL = hiveTable.getParameters().get("avro.schema.url");
                HivePlugInConfiguration.setOutputTableAvroURL(configuration, avroURL);
            }
            if (hiveTable.getParameters().containsKey("avro.schema.literal")) {
                final String avroLiteral = hiveTable.getParameters().get("avro.schema.literal");
                HivePlugInConfiguration.setOutputTableAvroLiteral(configuration, avroLiteral);
            }
            HivePlugInConfiguration.setOutputTableSchema(configuration, hiveTableSchema);
            HivePlugInConfiguration.setOutputPartitionSchema(configuration, partitionSchema);
            final String outputFormat = hiveTable.getSd().getOutputFormat();
            if ("org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat".equalsIgnoreCase(outputFormat) || "org.apache.hadoop.hive.ql.io.HiveSequenceFileOutputFormat".equalsIgnoreCase(outputFormat)) {
                final String separator = hiveTable.getSd().getSerdeInfo().getParameters().get("field.delim");
                HivePlugInConfiguration.setOutputSeparator(configuration, ConnectorUnicodeCharacterConverter.toEncodedUnicode(separator));
            }
            String value = hiveTable.getSd().getSerdeInfo().getParameters().get("colelction.delim");
            if (value != null && !value.isEmpty()) {
                configuration.set("colelction.delim", ConnectorUnicodeCharacterConverter.toEncodedUnicode(value));
            }
            value = hiveTable.getSd().getSerdeInfo().getParameters().get("mapkey.delim");
            if (value != null && !value.isEmpty()) {
                configuration.set("mapkey.delim", ConnectorUnicodeCharacterConverter.toEncodedUnicode(value));
            }
            value = hiveTable.getSd().getSerdeInfo().getParameters().get("serialization.null.format");
            if (value != null && !value.isEmpty()) {
                configuration.set("serialization.null.format", ConnectorUnicodeCharacterConverter.toEncodedUnicode(value));
            }
            value = hiveTable.getSd().getSerdeInfo().getParameters().get("serialization.last.column.takes.rest");
            if (value != null && !value.isEmpty()) {
                configuration.set("serialization.last.column.takes.rest", value);
            }
            value = hiveTable.getSd().getSerdeInfo().getParameters().get("escape.delim");
            if (value != null && !value.isEmpty()) {
                configuration.set("escape.delim", ConnectorUnicodeCharacterConverter.toEncodedUnicode(value));
            }
            if (hiveTable.getSd().getOutputFormat().contains("RCFile")) {
                value = hiveTable.getSd().getSerdeInfo().getSerializationLib();
                HivePlugInConfiguration.setOutputRCFileSerde(configuration, value);
            }
            path = storageDescriptor.getLocation();
        } catch (MetaException e) {
            throw new ConnectorException(e.getMessage(), (Throwable) e);
        } catch (NoSuchObjectException e2) {
            throw new ConnectorException(32001);
        } catch (TException e3) {
            throw new ConnectorException(32007);
        }
        return path;
    }

    public static String setHiveSchemaToInputHiveTable(final Configuration configuration, final String databaseName, final String tableName, final HiveMetaStoreClient client) throws ConnectorException {
        String path = null;
        final ConnectorSchemaParser parser = new ConnectorSchemaParser();
        parser.setDelimChar(' ');
        parser.setEscapeChar('\\');
        parser.setIgnoreContinousDelim(true);
        try {
            final Table hiveTable = client.getTable(databaseName, tableName);
            final StorageDescriptor storageDescriptor = hiveTable.getSd();
            checkHiveTableProperties(hiveTable);
            final List<FieldSchema> fieldSchemas = (List<FieldSchema>) storageDescriptor.getCols();
            String hiveTableSchema = "";
            int colNum = fieldSchemas.size() - 1;
            int i = 0;
            for (final FieldSchema field : fieldSchemas) {
                final List<String> tokens = parser.tokenize(field.getName().trim(), 2, false);
                hiveTableSchema = hiveTableSchema + ((tokens.size() > 1) ? "\"" : "") + field.getName() + ((tokens.size() > 1) ? "\"" : "") + " " + field.getType();
                if (i < colNum) {
                    hiveTableSchema += ",";
                }
                ++i;
            }
            final List<FieldSchema> partitioFields = (List<FieldSchema>) hiveTable.getPartitionKeys();
            String partitionSchema = "";
            i = 0;
            colNum = partitioFields.size() - 1;
            for (final FieldSchema field2 : partitioFields) {
                partitionSchema = partitionSchema + field2.getName() + " " + field2.getType();
                if (i < colNum) {
                    partitionSchema += ",";
                }
                ++i;
            }
            if (hiveTable.getParameters().containsKey("avro.schema.url")) {
                final String avroURL = hiveTable.getParameters().get("avro.schema.url");
                HivePlugInConfiguration.setInputTableAvroURL(configuration, avroURL);
            }
            if (hiveTable.getParameters().containsKey("avro.schema.literal")) {
                final String avroLiteral = hiveTable.getParameters().get("avro.schema.literal");
                HivePlugInConfiguration.setInputTableAvroLiteral(configuration, avroLiteral);
            }
            HivePlugInConfiguration.setInputTableSchema(configuration, hiveTableSchema);
            HivePlugInConfiguration.setInputPartitionSchema(configuration, partitionSchema);
            final String inputFormat = hiveTable.getSd().getInputFormat();
            if ("org.apache.hadoop.mapred.TextInputFormat".equalsIgnoreCase(inputFormat) || "org.apache.hadoop.mapred.SequenceFileInputFormat".equalsIgnoreCase(inputFormat)) {
                final String separator = hiveTable.getSd().getSerdeInfo().getParameters().get("field.delim");
                HivePlugInConfiguration.setInputSeparator(configuration, ConnectorUnicodeCharacterConverter.toEncodedUnicode(separator));
            }
            String value = hiveTable.getSd().getSerdeInfo().getParameters().get("colelction.delim");
            if (value != null && !value.isEmpty()) {
                configuration.set("colelction.delim", ConnectorUnicodeCharacterConverter.toEncodedUnicode(value));
            }
            value = hiveTable.getSd().getSerdeInfo().getParameters().get("mapkey.delim");
            if (value != null && !value.isEmpty()) {
                configuration.set("mapkey.delim", ConnectorUnicodeCharacterConverter.toEncodedUnicode(value));
            }
            value = hiveTable.getSd().getSerdeInfo().getParameters().get("serialization.null.format");
            if (value != null && !value.isEmpty()) {
                configuration.set("serialization.null.format", ConnectorUnicodeCharacterConverter.toEncodedUnicode(value));
            }
            value = hiveTable.getSd().getSerdeInfo().getParameters().get("serialization.last.column.takes.rest");
            if (value != null && !value.isEmpty()) {
                configuration.set("serialization.last.column.takes.rest", value);
            }
            value = hiveTable.getSd().getSerdeInfo().getParameters().get("escape.delim");
            if (value != null && !value.isEmpty()) {
                configuration.set("escape.delim", ConnectorUnicodeCharacterConverter.toEncodedUnicode(value));
            }
            if (hiveTable.getSd().getInputFormat().contains("RCFile")) {
                value = hiveTable.getSd().getSerdeInfo().getSerializationLib();
                HivePlugInConfiguration.setInputRCFileSerde(configuration, value);
            }
            path = storageDescriptor.getLocation();
        } catch (MetaException e) {
            throw new ConnectorException(e.getMessage(), (Throwable) e);
        } catch (NoSuchObjectException e2) {
            throw new ConnectorException(32001);
        } catch (TException e3) {
            throw new ConnectorException(32007);
        }
        return path;
    }

    public static void createHiveTable(final Configuration configuration, final String databaseName, final String tableName, final HiveConf hiveConf, final HiveMetaStoreClient client) throws ConnectorException {
        try {
            final Table table = new Table();
            table.setDbName(databaseName);
            table.setTableName(tableName);
            UserGroupInformation ugi = null;
            try {
                ugi = UserGroupInformation.getCurrentUser();
                table.setOwner(ugi.getUserName());
            } catch (Exception e6) {
                try {
                    final HadoopShims shims = ShimLoader.getHadoopShims();
                    Method mthd = null;
                    mthd = shims.getClass().getDeclaredMethod("getUGIForConf", Configuration.class);
                    ugi = (UserGroupInformation) mthd.invoke(shims, configuration);
                    table.setOwner(ugi.getUserName());
                } catch (Exception e7) {
                    HiveUtils.logger.warn((Object) "LoginException: Cannot get current user to set Hive table owner");
                }
            }
            final String targetPartitionSchema = HivePlugInConfiguration.getOutputPartitionSchema(configuration);
            table.setTableType(TableType.MANAGED_TABLE.toString());
            table.setTableTypeIsSet(true);
            final StorageDescriptor sd = new StorageDescriptor();
            final String targetTableSchema = HivePlugInConfiguration.getOutputTableSchema(configuration);
            final List<String> schemafields = ConnectorSchemaUtils.parseColumns(targetTableSchema);
            final List<String> colNames = ConnectorSchemaUtils.parseColumnNames(schemafields);
            final List<String> colTypes = ConnectorSchemaUtils.parseColumnTypes(schemafields);
            HiveSchemaUtils.updateNonScaleDecimalColumns(colTypes);
            final int colCount = colNames.size();
            final List<FieldSchema> fieldSchemas = new ArrayList<FieldSchema>();
            for (int i = 0; i < colCount; ++i) {
                fieldSchemas.add(new FieldSchema((String) colNames.get(i), colTypes.get(i).toLowerCase(), ""));
            }
            sd.setCols((List) fieldSchemas);
            final String outputFormat = ConnectorConfiguration.getPlugInOutputFormat(configuration);
            try {
                final Class<?> clz = Class.forName(outputFormat);
                if (clz.isAssignableFrom(HiveTextFileOutputFormat.class)) {
                    sd.setInputFormat("org.apache.hadoop.mapred.TextInputFormat");
                    sd.setOutputFormat("org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat");
                } else if (clz.isAssignableFrom(HiveSequenceFileOutputFormat.class)) {
                    sd.setInputFormat("org.apache.hadoop.mapred.SequenceFileInputFormat");
                    sd.setOutputFormat("org.apache.hadoop.hive.ql.io.HiveSequenceFileOutputFormat");
                } else if (clz.isAssignableFrom(HiveRCFileOutputFormat.class)) {
                    sd.setInputFormat("org.apache.hadoop.hive.ql.io.RCFileInputFormat");
                    sd.setOutputFormat("org.apache.hadoop.hive.ql.io.RCFileOutputFormat");
                } else if (clz.isAssignableFrom(HiveORCFileOutputFormat.class)) {
                    sd.setInputFormat("org.apache.hadoop.hive.ql.io.orc.OrcInputFormat");
                    sd.setOutputFormat("org.apache.hadoop.hive.ql.io.orc.OrcOutputFormat");
                } else if (clz.isAssignableFrom(HiveParquetFileOutputFormat.class)) {
                    sd.setInputFormat("org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat");
                    sd.setOutputFormat("org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat");
                } else {
                    if (!clz.isAssignableFrom(HiveAvroFileOutputFormat.class)) {
                        throw new ConnectorException(32014);
                    }
                    sd.setInputFormat("org.apache.hadoop.hive.ql.io.avro.AvroContainerInputFormat");
                    sd.setOutputFormat("org.apache.hadoop.hive.ql.io.avro.AvroContainerOutputFormat");
                }
            } catch (ClassNotFoundException e) {
                throw new ConnectorException(e.getMessage(), e);
            }
            sd.setInputFormatIsSet(true);
            sd.setOutputFormatIsSet(true);
            sd.setParameters((Map) new HashMap());
            sd.setParametersIsSet(true);
            SerDeInfo serDeInfo = null;
            final Map<String, String> sdParameters = new HashMap<String, String>();
            sdParameters.put("field.delim", ConnectorUnicodeCharacterConverter.fromEncodedUnicode(HivePlugInConfiguration.getOutputSeparator(configuration)));
            sdParameters.put("line.delim", ConnectorUnicodeCharacterConverter.fromEncodedUnicode(HivePlugInConfiguration.getOutputLineSeparator(configuration)));
            final String nullString = HivePlugInConfiguration.getOutputNullString(configuration);
            if (nullString != null) {
                sdParameters.put("serialization.null.format", nullString);
            }
            try {
                final Class<?> clz2 = Class.forName(outputFormat);
                if (clz2.isAssignableFrom(HiveRCFileOutputFormat.class)) {
                    final String defSerde = HivePlugInConfiguration.getOutputRCFileSerde(configuration);
                    if (defSerde.isEmpty()) {
                        serDeInfo = new SerDeInfo("LazyBinaryColumnarSerDe", "org.apache.hadoop.hive.serde2.columnar.LazyBinaryColumnarSerDe", (Map) sdParameters);
                    } else if (defSerde.equals("org.apache.hadoop.hive.serde2.columnar.LazyBinaryColumnarSerDe")) {
                        serDeInfo = new SerDeInfo("LazyBinaryColumnarSerDe", "org.apache.hadoop.hive.serde2.columnar.LazyBinaryColumnarSerDe", (Map) sdParameters);
                    } else {
                        if (!defSerde.equals("org.apache.hadoop.hive.serde2.columnar.ColumnarSerDe")) {
                            throw new ConnectorException(32005);
                        }
                        serDeInfo = new SerDeInfo("ColumnarSerDe", "org.apache.hadoop.hive.serde2.columnar.ColumnarSerDe", (Map) sdParameters);
                    }
                } else if (clz2.isAssignableFrom(HiveORCFileOutputFormat.class)) {
                    serDeInfo = new SerDeInfo("OrcSerDe", "org.apache.hadoop.hive.ql.io.orc.OrcSerde", (Map) sdParameters);
                } else if (clz2.isAssignableFrom(HiveTextFileOutputFormat.class) || clz2.isAssignableFrom(HiveSequenceFileOutputFormat.class)) {
                    serDeInfo = new SerDeInfo("LazySimpleSerDe", "org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe", (Map) sdParameters);
                } else if (clz2.isAssignableFrom(HiveParquetFileOutputFormat.class)) {
                    sdParameters.clear();
                    serDeInfo = new SerDeInfo("ParquetHiveSerDe", "org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe", (Map) sdParameters);
                } else {
                    if (!clz2.isAssignableFrom(HiveAvroFileOutputFormat.class)) {
                        throw new ConnectorException(32014);
                    }
                    sdParameters.clear();
                    serDeInfo = new SerDeInfo("AvroSerDe", "org.apache.hadoop.hive.serde2.avro.AvroSerDe", (Map) sdParameters);
                }
            } catch (ClassNotFoundException e2) {
                throw new ConnectorException(e2.getMessage(), e2);
            }
            serDeInfo.setParametersIsSet(true);
            sd.setSerdeInfo(serDeInfo);
            sd.setSerdeInfoIsSet(true);
            table.setSd(sd);
            table.setSdIsSet(true);
            table.unsetParameters();
            if (targetPartitionSchema != null && targetPartitionSchema.length() != 0) {
                final List<String> partitionfields = ConnectorSchemaUtils.parseColumns(targetPartitionSchema);
                final List<String> partitionColNames = ConnectorSchemaUtils.parseColumnNames(partitionfields);
                final List<String> partitionColTypes = ConnectorSchemaUtils.parseColumnTypes(partitionfields);
                HiveSchemaUtils.updateNonScaleDecimalColumns(partitionColTypes);
                final List<FieldSchema> partitionKeys = new ArrayList<FieldSchema>();
                for (int colCnt = partitionColNames.size(), j = 0; j < colCnt; ++j) {
                    partitionKeys.add(new FieldSchema((String) partitionColNames.get(j), partitionColTypes.get(j).toLowerCase(), ""));
                }
                table.setPartitionKeys((List) partitionKeys);
                table.setPartitionKeysIsSet(true);
            }
            client.createTable(table);
        } catch (MetaException e3) {
            throw new ConnectorException(e3.getMessage(), (Throwable) e3);
        } catch (AlreadyExistsException e4) {
            throw new ConnectorException(e4.getMessage(), (Throwable) e4);
        } catch (InvalidObjectException e5) {
            throw new ConnectorException(e5.getMessage(), (Throwable) e5);
        } catch (NoSuchObjectException e8) {
            throw new ConnectorException(32001);
        } catch (TException e9) {
            throw new ConnectorException(32007);
        }
    }

    public static void checkFieldNamesContainPartitions(final String[] fieldNames, final String partitionSchema) throws ConnectorException {
        for (int i = 0; i < fieldNames.length; ++i) {
            fieldNames[i] = fieldNames[i].toLowerCase();
        }
        if (!ConnectorStringUtils.isEmpty(partitionSchema)) {
            final List<String> fields = Arrays.asList(fieldNames);
            final List<String> partitionFields = ConnectorSchemaUtils.parseColumns(partitionSchema.toLowerCase());
            final List<String> partitionFieldNames = ConnectorSchemaUtils.parseColumnNames(partitionFields);
            if (!fields.containsAll(partitionFieldNames)) {
                throw new ConnectorException(30004);
            }
        }
    }

    public static void checkFieldNamesInSchema(final String[] fieldNames, final String tableSchema, final String partitionSchema) throws ConnectorException {
        for (int i = 0; i < fieldNames.length; ++i) {
            fieldNames[i] = fieldNames[i].toLowerCase();
        }
        final List<String> fields = Arrays.asList(fieldNames);
        final List<String> tableFields = ConnectorSchemaUtils.parseColumns(tableSchema.toLowerCase());
        final List<String> tableFieldNames = ConnectorSchemaUtils.parseColumnNames(tableFields);
        final List<String> partitionFields = ConnectorSchemaUtils.parseColumns(partitionSchema.toLowerCase());
        final List<String> partitionFieldNames = ConnectorSchemaUtils.parseColumnNames(partitionFields);
        tableFieldNames.addAll(partitionFieldNames);
        if (!tableFieldNames.containsAll(fields)) {
            throw new ConnectorException(33001);
        }
    }

    public static void checkPartitionNamesInSchema(final String tableSchema, final String partitionSchema) throws ConnectorException {
        final List<String> tableFields = ConnectorSchemaUtils.parseColumns(tableSchema);
        final List<String> tableFieldNames = ConnectorSchemaUtils.parseColumnNames(tableFields);
        final List<String> partitionFields = ConnectorSchemaUtils.parseColumns(partitionSchema);
        final List<String> partitionFieldNames = ConnectorSchemaUtils.parseColumnNames(partitionFields);
        for (final String partitionFildName : partitionFieldNames) {
            for (final String tableFildName : tableFieldNames) {
                if (partitionFildName.equalsIgnoreCase(tableFildName)) {
                    throw new ConnectorException(30006);
                }
            }
        }
    }

    public static long getRandomNumber() {
        final Random generator = new Random(HiveUtils.RANDOMSEED);
        return generator.nextLong();
    }

    public static String getFileNameUnderHiveNamingConvention(final String directoryName, final FileSystem fs) throws ConnectorException, IOException {
        final Path directory = new Path(directoryName);
        String fileName = null;
        final String firstFileName = "00000";
        if (!fs.exists(directory)) {
            throw new ConnectorException(32012);
        }
        if (!fs.getFileStatus(directory).isDir()) {
            throw new ConnectorException(32011);
        }
        int maxValue = 0;
        final FileStatus[] listStatus;
        final FileStatus[] fileStatus = listStatus = fs.listStatus(directory);
        for (final FileStatus file : listStatus) {
            final String name = file.getPath().getName();
            int value = 0;
            try {
                value = Integer.parseInt(name);
            } catch (NumberFormatException ex) {
            }
            if (value > maxValue) {
                maxValue = value;
            }
        }
        fileName = String.valueOf(maxValue + 1);
        final int length = fileName.length();
        final int fixedLength = firstFileName.length();
        if (length < fixedLength) {
            return firstFileName.substring(0, fixedLength - length) + fileName;
        }
        return fileName;
    }

    public static List<String> filterByValues(final String fieldname, final List<String> paths, final String values) {
        final List<String> result = new ArrayList<String>();
        final String[] partitionvalues = values.split(",");
        if (partitionvalues == null || partitionvalues.length == 0) {
            return result;
        }
        for (final String val : partitionvalues) {
            for (final String path : paths) {
                if (path.contains(fieldname + "=" + val)) {
                    result.add(path);
                }
            }
        }
        return result;
    }

    public static List<String> filterByRegex(final String fieldname, final List<String> paths, final String regex) {
        final List<String> result = new ArrayList<String>();
        for (final String path : paths) {
            if (path.contains(fieldname + "=")) {
                String val = "";
                val = path.substring(path.indexOf(fieldname + "=") + (fieldname.length() + 1));
                final int end = val.indexOf("/");
                if (end != -1) {
                    val = val.substring(0, end);
                }
                if (!val.matches(regex)) {
                    continue;
                }
                result.add(path);
            }
        }
        return result;
    }

    public static boolean isHiveOutputTablePartitioned(final Configuration configuration) throws ConnectorException {
        boolean flag = false;
        String database = HivePlugInConfiguration.getOutputDatabase(configuration);
        final String tablename = HivePlugInConfiguration.getOutputTable(configuration);
        try {
            if (database.isEmpty()) {
                database = "default";
            }
            if (!tablename.isEmpty()) {
                final HiveConf hiveConf = new HiveConf(configuration, (Class) HiveUtils.class);
                loadHiveConf((Configuration) hiveConf, ConnectorConfiguration.direction.output);
                final HiveMetaStoreClient client = new HiveMetaStoreClient(hiveConf);
                Table table;
                try {
                    table = client.getTable(database, tablename);
                } catch (NoSuchObjectException e3) {
                    return false;
                }
                final List<FieldSchema> partition = (List<FieldSchema>) table.getPartitionKeys();
                if (partition.size() > 0) {
                    flag = true;
                }
            }
        } catch (MetaException e) {
            throw new ConnectorException(e.getMessage(), (Throwable) e);
        } catch (TException e2) {
            e2.printStackTrace();
        }
        return flag;
    }

    @Deprecated
    public static boolean isPartition(final Configuration configuration) throws ConnectorException {
        boolean flag = false;
        String database = HivePlugInConfiguration.getOutputDatabase(configuration);
        final String tablename = HivePlugInConfiguration.getOutputTable(configuration);
        try {
            if (database.isEmpty()) {
                database = "default";
            }
            if (!tablename.isEmpty()) {
                final HiveConf hiveConf = new HiveConf(configuration, (Class) HiveUtils.class);
                try {
                    loadHiveConf((Configuration) hiveConf, ConnectorConfiguration.direction.output);
                } catch (ConnectorException e) {
                    e.printStackTrace();
                }
                final HiveMetaStoreClient client = new HiveMetaStoreClient(hiveConf);
                Table table;
                try {
                    table = client.getTable(database, tablename);
                } catch (NoSuchObjectException e4) {
                    return false;
                }
                final List<FieldSchema> partition = (List<FieldSchema>) table.getPartitionKeys();
                if (partition.size() > 0) {
                    flag = true;
                }
            }
        } catch (MetaException e2) {
            throw new ConnectorException(e2.getMessage(), (Throwable) e2);
        } catch (TException e3) {
            e3.printStackTrace();
        }
        return flag;
    }

    public static List<String> getPathsByPartitionFilter(final Configuration configuration, final String databaseName, final String tableName, final HiveMetaStoreClient client, final String[] filters) {
        final List<String> paths = new ArrayList<String>();
        try {
            final List<String> partitions = (List<String>) client.listPartitionNames(databaseName, tableName, (short) 0);
            if (partitions == null || partitions.size() == 0) {
                return paths;
            }
            for (final String filter : filters) {
                final String[] items = filter.split("=>");
                if (items.length != 3) {
                    throw new ConnectorException(32013);
                }
                final String fieldname = items[0];
                final String usage = items[1];
                final String values = items[2];
                if (usage.equalsIgnoreCase("values")) {
                    paths.addAll(filterByValues(fieldname, partitions, values));
                } else if (usage.equalsIgnoreCase("regex")) {
                    paths.addAll(filterByRegex(fieldname, partitions, values));
                }
            }
        } catch (MetaException e) {
            e.printStackTrace();
        } catch (NoSuchObjectException e2) {
            e2.printStackTrace();
        } catch (TException e3) {
            e3.printStackTrace();
        } catch (ConnectorException e4) {
            e4.printStackTrace();
        }
        return paths;
    }
}
