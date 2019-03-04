package com.teradata.connector.hive.processor;

import com.teradata.connector.common.ConnectorInputProcessor;
import com.teradata.connector.common.ConnectorRecordSchema;
import com.teradata.connector.common.exception.ConnectorException;
import com.teradata.connector.common.utils.ConnectorConfiguration;
import com.teradata.connector.common.utils.ConnectorSchemaUtils;
import com.teradata.connector.common.utils.ConnectorStringUtils;
import com.teradata.connector.common.utils.HadoopConfigurationUtils;
import com.teradata.connector.hive.utils.HivePlugInConfiguration;
import com.teradata.connector.hive.utils.HiveSchemaUtils;
import com.teradata.connector.hive.utils.HiveUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.metastore.HiveMetaHookLoader;
import org.apache.hadoop.hive.metastore.HiveMetaStoreClient;
import org.apache.hadoop.hive.metastore.api.MetaException;
import org.apache.hadoop.hive.metastore.api.NoSuchObjectException;
import org.apache.hadoop.hive.metastore.api.Partition;
import org.apache.hadoop.hive.metastore.api.UnknownDBException;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.thrift.TException;

import java.io.IOException;
import java.nio.file.FileSystems;
import java.nio.file.PathMatcher;
import java.nio.file.Paths;
import java.util.Arrays;
import java.util.List;

/**
 * @author Administrator
 */
public class HiveInputProcessor implements ConnectorInputProcessor {
    private Log logger = LogFactory.getLog ((Class) HiveInputProcessor.class);

    @Override
    public int inputPreProcessor(final JobContext context) throws ConnectorException {
        try {
            final Configuration configuration = context.getConfiguration ();
            String databaseName = HivePlugInConfiguration.getInputDatabase (configuration);
            final String tableName = HivePlugInConfiguration.getInputTable (configuration);
            String sourcePaths = HivePlugInConfiguration.getInputPaths (configuration);
            String sourceTableSchema = HivePlugInConfiguration.getInputTableSchema (configuration);
            HiveMetaStoreClient client = null;
            if (databaseName == null || databaseName.isEmpty ()) {
                databaseName = "default";
                HivePlugInConfiguration.setInputDatabase (configuration, databaseName);
            }
            if (tableName.isEmpty ()) {
                if (sourcePaths.isEmpty ()) {
                    sourcePaths = configuration.get ("mapred.input.dir", "");
                    if (sourcePaths.isEmpty ()) {
                        throw new ConnectorException (31002);
                    }
                    HivePlugInConfiguration.setInputPaths (configuration, sourcePaths);
                    if (sourceTableSchema.isEmpty ()) {
                        throw new ConnectorException (31003);
                    }
                } else if (sourceTableSchema.isEmpty ()) {
                    throw new ConnectorException (31003);
                }
            } else if (!sourcePaths.isEmpty () || !configuration.get ("mapred.input.dir", "").isEmpty ()) {
                throw new ConnectorException (31001);
            }
            if (!tableName.isEmpty ()) {
                final HiveConf hiveConf = new HiveConf (configuration, (Class) HiveInputProcessor.class);
                HiveUtils.loadHiveConf ((Configuration) hiveConf, ConnectorConfiguration.direction.input);
                client = new HiveMetaStoreClient (hiveConf, (HiveMetaHookLoader) null);
                final boolean tableExisted = client.tableExists (databaseName, tableName);
                if (!tableExisted) {
                    throw new ConnectorException (32001);
                }
                if (!ConnectorStringUtils.isEmpty (sourceTableSchema)) {
                    throw new ConnectorException (310005);
                }
                try {
                    HiveSchemaUtils.logHiveTableExtInfo (this.logger, databaseName, tableName, hiveConf);
                    HiveSchemaUtils.logHivePartitionValues (this.logger, databaseName, tableName, hiveConf);
                } catch (Exception e) {
                    e.printStackTrace ();
                }
                final String path = HiveUtils.setHiveSchemaToInputHiveTable (configuration, databaseName, tableName, client);
                final List<Partition> partitions = (List<Partition>) client.listPartitions (databaseName, tableName, (short) 2000);
                if (partitions.size () > 0) {
                    final StringBuilder allPaths = new StringBuilder ();

                    /**** anliang ***/
                    String sourcePartitionPath = configuration.get ("sourcepartitionpath", "");
                    PathMatcher matcher = FileSystems.getDefault ().getPathMatcher ("glob:" + sourcePartitionPath);
                    for (final Partition partition : partitions) {
                        String partitionPath = partition.getSd ().getLocation ();
                        if (!sourcePartitionPath.isEmpty ()) {
                            if (matcher.matches (Paths.get (partitionPath))) {
                                allPaths.append (partitionPath);
                                allPaths.append (",");
                            }
                        } else {
                            allPaths.append (partitionPath);
                            allPaths.append (",");
                        }
                    }
                    final int pos = allPaths.length () - 1;
                    if (pos < 0) {
                        throw new ConnectorException (33002);
                    }

                    allPaths.deleteCharAt (pos);
                    HivePlugInConfiguration.setInputPaths (configuration, allPaths.toString ());
                } else {
                    final Path tablePath = new Path (path);
                    final FileSystem fs = FileSystem.get (tablePath.toUri (), configuration);
                    if (!fs.exists (tablePath)) {
                        throw new ConnectorException (32008);
                    }
                    final String allPaths2 = HadoopConfigurationUtils.getAllFilePaths (configuration, new String[]{path});
                    if (allPaths2.isEmpty ()) {
                        throw new ConnectorException (32008);
                    }
                    HivePlugInConfiguration.setInputPaths (configuration, path);
                }
            } else {
                if (ConnectorStringUtils.isEmpty (sourceTableSchema)) {
                    throw new ConnectorException (31004);
                }
                final String[] paths = HivePlugInConfiguration.getInputPaths (configuration).split (",");
                final String allPaths3 = HadoopConfigurationUtils.getAllFilePaths (configuration, paths);
                HivePlugInConfiguration.setInputPaths (configuration, allPaths3);
                if (ConnectorConfiguration.getInputSerDe (configuration).contains ("RCFile")) {
                    final HiveConf hiveConf2 = new HiveConf (configuration, (Class) HiveInputProcessor.class);
                    HiveUtils.loadHiveConf ((Configuration) hiveConf2, ConnectorConfiguration.direction.input);
                    final HiveMetaStoreClient client2 = new HiveMetaStoreClient (hiveConf2, (HiveMetaHookLoader) null);
                    HivePlugInConfiguration.setInputRCFileSerde (configuration, client2.getConfigValue ("hive.default.rcfile.serde", ""));
                }
            }

            String[] sourceFieldNames = HivePlugInConfiguration.getInputFieldNamesArray (configuration);
            sourceTableSchema = HivePlugInConfiguration.getInputTableSchema (configuration);
            final String sourcePartitionSchema = HivePlugInConfiguration.getInputPartitionSchema (configuration);
            if (sourceFieldNames.length > 0) {
                /** add by zyx***/
                if (Arrays.asList (sourceFieldNames).contains ("*")) {
                    final List<String> columns = ConnectorSchemaUtils.parseColumns (sourceTableSchema.toLowerCase ());
                    final List<String> columnNames = ConnectorSchemaUtils.parseColumnNames (columns);
                    HivePlugInConfiguration.setInputFieldNamesArray (configuration, columnNames.toArray (new String[columnNames.size ()]));
                    sourceFieldNames = HivePlugInConfiguration.getInputFieldNamesArray (configuration);
                }
                HiveUtils.checkFieldNamesInSchema (sourceFieldNames, sourceTableSchema, sourcePartitionSchema);
            }
            this.setupSchema (configuration);
        } catch (IOException e2) {
            throw new ConnectorException (e2.getMessage (), e2);
        } catch (MetaException e3) {
            throw new ConnectorException (e3.getMessage (), (Throwable) e3);
        } catch (UnknownDBException e4) {
            throw new ConnectorException (e4.getMessage (), (Throwable) e4);
        } catch (NoSuchObjectException e5) {
            throw new ConnectorException (e5.getMessage (), (Throwable) e5);
        } catch (TException e6) {
            throw new ConnectorException (e6.getMessage (), (Throwable) e6);
        }
        return 0;
    }

    @Override
    public int inputPostProcessor(final JobContext context) throws ConnectorException {
        return 0;
    }

    private void setupSchema(final Configuration configuration) throws ConnectorException {
        boolean partitioned = false;
        List<String> pcolumnNames = null;
        String sourceTableSchema = HivePlugInConfiguration.getInputTableSchema (configuration).trim ();
        final String partitionSchema = HivePlugInConfiguration.getInputPartitionSchema (configuration).trim ();
        if (partitionSchema.length () > 0) {
            partitioned = true;
            final List<String> pcolumns = ConnectorSchemaUtils.parseColumns (partitionSchema.toLowerCase ());
            pcolumnNames = ConnectorSchemaUtils.parseColumnNames (pcolumns);
            sourceTableSchema = sourceTableSchema + "," + partitionSchema;
        }
        final List<String> columns = ConnectorSchemaUtils.parseColumns (sourceTableSchema.toLowerCase ());
        final List<String> columnNames = ConnectorSchemaUtils.parseColumnNames (columns);
        final List<String> columnTypes = ConnectorSchemaUtils.parseColumnTypes (columns);
        if (HivePlugInConfiguration.getInputFieldNamesArray (configuration).length == 0) {
            HivePlugInConfiguration.setInputFieldNamesArray (configuration, columnNames.toArray (new String[columnNames.size ()]));
        }
        final String[] fieldNames = HivePlugInConfiguration.getInputFieldNamesArray (configuration);
        HivePlugInConfiguration.setInputTableFieldTypes (configuration, columnTypes.toArray (new String[columnTypes.size ()]));
        HivePlugInConfiguration.setInputTableFieldNamesArray (configuration, columnNames.toArray (new String[columnNames.size ()]));
        final boolean isUDMapper = !ConnectorConfiguration.getJobMapper (configuration).isEmpty ();
        if (!isUDMapper) {
            final ConnectorRecordSchema r = new ConnectorRecordSchema (fieldNames.length);
            final int[] columnMapping = HiveSchemaUtils.getColumnMapping (columnNames.toArray (new String[columnNames.size ()]), fieldNames);
            for (int i = 0; i < columnMapping.length; ++i) {
                if (partitioned && pcolumnNames.contains (columnNames.get (columnMapping[i]))) {
                    r.setFieldType (i, 12);
                } else {
                    r.setFieldType (i, HiveSchemaUtils.lookupHiveDataTypeByName (columnTypes.get (columnMapping[i])));
                }
            }
            ConnectorSchemaUtils.formalizeConnectorRecordSchema (r);
            final ConnectorRecordSchema userSchema = ConnectorSchemaUtils.recordSchemaFromString (ConnectorConfiguration.getInputConverterRecordSchema (configuration));
            if (userSchema != null) {
                HiveSchemaUtils.checkSchemaMatch (userSchema, r);
            } else {
                ConnectorConfiguration.setInputConverterRecordSchema (configuration, ConnectorSchemaUtils.recordSchemaToString (r));
            }
        }
    }
}
