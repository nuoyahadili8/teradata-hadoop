package com.teradata.connector.hive.processor;

import com.teradata.connector.hive.utils.HiveUtils;
import org.apache.commons.logging.*;
import org.apache.hadoop.mapreduce.*;

import java.text.*;

import org.apache.hadoop.hive.conf.*;
import com.teradata.connector.common.exception.*;
import org.apache.hadoop.security.*;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.hive.metastore.*;
import com.teradata.connector.hive.utils.*;
import org.apache.hadoop.hive.ql.metadata.*;
import org.apache.hadoop.hive.metastore.api.*;
import org.apache.thrift.*;

import java.io.*;

import org.apache.hadoop.hive.shims.*;

import java.lang.reflect.*;

import com.teradata.connector.common.utils.*;
import com.teradata.connector.common.*;

import java.util.*;

public class HiveOutputProcessor implements ConnectorOutputProcessor {
    private Log logger = LogFactory.getLog((Class) HiveOutputProcessor.class);

    public HiveOutputProcessor() {
    }

    @Override
    public int outputPreProcessor(final JobContext context) throws ConnectorException {
        HiveMetaStoreClient client = null;
        try {
            final SimpleDateFormat sdf = new SimpleDateFormat("hhmmss");
            final Configuration configuration = context.getConfiguration();
            final HiveConf hiveConf = new HiveConf(configuration, (Class) HiveOutputProcessor.class);
            String databaseName = HivePlugInConfiguration.getOutputDatabase(configuration);
            final String tableName = HivePlugInConfiguration.getOutputTable(configuration);
            final String targetPath = HivePlugInConfiguration.getOutputPaths(configuration);
            if (databaseName.isEmpty()) {
                databaseName = "default";
                HivePlugInConfiguration.setOutputDatabase(configuration, databaseName);
            }
            if (tableName.isEmpty() && targetPath.isEmpty()) {
                throw new ConnectorException(30002);
            }
            if (!tableName.isEmpty() && !targetPath.isEmpty()) {
                throw new ConnectorException(30001);
            }
            if (!tableName.isEmpty() && targetPath.isEmpty()) {
                UserGroupInformation ugi = null;
                try {
                    ugi = UserGroupInformation.getCurrentUser();
                } catch (Exception e9) {
                    try {
                        final HadoopShims shims = ShimLoader.getHadoopShims();
                        Method mthd = null;
                        mthd = shims.getClass().getDeclaredMethod("getUGIForConf", Configuration.class);
                        ugi = (UserGroupInformation) mthd.invoke(shims, configuration);
                    } catch (Exception e2) {
                        throw new ConnectorException(e2.getMessage(), e2);
                    }
                }
                final String user = ugi.getShortUserName();
                final String userDirectory = "/user/" + user;
                final FileSystem fs = FileSystem.get(configuration);
                if (!fs.exists(new Path(userDirectory))) {
                    throw new ConnectorException(32009);
                }
                String path = userDirectory + "/temp_" + sdf.format(new Date());
                if (fs.exists(new Path(path))) {
                    final Long randomNumberLong = HiveUtils.getRandomNumber();
                    path = path + "_" + randomNumberLong.toString();
                }
                HivePlugInConfiguration.setOutputPaths(configuration, path);
            }
            if (!tableName.isEmpty()) {
                HiveUtils.loadHiveConf((Configuration) hiveConf, ConnectorConfiguration.direction.output);
                client = new HiveMetaStoreClient(hiveConf);
                boolean tableExisted = false;
                try {
                    tableExisted = client.tableExists(databaseName, tableName);
                    if (tableExisted) {
                        HiveSchemaUtils.logHiveTableExtInfo(this.logger, databaseName, tableName, hiveConf);
                        HiveSchemaUtils.logHivePartitionValues(this.logger, databaseName, tableName, hiveConf);
                    } else {
                        this.logger.info((Object) ("hive table " + databaseName + "." + tableName + " does not exist"));
                    }
                } catch (NoSuchObjectException e3) {
                    e3.printStackTrace();
                } catch (HiveException e4) {
                    e4.printStackTrace();
                }
                if (tableExisted) {
                    final String targetTableSchema = HivePlugInConfiguration.getOutputTableSchema(configuration);
                    if (!targetTableSchema.isEmpty()) {
                        throw new ConnectorException(30005);
                    }
                    HiveUtils.setHiveSchemaToOutputHiveTable(configuration, databaseName, tableName, client);
                } else {
                    final String targetTableSchema = HivePlugInConfiguration.getOutputTableSchema(configuration);
                    if (targetTableSchema.isEmpty()) {
                        throw new ConnectorException(30003);
                    }
                    if (ConnectorConfiguration.getOutputSerDe(configuration).contains("RCFile")) {
                        HivePlugInConfiguration.setOutputRCFileSerde(configuration, client.getConfigValue("hive.default.rcfile.serde", ""));
                    }
                }
            } else {
                final String targetTableSchema2 = HivePlugInConfiguration.getOutputTableSchema(configuration);
                if (targetTableSchema2.isEmpty()) {
                    throw new ConnectorException(30003);
                }
                if (ConnectorConfiguration.getOutputSerDe(configuration).contains("RCFile")) {
                    HiveUtils.loadHiveConf((Configuration) hiveConf, ConnectorConfiguration.direction.output);
                    client = new HiveMetaStoreClient(hiveConf);
                    HivePlugInConfiguration.setOutputRCFileSerde(configuration, client.getConfigValue("hive.default.rcfile.serde", ""));
                }
            }
            final String[] targetFieldNames = HivePlugInConfiguration.getOutputFieldNamesArray(configuration);
            final String targetPartitionSchema = HivePlugInConfiguration.getOutputPartitionSchema(configuration);
            final String targetTableSchema3 = HivePlugInConfiguration.getOutputTableSchema(configuration);
            if (targetPartitionSchema.length() > 0) {
                HiveUtils.checkPartitionNamesInSchema(targetTableSchema3, targetPartitionSchema);
            }
            if (targetFieldNames.length > 0) {
                HiveUtils.checkFieldNamesInSchema(targetFieldNames, targetTableSchema3, targetPartitionSchema);
                HiveUtils.checkFieldNamesContainPartitions(targetFieldNames, targetPartitionSchema);
            }
            this.setupSchema(configuration);
            return 0;
        } catch (MetaException e5) {
            throw new ConnectorException(e5.getMessage(), (Throwable) e5);
        } catch (UnknownDBException e6) {
            throw new ConnectorException(e6.getMessage(), (Throwable) e6);
        } catch (TException e7) {
            throw new ConnectorException(e7.getMessage(), (Throwable) e7);
        } catch (IOException e8) {
            throw new ConnectorException(e8.getMessage(), e8);
        } finally {
            if (client != null) {
                client.close();
                client = null;
            }
        }
    }

    private void setupSchema(final Configuration configuration) throws ConnectorException {
        String targetTableSchema = HivePlugInConfiguration.getOutputTableSchema(configuration).trim();
        final String partitionSchema = HivePlugInConfiguration.getOutputPartitionSchema(configuration).trim();
        if (partitionSchema.length() > 0) {
            targetTableSchema = targetTableSchema + "," + partitionSchema;
        }
        final List<String> columns = ConnectorSchemaUtils.parseColumns(targetTableSchema.toLowerCase());
        final List<String> columnNames = ConnectorSchemaUtils.parseColumnNames(columns);
        final List<String> columnTypes = ConnectorSchemaUtils.parseColumnTypes(columns);
        if (HivePlugInConfiguration.getOutputFieldNamesArray(configuration).length == 0) {
            HivePlugInConfiguration.setOutputFieldNamesArray(configuration, columnNames.toArray(new String[columnNames.size()]));
        }
        final String[] fieldNames = HivePlugInConfiguration.getOutputFieldNamesArray(configuration);
        final ConnectorRecordSchema r = new ConnectorRecordSchema(fieldNames.length);
        final int[] columnMapping = HiveSchemaUtils.getColumnMapping(columnNames.toArray(new String[columnNames.size()]), fieldNames);
        for (int i = 0; i < fieldNames.length; ++i) {
            r.setFieldType(i, HiveSchemaUtils.lookupHiveDataTypeByName(columnTypes.get(columnMapping[i])));
        }
        final ConnectorRecordSchema userSchema = ConnectorSchemaUtils.recordSchemaFromString(ConnectorConfiguration.getOutputConverterRecordSchema(configuration));
        ConnectorSchemaUtils.formalizeConnectorRecordSchema(r);
        if (userSchema != null) {
            HiveSchemaUtils.checkSchemaMatch(userSchema, r);
        } else {
            ConnectorConfiguration.setOutputConverterRecordSchema(configuration, ConnectorSchemaUtils.recordSchemaToString(r));
        }
        HivePlugInConfiguration.setOutputTableFieldTypes(configuration, columnTypes.toArray(new String[columnTypes.size()]));
        HivePlugInConfiguration.setOutputTableFieldNamesArray(configuration, columnNames.toArray(new String[columnNames.size()]));
    }

    @Override
    public int outputPostProcessor(final JobContext context) throws ConnectorException {
        final Configuration configuration = context.getConfiguration();
        if (!ConnectorConfiguration.getJobSucceeded(configuration)) {
            return 1;
        }
        final String databaseName = HivePlugInConfiguration.getOutputDatabase(configuration);
        final String tableName = HivePlugInConfiguration.getOutputTable(configuration);
        HiveMetaStoreClient client = null;
        try {
            if (!tableName.isEmpty()) {
                final HiveConf hiveConf = new HiveConf(configuration, (Class) HiveOutputProcessor.class);
                HiveUtils.loadHiveConf((Configuration) hiveConf, ConnectorConfiguration.direction.output);
                client = new HiveMetaStoreClient(hiveConf);
                final boolean tableExisted = client.tableExists(databaseName, tableName);
                if (!tableExisted) {
                    final String targetPartitionSchema = HivePlugInConfiguration.getOutputPartitionSchema(configuration);
                    HiveUtils.createHiveTable(configuration, databaseName, tableName, hiveConf, client);
                    if (!targetPartitionSchema.isEmpty()) {
                        HiveUtils.addPartitionsToHiveTable(configuration, databaseName, tableName, hiveConf, client);
                    } else {
                        HiveUtils.loadDataintoHiveTable(configuration, databaseName, tableName, hiveConf, client);
                    }
                } else {
                    final String targetPartitionSchema = HivePlugInConfiguration.getOutputPartitionSchema(configuration);
                    if (!targetPartitionSchema.isEmpty()) {
                        HiveUtils.addPartitionsToHiveTable(configuration, databaseName, tableName, hiveConf, client);
                    } else {
                        HiveUtils.loadDataintoHiveTable(configuration, databaseName, tableName, hiveConf, client);
                    }
                }
            }
        } catch (MetaException e) {
            throw new ConnectorException(e.getMessage(), (Throwable) e);
        } catch (UnknownDBException e2) {
            throw new ConnectorException(e2.getMessage(), (Throwable) e2);
        } catch (TException e3) {
            throw new ConnectorException(e3.getMessage(), (Throwable) e3);
        } catch (ConnectorException e4) {
            throw new ConnectorException(e4.getMessage(), e4);
        } finally {
            if (client != null) {
                client.close();
                client = null;
            }
        }
        return 0;
    }
}
