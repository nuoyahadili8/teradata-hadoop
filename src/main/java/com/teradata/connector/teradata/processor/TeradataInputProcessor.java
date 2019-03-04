package com.teradata.connector.teradata.processor;

import com.teradata.connector.common.ConnectorInputProcessor;
import com.teradata.connector.common.exception.ConnectorException;
import com.teradata.connector.common.utils.ConnectorConfiguration;
import com.teradata.connector.hive.utils.HivePlugInConfiguration;
import com.teradata.connector.hive.utils.HiveUtils;
import com.teradata.connector.teradata.db.TeradataConnection;
import com.teradata.connector.teradata.utils.TeradataPlugInConfiguration;
import com.teradata.connector.teradata.utils.TeradataSchemaUtils;
import com.teradata.connector.teradata.utils.TeradataUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.metastore.HiveMetaStoreClient;
import org.apache.hadoop.hive.metastore.api.MetaException;
import org.apache.hadoop.hive.metastore.api.UnknownDBException;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.thrift.TException;

import java.sql.SQLException;

public abstract class TeradataInputProcessor implements ConnectorInputProcessor {
    private static Log logger = LogFactory.getLog((Class) TeradataInputProcessor.class);
    protected TeradataConnection connection;
    protected static final String INNER_QUERY_ALIAS = "DT";

    public TeradataInputProcessor() {
        this.connection = null;
    }

    @Override
    public int inputPreProcessor(final JobContext context) throws ConnectorException {
        try {
            final long startTime = System.currentTimeMillis();
            logger.info((Object) ("input preprocessor " + this.getClass().getName() + " starts at:  " + startTime));
            this.connection = TeradataUtils.openInputConnection(context);
            final Configuration configuration = context.getConfiguration();
            String inputTableName = TeradataPlugInConfiguration.getInputTable(configuration);
            if (!inputTableName.isEmpty()) {
                try {
                    inputTableName = TeradataConnection.getQuotedEscapedName(inputTableName);
                    if (!this.connection.isTableNonEmpty(inputTableName)) {
                        logger.warn("input source table is empty");
                        final String databaseName = HivePlugInConfiguration.getOutputDatabase(configuration);
                        final String tableName = HivePlugInConfiguration.getOutputTable(configuration);
                        if (!tableName.isEmpty()) {
                            HiveMetaStoreClient client = null;
                            try {
                                final HiveConf hiveConf = new HiveConf(configuration, (Class) TeradataInputProcessor.class);
                                HiveUtils.loadHiveConf((Configuration) hiveConf, ConnectorConfiguration.direction.output);
                                client = new HiveMetaStoreClient(hiveConf);
                                final boolean tableExisted = client.tableExists(databaseName, tableName);
                                if (!tableExisted) {
                                    logger.info((Object) "creating target hive table");
                                    HiveUtils.createHiveTable(configuration, databaseName, tableName, hiveConf, client);
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
                        }
                        final long endTime = System.currentTimeMillis();
                        logger.info((Object) ("input preprocessor " + this.getClass().getName() + " ends at:  " + endTime));
                        logger.info((Object) ("the total elapsed time of input preprocessor " + this.getClass().getName() + " is: " + (endTime - startTime) / 1000L + "s"));
                        TeradataUtils.closeConnection(this.connection);
                        return 1001;
                    }
                } catch (SQLException e5) {
                    throw new ConnectorException(e5.getMessage(), e5);
                }
            }
            this.validateConfiguration(configuration, this.connection);
            final String tdchVersion = TeradataUtils.getTdchVersionNumber();
            logger.info("the teradata connector for hadoop version is: " + tdchVersion);
            logger.info((Object) ("input jdbc properties are " + TeradataPlugInConfiguration.getInputJdbcUrl(configuration)));
            TeradataSchemaUtils.setupTeradataSourceTableSchema(configuration, this.connection);
            this.setupDatabaseEnvironment(configuration);
            TeradataSchemaUtils.configureSourceConnectorRecordSchema(configuration);
            logger.info((Object) ("the number of mappers are " + ConnectorConfiguration.getNumMappers(configuration)));
            final long endTime2 = System.currentTimeMillis();
            logger.info((Object) ("input preprocessor " + this.getClass().getName() + " ends at:  " + endTime2));
            logger.info((Object) ("the total elapsed time of input preprocessor " + this.getClass().getName() + " is: " + (endTime2 - startTime) / 1000L + "s"));
        } finally {
            try {
                TeradataUtils.closeConnection(this.connection);
            } catch (ConnectorException e6) {
                e6.printStackTrace();
            }
        }
        return 0;
    }

    @Override
    public int inputPostProcessor(final JobContext context) throws ConnectorException {
        final long startTime = System.currentTimeMillis();
        logger.info((Object) ("input postprocessor " + this.getClass().getName() + " starts at:  " + startTime));
        try {
            this.connection = TeradataUtils.openInputConnection(context);
            this.cleanupDatabaseEnvironment(context.getConfiguration());
        } finally {
            try {
                TeradataUtils.closeConnection(this.connection);
            } catch (ConnectorException e) {
                e.printStackTrace();
            }
            final long endTime = System.currentTimeMillis();
            logger.info((Object) ("input postprocessor " + this.getClass().getName() + " ends at:  " + startTime));
            logger.info((Object) ("the total elapsed time of input postprocessor " + this.getClass().getName() + " is: " + (endTime - startTime) / 1000L + "s"));
        }
        return 0;
    }

    protected abstract void setupDatabaseEnvironment(final Configuration p0) throws ConnectorException;

    protected abstract void cleanupDatabaseEnvironment(final Configuration p0) throws ConnectorException;

    protected void validateConfiguration(final Configuration configuration, final TeradataConnection connection) throws ConnectorException {
        TeradataUtils.validateInputTeradataProperties(configuration, connection);
    }
}
