package com.teradata.connector.teradata.processor;

import com.teradata.connector.common.ConnectorOutputProcessor;
import com.teradata.connector.common.exception.ConnectorException;
import com.teradata.connector.common.utils.ConnectorConfiguration;
import com.teradata.connector.teradata.db.TeradataConnection;
import com.teradata.connector.teradata.utils.TeradataPlugInConfiguration;
import com.teradata.connector.teradata.utils.TeradataSchemaUtils;
import com.teradata.connector.teradata.utils.TeradataUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.JobContext;


public abstract class TeradataOutputProcessor implements ConnectorOutputProcessor {
    private static Log logger = LogFactory.getLog((Class) TeradataOutputProcessor.class);
    protected TeradataConnection connection = null;

    @Override
    public int outputPreProcessor(final JobContext context) throws ConnectorException {
        try {
            final long startTime = System.currentTimeMillis();
            logger.info((Object) ("output preprocessor " + this.getClass().getName() + " starts at:  " + startTime));
            this.connection = TeradataUtils.openOutputConnection(context);
            final Configuration configuration = context.getConfiguration();
            this.validateConfiguration(configuration, this.connection);
            final String tdchVersion = TeradataUtils.getTdchVersionNumber();
            logger.info((Object) ("the teradata connector for hadoop version is: " + tdchVersion));
            logger.info((Object) ("output jdbc properties are " + TeradataPlugInConfiguration.getOutputJdbcUrl(configuration)));
            TeradataSchemaUtils.setupTeradataTargetTableSchema(configuration, this.connection);
            TeradataSchemaUtils.configureTargetConnectorRecordSchema(configuration);
            this.setupDatabaseEnvironment(configuration);
            logger.info((Object) ("the number of mappers are " + ConnectorConfiguration.getNumMappers(configuration)));
            final long endTime = System.currentTimeMillis();
            logger.info((Object) ("output preprocessor " + this.getClass().getName() + " ends at:  " + endTime));
            logger.info((Object) ("the total elapsed time of output preprocessor " + this.getClass().getName() + " is: " + (endTime - startTime) / 1000L + "s"));
        } finally {
            try {
                TeradataUtils.closeConnection(this.connection);
            } catch (ConnectorException e) {
                e.printStackTrace();
            }
        }
        return 0;
    }

    @Override
    public int outputPostProcessor(final JobContext context) throws ConnectorException {
        final long startTime = System.currentTimeMillis();
        logger.info((Object) ("output postprocessor " + this.getClass().getName() + " starts at:  " + startTime));
        try {
            this.connection = TeradataUtils.openOutputConnection(context);
            this.cleanupDatabaseEnvironment(context.getConfiguration());
        } finally {
            try {
                TeradataUtils.closeConnection(this.connection);
            } catch (ConnectorException e) {
                e.printStackTrace();
            }
            final long endTime = System.currentTimeMillis();
            logger.info((Object) ("output postprocessor " + this.getClass().getName() + " ends at:  " + startTime));
            logger.info((Object) ("the total elapsed time of output postprocessor " + this.getClass().getName() + " is: " + (endTime - startTime) / 1000L + "s"));
        }
        return 0;
    }

    protected abstract void setupDatabaseEnvironment(final Configuration p0) throws ConnectorException;

    protected abstract void cleanupDatabaseEnvironment(final Configuration p0) throws ConnectorException;

    protected void validateConfiguration(final Configuration configuration, final TeradataConnection connection) throws ConnectorException {
        TeradataUtils.validateOutputTeradataProperties(configuration, connection);
    }
}
