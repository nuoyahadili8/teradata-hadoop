package com.teradata.connector.teradata.processor;

import com.teradata.connector.common.exception.ConnectorException;
import com.teradata.connector.common.utils.ConnectorConfiguration;
import com.teradata.connector.teradata.db.TeradataConnection;
import com.teradata.connector.teradata.utils.TeradataPlugInConfiguration;
import java.sql.SQLException;
import org.apache.hadoop.conf.Configuration;


public class TeradataSplitByHashProcessor extends TeradataInputProcessor {
    @Override
    protected void setupDatabaseEnvironment(final Configuration configuration) throws ConnectorException {
    }

    @Override
    protected void cleanupDatabaseEnvironment(final Configuration configuration) throws ConnectorException {
    }

    @Override
    protected void validateConfiguration(final Configuration configuration, final TeradataConnection connection) throws ConnectorException {
        super.validateConfiguration(configuration, connection);
        final int numMappers = ConnectorConfiguration.getNumMappers(configuration);
        String splitColumn = TeradataPlugInConfiguration.getInputSplitByColumn(configuration);
        if (splitColumn.isEmpty() && numMappers != 1) {
            try {
                final String inputTableName = TeradataConnection.getQuotedEscapedName(TeradataPlugInConfiguration.getInputDatabase(configuration), TeradataPlugInConfiguration.getInputTable(configuration));
                String[] piColNames = connection.getPrimaryIndex(inputTableName);
                if (piColNames.length < 1) {
                    final String[] tempPiColNames = connection.getColumnNamesForTable(inputTableName);
                    if (tempPiColNames.length > 0) {
                        piColNames = new String[]{tempPiColNames[0]};
                    }
                }
                if (piColNames.length == 0) {
                    throw new ConnectorException(23002);
                }
                splitColumn = piColNames[0];
                TeradataPlugInConfiguration.setInputSplitByColumn(configuration, splitColumn);
            } catch (SQLException e) {
                throw new ConnectorException(e.getMessage(), e);
            }
        }
    }
}
