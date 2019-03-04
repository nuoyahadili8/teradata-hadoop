package com.teradata.connector.teradata.processor;

import com.teradata.connector.common.exception.ConnectorException;
import com.teradata.connector.common.utils.ConnectorConfiguration;
import com.teradata.connector.teradata.db.TeradataConnection;
import com.teradata.connector.teradata.utils.TeradataPlugInConfiguration;
import java.sql.SQLException;
import org.apache.hadoop.conf.Configuration;


public class TeradataSplitByValueProcessor extends TeradataInputProcessor {
    @Override
    protected void setupDatabaseEnvironment(final Configuration configuration) throws ConnectorException {
    }

    @Override
    protected void cleanupDatabaseEnvironment(final Configuration configuration) throws ConnectorException {
    }

    @Override
    protected void validateConfiguration(final Configuration configuration, final TeradataConnection connection) throws ConnectorException {
        super.validateConfiguration(configuration, connection);
        final String inputTableName = TeradataConnection.getQuotedEscapedName(TeradataPlugInConfiguration.getInputDatabase(configuration), TeradataPlugInConfiguration.getInputTable(configuration));
        final String splitColumn = this.getSplitColumn(inputTableName, configuration);
        TeradataPlugInConfiguration.setInputSplitByColumn(configuration, splitColumn);
    }

    protected String getSplitColumn(final String tableName, final Configuration configuration) throws ConnectorException {
        String splitColumn = TeradataPlugInConfiguration.getInputSplitByColumn(configuration);
        final int numMappers = ConnectorConfiguration.getNumMappers(configuration);
        if (!splitColumn.isEmpty() || numMappers == 1) {
            return splitColumn;
        }
        try {
            final String[] primaryKeys = this.connection.getPrimaryKey(tableName);
            if (primaryKeys.length > 0) {
                splitColumn = primaryKeys[0];
            } else {
                String[] primaryIndexs = this.connection.getPrimaryIndex(tableName);
                if (primaryIndexs.length < 1) {
                    final String[] tempPrimaryIndexs = this.connection.getColumnNamesForTable(tableName);
                    if (tempPrimaryIndexs.length > 0) {
                        primaryIndexs = new String[]{tempPrimaryIndexs[0]};
                    }
                }
                if (primaryIndexs.length <= 0) {
                    throw new ConnectorException(23002);
                }
                splitColumn = primaryIndexs[0];
            }
        } catch (SQLException e) {
            throw new ConnectorException(e.getMessage(), e);
        }
        return splitColumn;
    }
}
