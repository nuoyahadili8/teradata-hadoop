package com.teradata.connector.teradata.processor;

import com.teradata.connector.common.exception.ConnectorException;
import com.teradata.connector.common.utils.ConnectorConfiguration;
import com.teradata.connector.teradata.db.TeradataConnection;
import com.teradata.connector.teradata.utils.TeradataPlugInConfiguration;
import com.teradata.jdbc.jdbc_4.TDSession;
import java.sql.SQLException;
import org.apache.hadoop.conf.Configuration;


/**
 * @author Administrator
 */
public class TeradataInternalFastExportProcessor extends TeradataInputProcessor {
    public static boolean jobSuccess = true;

    @Override
    protected void setupDatabaseEnvironment(final Configuration configuration) throws ConnectorException {
        try {
            TeradataPlugInConfiguration.setInputFastFail(configuration, ((TDSession) this.connection.getConnection()).getConfigResponse().isFailFastSupported());
        } catch (SQLException e1) {
            throw new ConnectorException(e1.getMessage(), e1);
        }
    }

    @Override
    protected void cleanupDatabaseEnvironment(final Configuration configuration) throws ConnectorException {
    }

    @Override
    protected void validateConfiguration(final Configuration configuration, final TeradataConnection connection) throws ConnectorException {
        super.validateConfiguration(configuration, connection);
        int numMappers = 0;
        int numAmps = 0;
        try {
            numAmps = connection.getAMPCount();
        } catch (SQLException e) {
            throw new ConnectorException(e.getMessage(), e);
        }
        numMappers = ConnectorConfiguration.getNumMappers(configuration);
        if (numMappers == 0 || numMappers > numAmps) {
            numMappers = numAmps;
            ConnectorConfiguration.setNumMappers(configuration, numMappers);
        }
    }
}
