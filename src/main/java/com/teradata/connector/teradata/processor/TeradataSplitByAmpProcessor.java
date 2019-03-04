package com.teradata.connector.teradata.processor;

import com.teradata.connector.common.exception.ConnectorException;
import org.apache.hadoop.conf.Configuration;

public class TeradataSplitByAmpProcessor extends TeradataInputProcessor {
    @Override
    protected void setupDatabaseEnvironment(final Configuration configuration) throws ConnectorException {
    }

    @Override
    protected void cleanupDatabaseEnvironment(final Configuration configuration) throws ConnectorException {
    }
}
