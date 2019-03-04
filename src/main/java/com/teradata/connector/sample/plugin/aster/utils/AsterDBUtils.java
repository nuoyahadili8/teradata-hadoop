package com.teradata.connector.sample.plugin.aster.utils;

import org.apache.hadoop.conf.*;

import java.sql.*;

import com.teradata.connector.common.exception.*;

public class AsterDBUtils {
    public static Connection openInputConnection(final Configuration configuration) throws ConnectorException {
        final String jdbcClassName = AsterDBConfiguration.getInputJdbcDriverClass(configuration);
        final String jdbcUrl = AsterDBConfiguration.getInputJdbcUrl(configuration);
        final String jdbcUserName = AsterDBConfiguration.getInputJdbcUserName(configuration);
        final String jdbcPassword = AsterDBConfiguration.getInputJdbcPassword(configuration);
        try {
            Class.forName(jdbcClassName);
            return DriverManager.getConnection(jdbcUrl, jdbcUserName, jdbcPassword);
        } catch (Exception e) {
            throw new ConnectorException(e.getMessage(), e);
        }
    }

    public static Connection openOutputConnection(final Configuration configuration) throws ConnectorException {
        final String jdbcClassName = AsterDBConfiguration.getOutputJdbcDriverClass(configuration);
        final String jdbcUrl = AsterDBConfiguration.getOutputJdbcUrl(configuration);
        final String jdbcUserName = AsterDBConfiguration.getOutputJdbcUserName(configuration);
        final String jdbcPassword = AsterDBConfiguration.getOutputJdbcPassword(configuration);
        try {
            Class.forName(jdbcClassName);
            return DriverManager.getConnection(jdbcUrl, jdbcUserName, jdbcPassword);
        } catch (Exception e) {
            throw new ConnectorException(e.getMessage(), e);
        }
    }
}
