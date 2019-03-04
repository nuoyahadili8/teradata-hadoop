package com.teradata.connector.sample.plugin.aster.processor;

import com.teradata.connector.sample.plugin.processor.*;
import org.apache.hadoop.conf.*;
import com.teradata.connector.sample.plugin.aster.utils.*;
import com.teradata.connector.sample.plugin.utils.*;
import com.teradata.connector.common.exception.*;

import java.sql.*;

public class AsterBatchInsertProcessor extends CommonDBOutputProcessor {
    @Override
    public String getTableName(final Configuration configuration) throws ConnectorException {
        Connection connection = null;
        try {
            connection = AsterDBUtils.openOutputConnection(configuration);
            final String databaseName = AsterDBConfiguration.getAsterOutputDatabase(configuration);
            final String schemaName = AsterDBConfiguration.getAsterOutputSchema(configuration);
            String tableName = AsterDBConfiguration.getAsterOutputTable(configuration);
            tableName = CommonDBUtils.getQuotedEscapedName(databaseName, schemaName, tableName);
            return tableName;
        } catch (ConnectorException e) {
            throw new ConnectorException(e.getMessage(), e);
        } finally {
            CommonDBUtils.CloseConnection(connection);
        }
    }

    @Override
    public Connection getConnection(final Configuration conf) throws ConnectorException {
        return AsterDBUtils.openOutputConnection(conf);
    }
}
