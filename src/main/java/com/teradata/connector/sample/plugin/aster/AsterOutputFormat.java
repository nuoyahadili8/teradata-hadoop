package com.teradata.connector.sample.plugin.aster;

import com.teradata.connector.common.exception.ConnectorException;
import com.teradata.connector.sample.CommonDBOutputFormat;
import com.teradata.connector.sample.plugin.aster.utils.AsterDBConfiguration;
import com.teradata.connector.sample.plugin.aster.utils.AsterDBUtils;
import com.teradata.connector.sample.plugin.utils.CommonDBConfiguration;
import com.teradata.connector.sample.plugin.utils.CommonDBSchemaUtils;
import com.teradata.connector.sample.plugin.utils.CommonDBUtils;
import java.sql.Connection;
import org.apache.hadoop.conf.Configuration;


/**
 * @author Administrator
 */
public class AsterOutputFormat<K, V> extends CommonDBOutputFormat<K, V> {
    protected static final String SQL_INSERT = "INSERT INTO %s (%s) VALUES (%s)";

    @Override
    public String getInsertPreparedStatmentSQL(final Configuration configuration) {
        final String[] columns = CommonDBConfiguration.getOutputFieldNamesArray(configuration);
        final String outputDatabase = AsterDBConfiguration.getAsterOutputDatabase(configuration);
        String outputTableName = AsterDBConfiguration.getAsterOutputTable(configuration);
        final String outputSchema = AsterDBConfiguration.getAsterOutputSchema(configuration);
        outputTableName = CommonDBUtils.getQuotedEscapedName(outputDatabase, outputSchema, outputTableName);
        final StringBuilder colExpBuilder = new StringBuilder();
        final StringBuilder valuesExpBuilder = new StringBuilder();
        for (int i = 0; i < columns.length; ++i) {
            if (i > 0) {
                colExpBuilder.append(", ");
                valuesExpBuilder.append(", ");
            }
            colExpBuilder.append(CommonDBSchemaUtils.quoteFieldNameForSql(columns[i]));
            valuesExpBuilder.append('?');
        }
        return String.format("INSERT INTO %s (%s) VALUES (%s)", outputTableName, colExpBuilder.toString(), valuesExpBuilder.toString());
    }

    @Override
    public Connection getConnection(final Configuration configuration) throws ConnectorException {
        return AsterDBUtils.openOutputConnection(configuration);
    }
}
