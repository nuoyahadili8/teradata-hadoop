package com.teradata.connector.sample.plugin.aster;

import com.teradata.connector.common.exception.ConnectorException;
import com.teradata.connector.sample.CommonDBInputFormat;
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
public class AsterInputFormat extends CommonDBInputFormat {
    protected static final String SQL_SELECT_COLUMN_SPLIT_RANGE = "SELECT %s FROM %s WHERE %s";
    protected static final String SQL_GET_COLUMN_VALUE_MIN_MAX = "SELECT MIN( %s ), MAX( %s ) FROM %s";

    @Override
    public String getSplitRangeSQL(final Configuration configuration) {
        final String[] inputFieldNamesArray = CommonDBConfiguration.getInputFieldNamesArray(configuration);
        final String inputTableName = CommonDBUtils.getQuotedEscapedName(AsterDBConfiguration.getAsterInputDatabase(configuration), AsterDBConfiguration.getAsterInputSchema(configuration), AsterDBConfiguration.getAsterInputTable(configuration));
        return String.format("SELECT %s FROM %s WHERE %s", CommonDBSchemaUtils.concatFieldNamesArray(CommonDBSchemaUtils.quoteFieldNamesArray(inputFieldNamesArray)), inputTableName, "");
    }

    @Override
    public String getMinMaxSQL(final Configuration configuration) {
        final String splitColumnName = AsterDBConfiguration.getInputSplitByColumn(configuration);
        final String inputTableName = CommonDBUtils.getQuotedEscapedName(AsterDBConfiguration.getAsterInputDatabase(configuration), AsterDBConfiguration.getAsterInputSchema(configuration), AsterDBConfiguration.getAsterInputTable(configuration));
        return String.format("SELECT MIN( %s ), MAX( %s ) FROM %s", splitColumnName, splitColumnName, inputTableName);
    }

    @Override
    public String getOneAmpSQL(final Configuration configuration) {
        final String[] inputFieldNamesArray = CommonDBConfiguration.getInputFieldNamesArray(configuration);
        final String inputTableName = CommonDBUtils.getQuotedEscapedName(AsterDBConfiguration.getAsterInputDatabase(configuration), AsterDBConfiguration.getAsterInputSchema(configuration), AsterDBConfiguration.getAsterInputTable(configuration));
        return String.format("SELECT %s FROM %s WHERE %s", CommonDBSchemaUtils.concatFieldNamesArray(CommonDBSchemaUtils.quoteFieldNamesArray(inputFieldNamesArray)), inputTableName, "");
    }

    @Override
    public void validataConfiguration(final Configuration configuration) throws ConnectorException {
        if (AsterDBConfiguration.getInputSplitByColumn(configuration).trim().equals("")) {
            throw new ConnectorException("Missing split column");
        }
    }

    @Override
    public Connection getConnection(final Configuration conf) throws ConnectorException {
        return AsterDBUtils.openInputConnection(conf);
    }

    @Override
    public String getSplitColumn(final Configuration configuration) {
        return AsterDBConfiguration.getInputSplitByColumn(configuration);
    }
}
