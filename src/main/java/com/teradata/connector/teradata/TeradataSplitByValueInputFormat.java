package com.teradata.connector.teradata;

import com.teradata.connector.common.exception.ConnectorException;
import com.teradata.connector.common.exception.ConnectorException.ErrorCode;
import com.teradata.connector.common.utils.ConnectorConfiguration;
import com.teradata.connector.common.utils.ConnectorSchemaUtils;
import com.teradata.connector.common.utils.HadoopConfigurationUtils;
import com.teradata.connector.teradata.TeradataInputFormat.TeradataInputSplit;
import com.teradata.connector.teradata.db.TeradataConnection;
import com.teradata.connector.teradata.utils.TeradataPlugInConfiguration;
import com.teradata.connector.teradata.utils.TeradataSplitUtils;
import com.teradata.connector.teradata.utils.TeradataUtils;
import java.io.IOException;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.JobContext;


/**
 * @author Administrator
 */
public class TeradataSplitByValueInputFormat extends TeradataInputFormat {
    private static Log logger;
    protected static final String SQL_SELECT_COLUMN_SPLIT_RANGE = "SELECT %s FROM %s WHERE %s";
    protected static final String SQL_GET_COLUMN_VALUE_MIN_MAX = "SELECT MIN( %s ), MAX( %s ) FROM %s";
    protected static final int NUM_MAPPER_DEFAULT = 20;

    @Override
    public void validateConfiguration(final JobContext context) throws ConnectorException {
        super.validateConfiguration(context);
        final Configuration configuration = context.getConfiguration();
        final String splitColumn = TeradataPlugInConfiguration.getInputSplitByColumn(configuration);
        final int numMappers = ConnectorConfiguration.getNumMappers(configuration);
        if (splitColumn.isEmpty() && numMappers != 1) {
            throw new ConnectorException(23002);
        }
        this.connection = TeradataUtils.openInputConnection(context);
    }

    public List<InputSplit> getSplits(final JobContext context) throws IOException, InterruptedException {
        this.validateConfiguration(context);
        final Configuration configuration = context.getConfiguration();
        this.inputTableName = TeradataConnection.getQuotedEscapedName(TeradataPlugInConfiguration.getInputDatabase(configuration), TeradataPlugInConfiguration.getInputTable(configuration));
        this.inputConditions = TeradataPlugInConfiguration.getInputConditions(configuration);
        this.inputFieldNamesArray = TeradataPlugInConfiguration.getInputFieldNamesArray(configuration);
        final Boolean accessLock = TeradataPlugInConfiguration.getInputAccessLock(configuration);
        final int numMappers = ConnectorConfiguration.getNumMappers(configuration);
        final String[] locations = HadoopConfigurationUtils.getAllActiveHosts(context);
        final List<InputSplit> splits = new ArrayList<InputSplit>();
        if (numMappers == 1) {
            String inputSplitSql = TeradataConnection.getSelectSQL(this.inputTableName, this.inputFieldNamesArray, this.inputConditions);
            if (accessLock) {
                inputSplitSql = TeradataConnection.addAccessLockToSql(inputSplitSql);
            }
            final TeradataInputSplit split = new TeradataInputSplit(inputSplitSql);
            split.setLocations(HadoopConfigurationUtils.selectUniqueActiveHosts(locations, 6));
            splits.add(split);
        } else {
            final String splitColumnName = TeradataPlugInConfiguration.getInputSplitByColumn(configuration);
            String splitSql;
            if (this.inputConditions.isEmpty()) {
                splitSql = String.format("SELECT %s FROM %s WHERE %s", ConnectorSchemaUtils.concatFieldNamesArray(ConnectorSchemaUtils.quoteFieldNamesArray(this.inputFieldNamesArray)), this.inputTableName, "");
            } else {
                splitSql = String.format("SELECT %s FROM %s WHERE %s", ConnectorSchemaUtils.concatFieldNamesArray(ConnectorSchemaUtils.quoteFieldNamesArray(this.inputFieldNamesArray)), this.inputTableName, "(" + this.inputConditions + ") AND ");
            }
            TeradataPlugInConfiguration.setInputSplitSql(configuration, splitSql);
            ResultSet resultSetMinMaxValues = null;
            Statement statement = null;
            try {
                final Connection sqlConnection = this.connection.getConnection();
                if (sqlConnection != null) {
                    String sql = String.format("SELECT MIN( %s ), MAX( %s ) FROM %s", splitColumnName, splitColumnName, this.inputTableName);
                    if (accessLock) {
                        sql = TeradataConnection.addAccessLockToSql(sql);
                    }
                    TeradataSplitByValueInputFormat.logger.info((Object) sql);
                    statement = sqlConnection.createStatement();
                    resultSetMinMaxValues = statement.executeQuery(sql);
                }
                if (resultSetMinMaxValues == null || !resultSetMinMaxValues.next()) {
                    throw new ConnectorException(23003);
                }
                if (resultSetMinMaxValues.getString(1) == null || resultSetMinMaxValues.getString(2) == null) {
                    throw new ConnectorException(23004);
                }
            } catch (SQLException e) {
                TeradataUtils.closeConnection(this.connection);
                throw new ConnectorException(e.getMessage(), e);
            }
            final List<TeradataInputSplit> columnSplits = TeradataSplitUtils.getSplitsByColumnType(configuration, splitColumnName, resultSetMinMaxValues);
            for (int i = 0; i < columnSplits.size(); ++i) {
                final TeradataInputSplit split2 = columnSplits.get(i);
                if (accessLock) {
                    final String inputSplitSql2 = TeradataConnection.addAccessLockToSql(split2.getSplitSql());
                    split2.setSplitSql(inputSplitSql2);
                }
                split2.setLocations(HadoopConfigurationUtils.selectUniqueActiveHosts(locations, 6));
                splits.add(split2);
            }
            try {
                if (resultSetMinMaxValues != null && !resultSetMinMaxValues.isClosed()) {
                    resultSetMinMaxValues.close();
                    resultSetMinMaxValues = null;
                }
                if (statement != null && !statement.isClosed()) {
                    statement.close();
                    statement = null;
                }
            } catch (SQLException e2) {
                throw new ConnectorException(e2.getMessage(), e2);
            } finally {
                TeradataUtils.closeConnection(this.connection);
            }
        }
        return splits;
    }

    static {
        TeradataSplitByValueInputFormat.logger = LogFactory.getLog((Class) TeradataSplitByValueInputFormat.class);
    }
}
