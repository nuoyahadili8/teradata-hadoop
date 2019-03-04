package com.teradata.connector.teradata;

import com.teradata.connector.common.exception.ConnectorException;
import com.teradata.connector.common.utils.ConnectorConfiguration;
import com.teradata.connector.common.utils.ConnectorSchemaUtils;
import com.teradata.connector.common.utils.HadoopConfigurationUtils;
import com.teradata.connector.teradata.db.TeradataConnection;
import com.teradata.connector.teradata.utils.TeradataPlugInConfiguration;
import com.teradata.connector.teradata.utils.TeradataUtils;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.JobContext;

/**
 * @author Administrator
 */
public class TeradataSplitByHashInputFormat extends TeradataInputFormat {
    protected static final String SQL_SELECT_NOSTAGE_RANGE = "SELECT %s FROM %s WHERE %s HASHAMP(HASHBUCKET(HASHROW(%s)))";

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

    @Override
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
            final String splitColumn = TeradataPlugInConfiguration.getInputSplitByColumn(configuration);
            String splitSql;
            if (this.inputConditions.isEmpty()) {
                splitSql = String.format("SELECT %s FROM %s WHERE %s HASHAMP(HASHBUCKET(HASHROW(%s)))", ConnectorSchemaUtils.concatFieldNamesArray(ConnectorSchemaUtils.quoteFieldNamesArray(this.inputFieldNamesArray)), this.inputTableName, "", TeradataConnection.getQuotedName(splitColumn));
            } else {
                splitSql = String.format("SELECT %s FROM %s WHERE %s HASHAMP(HASHBUCKET(HASHROW(%s)))", ConnectorSchemaUtils.concatFieldNamesArray(ConnectorSchemaUtils.quoteFieldNamesArray(this.inputFieldNamesArray)), this.inputTableName, "(" + this.inputConditions + ") AND ", TeradataConnection.getQuotedName(splitColumn));
            }
            final int numAmps = TeradataPlugInConfiguration.getInputNumAmps(configuration);
            final int ampRange = numAmps / numMappers;
            final int remainder = numAmps % numMappers;
            if (numAmps == numMappers) {
                for (int i = 0; i < numMappers; ++i) {
                    String inputSplitSql2 = splitSql + " = " + i;
                    if (accessLock) {
                        inputSplitSql2 = TeradataConnection.addAccessLockToSql(inputSplitSql2);
                    }
                    final TeradataInputSplit split2 = new TeradataInputSplit(inputSplitSql2);
                    split2.setLocations(HadoopConfigurationUtils.selectUniqueActiveHosts(locations, 6));
                    splits.add(split2);
                }
            } else {
                int currAmpId = 0;
                for (int j = 1; j <= remainder; ++j) {
                    String inputSplitSql3 = splitSql + " BETWEEN " + currAmpId + " AND " + String.valueOf(currAmpId + ampRange);
                    if (accessLock) {
                        inputSplitSql3 = TeradataConnection.addAccessLockToSql(inputSplitSql3);
                    }
                    final TeradataInputSplit split3 = new TeradataInputSplit(inputSplitSql3);
                    split3.setLocations(HadoopConfigurationUtils.selectUniqueActiveHosts(locations, 6));
                    splits.add(split3);
                    currAmpId = currAmpId + ampRange + 1;
                }
                for (int j = remainder + 1; j < numMappers; ++j) {
                    String inputSplitSql3 = splitSql + " BETWEEN " + currAmpId + " AND " + String.valueOf(currAmpId + ampRange - 1);
                    if (accessLock) {
                        inputSplitSql3 = TeradataConnection.addAccessLockToSql(inputSplitSql3);
                    }
                    final TeradataInputSplit split3 = new TeradataInputSplit(inputSplitSql3);
                    split3.setLocations(HadoopConfigurationUtils.selectUniqueActiveHosts(locations, 6));
                    splits.add(split3);
                    currAmpId += ampRange;
                }
                String inputSplitSql2 = splitSql + " BETWEEN " + currAmpId + " AND " + String.valueOf(numAmps - 1);
                if (accessLock) {
                    inputSplitSql2 = TeradataConnection.addAccessLockToSql(inputSplitSql2);
                }
                final TeradataInputSplit split2 = new TeradataInputSplit(inputSplitSql2);
                split2.setLocations(HadoopConfigurationUtils.selectUniqueActiveHosts(locations, 6));
                splits.add(split2);
            }
        }
        TeradataUtils.closeConnection(this.connection);
        return splits;
    }
}
