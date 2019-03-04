package com.teradata.connector.teradata;

import com.teradata.connector.common.exception.ConnectorException;
import com.teradata.connector.common.utils.ConnectorConfiguration;
import com.teradata.connector.common.utils.ConnectorSchemaUtils;
import com.teradata.connector.common.utils.HadoopConfigurationUtils;
import com.teradata.connector.teradata.db.TeradataConnection;
import com.teradata.connector.teradata.utils.TeradataPlugInConfiguration;
import com.teradata.connector.teradata.utils.TeradataUtils;
import java.io.IOException;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.JobContext;


public class TeradataSplitByAmpInputFormat extends TeradataInputFormat {
    private static final int PARALLEL_IMPORT_MIN_DB_MAJOR_VERSION = 14;
    private static final int PARALLEL_IMPORT_MIN_DB_MINOR_VERSION = 10;
    protected static final String KEYWORD_REPLACE_EXP = "TDIN_AMP_ID";
    protected static final String SQL_SELECT_AMP_OPERATOR = "SELECT %s FROM tdampcopy(ON %s USING AMPList(TDIN_AMP_ID)) AS THCALIAS1";

    @Override
    public void validateConfiguration(final JobContext context) throws ConnectorException {
        super.validateConfiguration(context);
        final Configuration configuration = context.getConfiguration();
        this.inputTableName = TeradataConnection.getQuotedEscapedName(TeradataPlugInConfiguration.getInputDatabase(configuration), TeradataPlugInConfiguration.getInputTable(configuration));
        this.inputFieldNamesArray = TeradataPlugInConfiguration.getInputFieldNamesArray(configuration);
        this.inputConditions = TeradataPlugInConfiguration.getInputConditions(configuration);
        final String inputQuery = TeradataConnection.getSelectSQL(this.inputTableName, this.inputFieldNamesArray, this.inputConditions);
        int numMappers = ConnectorConfiguration.getNumMappers(configuration);
        final int numAmps = TeradataPlugInConfiguration.getInputNumAmps(configuration);
        final boolean accessLock = TeradataPlugInConfiguration.getInputAccessLock(configuration);
        String inputFieldNames = "";
        if (this.inputFieldNamesArray.length == 0) {
            inputFieldNames = "*";
        } else {
            this.inputFieldNamesArray = ConnectorSchemaUtils.quoteFieldNamesArray(this.inputFieldNamesArray);
            inputFieldNames = ConnectorSchemaUtils.concatFieldNamesArray(this.inputFieldNamesArray);
        }
        this.connection = TeradataUtils.openInputConnection(context);
        try {
            this.connection.getDatabaseProperty();
            final int dbMajorVersion = this.connection.getDatabaseMajorVersion();
            final int dbMinorVersion = this.connection.getDatabaseMinorVersion();
            if (dbMajorVersion < 14 || (dbMajorVersion == 14 && dbMinorVersion < 10)) {
                throw new ConnectorException(12014);
            }
        } catch (SQLException e) {
            throw new ConnectorException(e.getMessage(), e);
        }
        if (numMappers <= 0) {
            numMappers = numAmps;
            ConnectorConfiguration.setNumMappers(configuration, numMappers);
        }
        if (numMappers == 1) {
            String inputSplitSql = String.format("SELECT %s FROM tdampcopy(ON %s USING AMPList(TDIN_AMP_ID)) AS THCALIAS1", inputFieldNames, this.inputTableName);
            if (accessLock) {
                inputSplitSql = TeradataConnection.addAccessLockToSql(inputSplitSql);
            }
            if (!this.inputConditions.isEmpty()) {
                inputSplitSql = inputSplitSql + " WHERE " + this.inputConditions;
            }
            TeradataPlugInConfiguration.setInputSplitSql(configuration, inputSplitSql);
        } else {
            if (numAmps < numMappers) {
                numMappers = numAmps;
                ConnectorConfiguration.setNumMappers(configuration, numMappers);
            }
            String inputSplitSql = String.format("SELECT %s FROM tdampcopy(ON %s USING AMPList(TDIN_AMP_ID)) AS THCALIAS1", inputFieldNames, this.inputTableName);
            if (accessLock) {
                inputSplitSql = TeradataConnection.addAccessLockToSql(inputSplitSql);
            }
            if (!this.inputConditions.isEmpty()) {
                inputSplitSql = inputSplitSql + " WHERE " + this.inputConditions;
            }
            TeradataPlugInConfiguration.setInputSplitSql(configuration, inputSplitSql);
        }
    }

    @Override
    public List<InputSplit> getSplits(final JobContext context) throws IOException, InterruptedException {
        this.validateConfiguration(context);
        final List<InputSplit> splits = new ArrayList<InputSplit>();
        final int numAmps = TeradataPlugInConfiguration.getInputNumAmps(context.getConfiguration());
        final String[] locations = HadoopConfigurationUtils.getAllActiveHosts(context);
        final int numMappers = ConnectorConfiguration.getNumMappers(context.getConfiguration());
        final String splitSql = TeradataPlugInConfiguration.getInputSplitSql(context.getConfiguration());
        if (numMappers == 1) {
            final int remainder = numAmps % numMappers;
            final int range = numAmps / numMappers;
            int currAmpPos = 0;
            final StringBuilder builder = new StringBuilder();
            final int i = 0;
            builder.setLength(0);
            for (int j = 0; j < range; ++j) {
                builder.append(currAmpPos + j);
                builder.append(",");
            }
            currAmpPos += range;
            if (i < remainder) {
                builder.append(currAmpPos);
                ++currAmpPos;
            } else {
                builder.setLength(builder.length() - 1);
            }
            final String amps = builder.toString();
            final String inputSplitSql = splitSql.replace("TDIN_AMP_ID", amps);
            final TeradataInputSplit split = new TeradataInputSplit(inputSplitSql);
            split.setLocations(HadoopConfigurationUtils.selectUniqueActiveHosts(locations, 6));
            splits.add(split);
        } else if (numAmps == numMappers) {
            for (int k = 0; k < numMappers; ++k) {
                final String inputSplitSql2 = splitSql.replace("TDIN_AMP_ID", String.valueOf(k));
                final TeradataInputSplit split2 = new TeradataInputSplit(inputSplitSql2);
                split2.setLocations(HadoopConfigurationUtils.selectUniqueActiveHosts(locations, 6));
                splits.add(split2);
            }
        } else {
            final int remainder = numAmps % numMappers;
            final int range = numAmps / numMappers;
            int currAmpPos = 0;
            final StringBuilder builder = new StringBuilder();
            for (int i = 0; i < numMappers; ++i) {
                builder.setLength(0);
                for (int j = 0; j < range; ++j) {
                    builder.append(currAmpPos + j);
                    builder.append(",");
                }
                currAmpPos += range;
                if (i < remainder) {
                    builder.append(currAmpPos);
                    ++currAmpPos;
                } else {
                    builder.setLength(builder.length() - 1);
                }
                final String amps = builder.toString();
                final String inputSplitSql = splitSql.replace("TDIN_AMP_ID", amps);
                final TeradataInputSplit split = new TeradataInputSplit(inputSplitSql);
                split.setLocations(HadoopConfigurationUtils.selectUniqueActiveHosts(locations, 6));
                splits.add(split);
            }
        }
        TeradataUtils.closeConnection(this.connection);
        return splits;
    }
}
