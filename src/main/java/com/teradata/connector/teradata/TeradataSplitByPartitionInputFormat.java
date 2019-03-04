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
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.JobContext;


/**
 * @author Administrator
 */
public class TeradataSplitByPartitionInputFormat extends TeradataInputFormat {
    private static Log logger = LogFactory.getLog((Class) TeradataSplitByPartitionInputFormat.class);
    protected static final int PARTITION_RANGE_MIN = 10;
    protected static final String COLUMN_PARTITION = "PARTITION";
    protected static final String SQL_SELECT_NOSTAGE_PARTITION = "SELECT %s FROM %s WHERE %s %s.PARTITION";
    protected static final String SQL_SELECT_NOSTAGE_NOPARTITION = "SELECT %s FROM %s WHERE %s";

    @Override
    public void validateConfiguration(final JobContext context) throws ConnectorException {
        super.validateConfiguration(context);
        final Configuration configuration = context.getConfiguration();
        this.inputTableName = TeradataConnection.getQuotedEscapedName(TeradataPlugInConfiguration.getInputDatabase(configuration), TeradataPlugInConfiguration.getInputTable(configuration));
        this.inputConditions = TeradataPlugInConfiguration.getInputConditions(configuration);
        this.inputFieldNamesArray = TeradataPlugInConfiguration.getInputFieldNamesArray(configuration);
        final int numMappers = ConnectorConfiguration.getNumMappers(configuration);
        if (TeradataPlugInConfiguration.getInputTable(configuration).isEmpty()) {
            if (TeradataPlugInConfiguration.getInputQuery(configuration).isEmpty() || ConnectorConfiguration.getNumMappers(configuration) != 1) {
                throw new ConnectorException(12005);
            }
            this.connection = TeradataUtils.openInputConnection(context);
        } else {
            this.connection = TeradataUtils.openInputConnection(context);
            try {
                if (!this.connection.isTablePPI(this.inputTableName) && numMappers != 1) {
                    throw new ConnectorException(12020);
                }
            } catch (SQLException e) {
                throw new ConnectorException(e.getMessage(), e);
            }
        }
    }

    @Override
    public List<InputSplit> getSplits(final JobContext context) throws IOException, InterruptedException {
        this.validateConfiguration(context);
        final Configuration configuration = context.getConfiguration();
        long numMappers = ConnectorConfiguration.getNumMappers(configuration);
        final boolean accessLock = TeradataPlugInConfiguration.getInputAccessLock(configuration);
        final boolean stagingEnabled = TeradataPlugInConfiguration.getInputStageTableEnabled(configuration);
        final String[] locations = HadoopConfigurationUtils.getAllActiveHosts(context);
        final List<InputSplit> splits = new ArrayList<InputSplit>();
        long numPartitions = 1L;
        if (stagingEnabled || !TeradataPlugInConfiguration.getInputQuery(configuration).isEmpty()) {
            numPartitions = TeradataPlugInConfiguration.getInputNumPartitions(configuration);
        } else if (numMappers > 1L) {
            try {
                numPartitions = this.connection.getTablePartitionCount(this.inputTableName, accessLock);
            } catch (SQLException e1) {
                throw new ConnectorException(e1.getMessage(), e1);
            }
        }
        String splitSql;
        if (this.inputConditions.isEmpty()) {
            splitSql = String.format("SELECT %s FROM %s WHERE %s %s.PARTITION", ConnectorSchemaUtils.concatFieldNamesArray(ConnectorSchemaUtils.quoteFieldNamesArray(this.inputFieldNamesArray)), this.inputTableName, "", this.inputTableName);
        } else {
            splitSql = String.format("SELECT %s FROM %s WHERE %s %s.PARTITION", ConnectorSchemaUtils.concatFieldNamesArray(ConnectorSchemaUtils.quoteFieldNamesArray(this.inputFieldNamesArray)), this.inputTableName, "(" + this.inputConditions + ") AND ", this.inputTableName);
        }
        final long partitionRange = numPartitions / numMappers;
        if (numMappers == 1L || numPartitions == 0L) {
            logger.debug((Object) "SPLIT.BY.PARTITION; NUM MAPPERS = 1 OR NUM PARTITIONS = 0 - NO NEED TO PARTITION");
            if (!this.inputTableName.isEmpty()) {
                splitSql = TeradataConnection.getSelectSQL(this.inputTableName, this.inputFieldNamesArray, this.inputConditions);
            } else {
                splitSql = TeradataPlugInConfiguration.getInputQuery(configuration);
            }
            if (accessLock) {
                splitSql = TeradataConnection.addAccessLockToSql(splitSql);
            }
            final TeradataInputSplit split = new TeradataInputSplit(splitSql);
            split.setLocations(HadoopConfigurationUtils.selectUniqueActiveHosts(locations, 6));
            splits.add(split);
            logger.debug((Object) splitSql);
            TeradataUtils.closeConnection(this.connection);
        } else if (numMappers >= numPartitions) {
            logger.debug((Object) "SPLIT.BY.PARTITION; NUM MAPPERS >= NUM PARTITIONS - SPLIT FOR EACH PARTITION");
            if (numMappers > numPartitions) {
                logger.debug((Object) "SPLIT.BY.PARTITION; NUM MAPPERS > NUM PARTITIONS - SET NUM MAPPERS TO NUM PARTITIONS");
                numMappers = numPartitions;
                ConnectorConfiguration.setNumMappers(configuration, (int) numMappers);
            }
            try {
                final ArrayList<Long> allPartitions = this.connection.getTablePartitions(this.inputTableName, accessLock);
                for (int i = 0; i < numMappers; ++i) {
                    final long partId = stagingEnabled ? (i + 1) : ((long) allPartitions.get(i));
                    String inputSplitSql = splitSql + " = " + partId;
                    if (accessLock) {
                        inputSplitSql = TeradataConnection.addAccessLockToSql(inputSplitSql);
                    }
                    logger.debug((Object) inputSplitSql);
                    final TeradataInputSplit split2 = new TeradataInputSplit(inputSplitSql);
                    split2.setLocations(HadoopConfigurationUtils.selectUniqueActiveHosts(locations, 6));
                    splits.add(split2);
                }
            } catch (SQLException e2) {
                throw new ConnectorException(e2.getMessage(), e2);
            } finally {
                TeradataUtils.closeConnection(this.connection);
            }
        } else if (numPartitions > 2147483647L || partitionRange >= 10L) {
            logger.debug((Object) "SPLIT.BY.PARTITION; LARGE NUM PARTITIONS - CALCULATE OPTIMIZED RANGE");
            try {
                this.connection = TeradataUtils.openInputConnection(context);
                final ArrayList<Long> minMaxPartitions = this.connection.getTablePartitionMinMax(this.inputTableName);
                final long minPartId = stagingEnabled ? 1L : minMaxPartitions.get(0);
                final long maxPartId = stagingEnabled ? numPartitions : minMaxPartitions.get(1);
                long currPartId = minPartId;
                final long estimatedRange = (maxPartId - minPartId) / numMappers;
                final long remainder = (maxPartId - minPartId) % numMappers;
                for (long j = 1L; j <= remainder; ++j) {
                    String inputSplitSql2 = splitSql + " BETWEEN " + currPartId + " AND " + String.valueOf(currPartId + estimatedRange);
                    if (accessLock) {
                        inputSplitSql2 = TeradataConnection.addAccessLockToSql(inputSplitSql2);
                    }
                    logger.debug((Object) inputSplitSql2);
                    final TeradataInputSplit split3 = new TeradataInputSplit(inputSplitSql2);
                    split3.setLocations(HadoopConfigurationUtils.selectUniqueActiveHosts(locations, 6));
                    splits.add(split3);
                    currPartId = currPartId + estimatedRange + 1L;
                }
                for (long j = remainder + 1L; j < numMappers; ++j) {
                    String inputSplitSql2 = splitSql + " BETWEEN " + currPartId + " AND " + String.valueOf(currPartId + estimatedRange - 1L);
                    if (accessLock) {
                        inputSplitSql2 = TeradataConnection.addAccessLockToSql(inputSplitSql2);
                    }
                    logger.debug((Object) inputSplitSql2);
                    final TeradataInputSplit split3 = new TeradataInputSplit(inputSplitSql2);
                    split3.setLocations(HadoopConfigurationUtils.selectUniqueActiveHosts(locations, 6));
                    splits.add(split3);
                    currPartId += estimatedRange;
                }
                String inputSplitSql3 = splitSql + " BETWEEN " + currPartId + " AND " + maxPartId;
                if (accessLock) {
                    inputSplitSql3 = TeradataConnection.addAccessLockToSql(inputSplitSql3);
                }
                logger.debug((Object) inputSplitSql3);
                final TeradataInputSplit split4 = new TeradataInputSplit(inputSplitSql3);
                split4.setLocations(HadoopConfigurationUtils.selectUniqueActiveHosts(locations, 6));
                splits.add(split4);
            } catch (SQLException e2) {
                throw new ConnectorException(e2.getMessage(), e2);
            } finally {
                TeradataUtils.closeConnection(this.connection);
            }
        } else {
            logger.debug((Object) "SPLIT.BY.PARTITION; DEFAULT CASE: NUM MAPPERS < NUM PARTITIONS");
            final int rangeExtras = (int) (numPartitions % numMappers);
            final int range = (int) partitionRange;
            int currPartRangePos = 0;
            try {
                this.connection = TeradataUtils.openInputConnection(context);
                final ArrayList<Long> allPartitions2 = this.connection.getTablePartitions(this.inputTableName, accessLock);
                for (int k = 1; k <= rangeExtras; ++k) {
                    final int beginPos = stagingEnabled ? currPartRangePos : allPartitions2.get(currPartRangePos).intValue();
                    final int endPos = stagingEnabled ? (currPartRangePos + range) : allPartitions2.get(currPartRangePos + range).intValue();
                    String inputSplitSql4 = splitSql + " BETWEEN " + beginPos + " AND " + endPos;
                    if (accessLock) {
                        inputSplitSql4 = TeradataConnection.addAccessLockToSql(inputSplitSql4);
                    }
                    logger.debug((Object) inputSplitSql4);
                    final TeradataInputSplit split5 = new TeradataInputSplit(inputSplitSql4);
                    split5.setLocations(HadoopConfigurationUtils.selectUniqueActiveHosts(locations, 6));
                    splits.add(split5);
                    currPartRangePos = currPartRangePos + range + 1;
                }
                for (int k = rangeExtras + 1; k < numMappers; ++k) {
                    final int beginPos = stagingEnabled ? currPartRangePos : allPartitions2.get(currPartRangePos).intValue();
                    final int endPos = stagingEnabled ? (currPartRangePos + range - 1) : allPartitions2.get(currPartRangePos + range - 1).intValue();
                    String inputSplitSql4 = splitSql + " BETWEEN " + beginPos + " AND " + endPos;
                    if (accessLock) {
                        inputSplitSql4 = TeradataConnection.addAccessLockToSql(inputSplitSql4);
                    }
                    logger.debug((Object) inputSplitSql4);
                    final TeradataInputSplit split5 = new TeradataInputSplit(inputSplitSql4);
                    split5.setLocations(HadoopConfigurationUtils.selectUniqueActiveHosts(locations, 6));
                    splits.add(split5);
                    currPartRangePos += range;
                }
                if (currPartRangePos < (stagingEnabled ? numPartitions : allPartitions2.size())) {
                    final int beginPos2 = stagingEnabled ? currPartRangePos : allPartitions2.get(currPartRangePos).intValue();
                    final int endPos2 = stagingEnabled ? ((int) numPartitions) : allPartitions2.get(allPartitions2.size() - 1).intValue();
                    String inputSplitSql5 = splitSql + " BETWEEN " + beginPos2 + " AND " + endPos2;
                    if (accessLock) {
                        inputSplitSql5 = TeradataConnection.addAccessLockToSql(inputSplitSql5);
                    }
                    logger.debug((Object) inputSplitSql5);
                    final TeradataInputSplit split6 = new TeradataInputSplit(inputSplitSql5);
                    split6.setLocations(HadoopConfigurationUtils.selectUniqueActiveHosts(locations, 6));
                    splits.add(split6);
                } else {
                    String inputSplitSql = splitSql + " = " + currPartRangePos;
                    if (accessLock) {
                        inputSplitSql = TeradataConnection.addAccessLockToSql(inputSplitSql);
                    }
                    logger.debug((Object) inputSplitSql);
                    final TeradataInputSplit split2 = new TeradataInputSplit(inputSplitSql);
                    split2.setLocations(HadoopConfigurationUtils.selectUniqueActiveHosts(locations, 6));
                    splits.add(split2);
                }
            } catch (SQLException e3) {
                throw new ConnectorException(e3.getMessage(), e3);
            } finally {
                TeradataUtils.closeConnection(this.connection);
            }
        }
        return splits;
    }
}
