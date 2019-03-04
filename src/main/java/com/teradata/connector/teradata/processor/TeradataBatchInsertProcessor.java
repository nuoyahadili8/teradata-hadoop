package com.teradata.connector.teradata.processor;

import com.teradata.connector.common.exception.ConnectorException;
import com.teradata.connector.common.exception.ConnectorException.ErrorCode;
import com.teradata.connector.common.utils.ConnectorConfiguration;
import com.teradata.connector.teradata.db.TeradataConnection;
import com.teradata.connector.teradata.schema.TeradataColumnDesc;
import com.teradata.connector.teradata.schema.TeradataTableDesc;
import com.teradata.connector.teradata.utils.TeradataPlugInConfiguration;
import com.teradata.connector.teradata.utils.TeradataSchemaUtils;

import java.sql.SQLException;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;


public class TeradataBatchInsertProcessor extends TeradataOutputProcessor {
    public static String taskIDColumnName = "TDCH_BI_TASKID";
    private static Log logger = LogFactory.getLog((Class) TeradataBatchInsertProcessor.class);

    @Override
    public void setupDatabaseEnvironment(final Configuration configuration) throws ConnectorException {
        final String outputDatabase = TeradataPlugInConfiguration.getOutputDatabase(configuration);
        final String objectName;
        String outputTableName = objectName = TeradataPlugInConfiguration.getOutputTable(configuration);
        outputTableName = TeradataConnection.getQuotedEscapedName(outputDatabase, outputTableName);
        final String stageDatabase = TeradataPlugInConfiguration.getOutputStageDatabase(configuration);
        final String stageDatabaseForTable = TeradataPlugInConfiguration.getOutputStageDatabaseForTable(configuration);
        final String stageDatabaseForView = TeradataPlugInConfiguration.getOutputStageDatabaseForView(configuration);
        String stageTableName = TeradataPlugInConfiguration.getOutputStageTableName(configuration);
        boolean stagingEnabled = true;
        if (!TeradataPlugInConfiguration.getOutputStageTableForced(configuration)) {
            try {
                if (this.connection.isTableNoPrimaryIndex(outputTableName)) {
                    stagingEnabled = false;
                }
            } catch (SQLException e) {
                throw new ConnectorException(e.getMessage(), e);
            }
        }
        TeradataPlugInConfiguration.setOutputStageEnabled(configuration, stagingEnabled);
        final int maxLength = this.connection.getMaxTableNameLength();
        if (stagingEnabled) {
            if (stageTableName.isEmpty()) {
                stageTableName = TeradataSchemaUtils.getStageTableName(maxLength, objectName, "TDCBISTAGE");
            } else if (stageTableName.length() > maxLength) {
                throw new ConnectorException(13014, new Object[]{maxLength});
            }
            if (!stageDatabase.isEmpty() && (!stageDatabaseForTable.isEmpty() || !stageDatabaseForView.isEmpty())) {
                throw new ConnectorException(15051);
            }
            if ((!stageDatabaseForTable.isEmpty() && stageDatabaseForView.isEmpty()) || (stageDatabaseForTable.isEmpty() && !stageDatabaseForView.isEmpty())) {
                throw new ConnectorException(15052);
            }
            TeradataPlugInConfiguration.setOutputStageAreas(configuration, stageTableName);
            TeradataPlugInConfiguration.setOutputFinalTable(configuration, objectName);
            TeradataPlugInConfiguration.setOutputFinalDatabase(configuration, outputDatabase);
            TeradataPlugInConfiguration.setOutputTable(configuration, stageTableName);
            if (!stageDatabaseForTable.isEmpty()) {
                TeradataPlugInConfiguration.setOutputDatabase(configuration, stageDatabaseForTable);
            } else {
                TeradataPlugInConfiguration.setOutputDatabase(configuration, stageDatabase);
            }
            final TeradataColumnDesc[] fieldDescs = TeradataSchemaUtils.tableDescFromText(TeradataPlugInConfiguration.getOutputTableDesc(configuration)).getColumns();
            final TeradataTableDesc stageTableDesc = new TeradataTableDesc();
            stageTableDesc.setName(stageTableName);
            if (!stageDatabaseForTable.isEmpty()) {
                stageTableDesc.setDatabaseName(stageDatabaseForTable);
            } else {
                stageTableDesc.setDatabaseName(stageDatabase);
            }
            stageTableDesc.setColumns(fieldDescs);
            stageTableDesc.setHasPrimaryIndex(false);
            stageTableDesc.setHasPartitionColumns(false);
            stageTableDesc.setBlockSize(TeradataPlugInConfiguration.getOutputStageTableBlocksize(configuration));
            TeradataPlugInConfiguration.setOutputTableDesc(configuration, TeradataSchemaUtils.tableDescToJson(stageTableDesc));
            if (!TeradataPlugInConfiguration.getOutputBIDisableFailoverSupport(configuration)) {
                final TeradataColumnDesc[] fieldDescsWithTaskID = TeradataSchemaUtils.addTaskIDColumn(fieldDescs, TeradataBatchInsertProcessor.taskIDColumnName);
                stageTableDesc.setColumns(fieldDescsWithTaskID);
            }
            final String createStageSQL = TeradataConnection.getCreateTableSQL(stageTableDesc);
            try {
                this.connection.executeDDL(createStageSQL);
                TeradataBatchInsertProcessor.logger.info((Object) ("create output stage table " + TeradataPlugInConfiguration.getOutputStageAreas(configuration) + "the sql is " + createStageSQL));
            } catch (SQLException e2) {
                throw new ConnectorException(e2.getMessage(), e2);
            }
        } else {
            TeradataBatchInsertProcessor.logger.info((Object) "output staging table is not needed");
        }
    }

    @Override
    public void cleanupDatabaseEnvironment(final Configuration configuration) throws ConnectorException {
        if (TeradataPlugInConfiguration.getOutputStageEnabled(configuration)) {
            final String outputDatabase = TeradataPlugInConfiguration.getOutputFinalDatabase(configuration);
            String outputTableName = TeradataPlugInConfiguration.getOutputFinalTable(configuration);
            final String stageDatabase = TeradataPlugInConfiguration.getOutputStageDatabase(configuration);
            final String stageDatabaseForTable = TeradataPlugInConfiguration.getOutputStageDatabaseForTable(configuration);
            String stageTableName = TeradataPlugInConfiguration.getOutputStageAreas(configuration);
            final String[] outputTableFieldNames = TeradataPlugInConfiguration.getOutputFieldNamesArray(configuration);
            final boolean jobSucceeded = ConnectorConfiguration.getJobSucceeded(configuration);
            outputTableName = TeradataConnection.getQuotedEscapedName(outputDatabase, outputTableName);
            if (!stageDatabaseForTable.isEmpty()) {
                stageTableName = TeradataConnection.getQuotedEscapedName(stageDatabaseForTable, stageTableName);
            } else {
                stageTableName = TeradataConnection.getQuotedEscapedName(stageDatabase, stageTableName);
            }
            boolean stageTableInsertSelected = false;
            boolean stageTableDeleted = false;
            Exception firstException = null;
            final boolean stageKept = TeradataPlugInConfiguration.getOutputStageTableKept(configuration);
            if (jobSucceeded) {
                try {
                    TeradataBatchInsertProcessor.logger.info((Object) "insert from staget table to target table ");
                    final long startTime = System.currentTimeMillis();
                    TeradataBatchInsertProcessor.logger.info((Object) ("the insert select sql starts at: " + startTime));
                    this.connection.executeInsertSelect(stageTableName, outputTableFieldNames, outputTableName, outputTableFieldNames);
                    final long endTime = System.currentTimeMillis();
                    TeradataBatchInsertProcessor.logger.info((Object) ("the insert select sql ends at: " + endTime));
                    TeradataBatchInsertProcessor.logger.info((Object) ("the total elapsed time of the insert select sql  is: " + (endTime - startTime) / 1000L + "s"));
                    stageTableInsertSelected = true;
                } catch (SQLException e) {
                    firstException = e;
                    TeradataBatchInsertProcessor.logger.debug((Object) e.getMessage());
                }
                if (stageTableInsertSelected) {
                    try {
                        this.connection.dropTable(stageTableName);
                        TeradataBatchInsertProcessor.logger.info((Object) ("drop stage table " + stageTableName));
                        stageTableDeleted = true;
                    } catch (SQLException e) {
                        firstException = e;
                        TeradataBatchInsertProcessor.logger.debug((Object) e.getMessage());
                    }
                } else if (!stageKept) {
                    try {
                        this.connection.dropTable(stageTableName);
                        TeradataBatchInsertProcessor.logger.info((Object) ("drop stage table " + stageTableName));
                        stageTableDeleted = true;
                    } catch (SQLException e) {
                        TeradataBatchInsertProcessor.logger.debug((Object) e.getMessage());
                    }
                }
            } else if (!stageKept) {
                try {
                    this.connection.dropTable(stageTableName);
                    TeradataBatchInsertProcessor.logger.info((Object) ("drop stage table " + stageTableName));
                    stageTableDeleted = true;
                } catch (SQLException e) {
                    TeradataBatchInsertProcessor.logger.debug((Object) e.getMessage());
                }
            }
            if (!stageTableInsertSelected && stageKept) {
                TeradataBatchInsertProcessor.logger.warn((Object) ("unable to insert data from staging table into the target table. Please manually move data from " + stageTableName + " into the target table"));
            }
            if (!stageTableDeleted) {
                TeradataBatchInsertProcessor.logger.warn((Object) ("staging table " + stageTableName + " is not dropped"));
            }
            if (firstException != null) {
                throw new ConnectorException(firstException.getMessage(), firstException);
            }
        }
    }

    @Override
    protected void validateConfiguration(final Configuration configuration, final TeradataConnection connection) throws ConnectorException {
        super.validateConfiguration(configuration, connection);
        if (TeradataPlugInConfiguration.getOutputBatchSize(configuration) > 13683) {
            TeradataPlugInConfiguration.setOutputBatchSize(configuration, 13683);
        }
    }
}
