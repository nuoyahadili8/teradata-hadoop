package com.teradata.connector.teradata.processor;

import com.teradata.connector.common.exception.ConnectorException;
import com.teradata.connector.common.utils.ConnectorConfiguration;
import com.teradata.connector.teradata.db.TeradataConnection;
import com.teradata.connector.teradata.schema.TeradataColumnDesc;
import com.teradata.connector.teradata.schema.TeradataTableDesc;
import com.teradata.connector.teradata.utils.TeradataPlugInConfiguration;
import com.teradata.connector.teradata.utils.TeradataSchemaUtils;
import com.teradata.jdbc.jdbc_4.TDSession;
import java.sql.SQLException;
import java.text.SimpleDateFormat;
import java.util.Date;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;


/**
 * @author Administrator
 */
public class TeradataInternalFastloadProcessor extends TeradataOutputProcessor {
    private static Log logger = LogFactory.getLog((Class) TeradataInternalFastloadProcessor.class);
    public static boolean jobSuccess = true;
    private boolean setupFinished= false;

    @Override
    protected void setupDatabaseEnvironment(final Configuration configuration) throws ConnectorException {
        final String outputDatabase = TeradataPlugInConfiguration.getOutputDatabase(configuration);
        String outputTableName = TeradataPlugInConfiguration.getOutputTable(configuration);
        final String errorTableName = TeradataPlugInConfiguration.getOutputErrorTableName(configuration);
        final String objectName = outputTableName;
        outputTableName = TeradataConnection.getQuotedEscapedName(outputDatabase, outputTableName);
        try {
            TeradataPlugInConfiguration.setOutputFastFail(configuration, ((TDSession) this.connection.getConnection()).getConfigResponse().isFailFastSupported());
        } catch (SQLException e1) {
            throw new ConnectorException(e1.getMessage(), e1);
        }
        boolean stagingEnabled = true;
        if (!TeradataPlugInConfiguration.getOutputStageTableForced(configuration)) {
            try {
                if (this.connection.isTableFastloadable(outputTableName)) {
                    stagingEnabled = false;
                }
            } catch (SQLException e2) {
                throw new ConnectorException(e2.getMessage(), e2);
            }
        }
        TeradataPlugInConfiguration.setOutputStageEnabled(configuration, stagingEnabled);
        if (stagingEnabled) {
            String errorTable1 = "";
            String errorTable2 = "";
            final int maxLength = this.connection.getMaxTableNameLength();
            if (!errorTableName.isEmpty()) {
                final int errorTable1Len = errorTableName.length() + "_ERR_1".length();
                final int errorTable2Len = errorTableName.length() + "_ERR_2".length();
                if (errorTable1Len > maxLength || errorTable2Len > maxLength) {
                    throw new ConnectorException(13018, new Object[]{maxLength});
                }
                errorTable1 = errorTableName + "_ERR_1";
                errorTable2 = errorTableName + "_ERR_2";
            }
            final String stageDatabase = TeradataPlugInConfiguration.getOutputStageDatabase(configuration);
            final String stageDatabaseForTable = TeradataPlugInConfiguration.getOutputStageDatabaseForTable(configuration);
            final String stageDatabaseForView = TeradataPlugInConfiguration.getOutputStageDatabaseForView(configuration);
            String stageTableName = TeradataPlugInConfiguration.getOutputStageTableName(configuration);
            if (!stageDatabase.isEmpty() && (!stageDatabaseForTable.isEmpty() || !stageDatabaseForView.isEmpty())) {
                throw new ConnectorException(15051);
            }
            if ((!stageDatabaseForTable.isEmpty() && stageDatabaseForView.isEmpty()) || (stageDatabaseForTable.isEmpty() && !stageDatabaseForView.isEmpty())) {
                throw new ConnectorException(15052);
            }
            if (stageTableName.isEmpty()) {
                stageTableName = TeradataSchemaUtils.getStageTableName(maxLength, objectName, "TDCIFSTAGE");
                if (errorTableName.isEmpty()) {
                    errorTable1 = this.getErrorTable1Name(maxLength, objectName, stageTableName);
                    errorTable2 = this.getErrorTable2Name(maxLength, objectName, stageTableName);
                }
            } else {
                if (stageTableName.length() > maxLength) {
                    throw new ConnectorException(13014, new Object[]{maxLength});
                }
                if (errorTableName.isEmpty()) {
                    errorTable1 = this.getErrorTable1Name(maxLength, stageTableName, objectName);
                    errorTable2 = this.getErrorTable2Name(maxLength, stageTableName, objectName);
                }
            }
            TeradataPlugInConfiguration.setOutputStageAreas(configuration, stageTableName);
            final TeradataTableDesc targetTableDesc = TeradataSchemaUtils.tableDescFromText(TeradataPlugInConfiguration.getOutputTableDesc(configuration));
            final TeradataColumnDesc[] fieldDescs = targetTableDesc.getColumns();
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
            final String createStageSQL = TeradataConnection.getCreateTableSQL(stageTableDesc);
            try {
                logger.info((Object) createStageSQL);
                this.connection.executeDDL(createStageSQL);
                logger.info((Object) ("output stage table " + TeradataPlugInConfiguration.getOutputStageAreas(configuration) + " is created"));
            } catch (SQLException e3) {
                throw new ConnectorException(e3.getMessage(), e3);
            }
            TeradataPlugInConfiguration.setOutputFinalTable(configuration, objectName);
            TeradataPlugInConfiguration.setOutputFinalDatabase(configuration, outputDatabase);
            TeradataPlugInConfiguration.setOutputTable(configuration, stageTableName);
            if (!stageDatabaseForTable.isEmpty()) {
                TeradataPlugInConfiguration.setOutputDatabase(configuration, stageDatabaseForTable);
            } else {
                TeradataPlugInConfiguration.setOutputDatabase(configuration, stageDatabase);
            }
            TeradataPlugInConfiguration.setOutputErrorTable1Name(configuration, errorTable1);
            TeradataPlugInConfiguration.setOutputErrorTable2Name(configuration, errorTable2);
        } else {
            logger.info((Object) "output staging table is not needed");
            String errorTable1 = "";
            String errorTable2 = "";
            final int maxLength = this.connection.getMaxTableNameLength();
            if (!errorTableName.isEmpty()) {
                final int errorTable1Len = errorTableName.length() + "_ERR_1".length();
                final int errorTable2Len = errorTableName.length() + "_ERR_2".length();
                if (errorTable1Len > maxLength || errorTable2Len > maxLength) {
                    throw new ConnectorException(13018);
                }
                errorTable1 = errorTableName + "_ERR_1";
                errorTable2 = errorTableName + "_ERR_2";
            } else {
                errorTable1 = this.getErrorTable1Name(maxLength, objectName, "TDCIFERROR");
                errorTable2 = this.getErrorTable2Name(maxLength, objectName, "TDCIFERROR");
            }
            TeradataPlugInConfiguration.setOutputErrorTable1Name(configuration, errorTable1);
            TeradataPlugInConfiguration.setOutputErrorTable2Name(configuration, errorTable2);
        }
        this.setupFinished = true;
    }

    @Override
    protected void cleanupDatabaseEnvironment(final Configuration configuration) throws ConnectorException {
        ConnectorException insertException = null;
        if (!this.setupFinished) {
            return;
        }
        final String outputDatabase = TeradataPlugInConfiguration.getOutputFinalDatabase(configuration);
        String outputTableName = TeradataPlugInConfiguration.getOutputFinalTable(configuration);
        final String[] outputTableFieldNames = TeradataPlugInConfiguration.getOutputFieldNamesArray(configuration);
        final String stageDatabase = TeradataPlugInConfiguration.getOutputStageDatabase(configuration);
        final String stageDatabaseForTable = TeradataPlugInConfiguration.getOutputStageDatabaseForTable(configuration);
        String stageTableName = TeradataPlugInConfiguration.getOutputStageAreas(configuration);
        String errorTableDatabase = TeradataPlugInConfiguration.getOutputErrorTableDatabase(configuration);
        final boolean jobSucceed = ConnectorConfiguration.getJobSucceeded(configuration);
        String errorTable1 = "";
        String errorTable2 = "";
        final long errorLimit = TeradataPlugInConfiguration.getOutputFastloadErrorLimit(configuration);
        final Boolean stageKept = TeradataPlugInConfiguration.getOutputStageTableKept(configuration);
        if (!TeradataPlugInConfiguration.getOutputStageEnabled(configuration)) {
            if (errorTableDatabase.isEmpty()) {
                errorTableDatabase = outputDatabase;
            }
            errorTable1 = TeradataConnection.getQuotedEscapedName(errorTableDatabase, TeradataPlugInConfiguration.getOutputErrorTable1Name(configuration));
            errorTable2 = TeradataConnection.getQuotedEscapedName(errorTableDatabase, TeradataPlugInConfiguration.getOutputErrorTable2Name(configuration));
            outputTableName = TeradataConnection.getQuotedEscapedName(outputDatabase, outputTableName);
            try {
                if (errorLimit > 0L && this.connection.getTableRowCount(errorTable1, "") >= errorLimit) {
                    throw new ConnectorException(22013);
                }
            } catch (SQLException e) {
                logger.error((Object) e.getMessage());
            }
        } else {
            if (errorTableDatabase.isEmpty()) {
                if (!stageDatabaseForTable.isEmpty()) {
                    errorTableDatabase = stageDatabaseForTable;
                } else {
                    errorTableDatabase = stageDatabase;
                }
            }
            errorTable1 = TeradataConnection.getQuotedEscapedName(errorTableDatabase, TeradataPlugInConfiguration.getOutputErrorTable1Name(configuration));
            errorTable2 = TeradataConnection.getQuotedEscapedName(errorTableDatabase, TeradataPlugInConfiguration.getOutputErrorTable2Name(configuration));
            outputTableName = TeradataConnection.getQuotedEscapedName(outputDatabase, outputTableName);
            if (!stageDatabaseForTable.isEmpty()) {
                stageTableName = TeradataConnection.getQuotedEscapedName(stageDatabaseForTable, stageTableName);
            } else {
                stageTableName = TeradataConnection.getQuotedEscapedName(stageDatabase, stageTableName);
            }
            try {
                if (errorLimit > 0L && this.connection.getTableRowCount(errorTable1, "") >= errorLimit) {
                    throw new ConnectorException(22013);
                }
            } catch (SQLException e) {
                logger.error((Object) e.getMessage());
            }
            if (jobSucceed) {
                try {
                    logger.info((Object) "insert from staget table to target table ");
                    final long startTime = System.currentTimeMillis();
                    logger.info((Object) ("the insert select sql starts at: " + startTime));
                    this.connection.executeInsertSelect(stageTableName, outputTableFieldNames, outputTableName, outputTableFieldNames);
                    final long endTime = System.currentTimeMillis();
                    logger.info((Object) ("the insert select sql ends at: " + endTime));
                    logger.info((Object) ("the total elapsed time of the insert select sql  is: " + (endTime - startTime) / 1000L + "s"));
                    this.connection.commit();
                    try {
                        this.connection.dropTable(stageTableName);
                        logger.info((Object) ("staging table " + stageTableName + " was dropped"));
                    } catch (SQLException e2) {
                        logger.error((Object) ("staging table " + stageTableName + " was not dropped"), (Throwable) e2);
                    }
                } catch (SQLException e) {
                    insertException = new ConnectorException(22016);
                    logger.error((Object) ("unable to insert data in the staging table into the target table. Please manually move data in " + stageTableName + " into the target table"), (Throwable) e);
                    if (!stageKept) {
                        try {
                            this.connection.dropTable(stageTableName);
                            logger.info((Object) ("staging table " + stageTableName + " was dropped"));
                        } catch (SQLException e4) {
                            logger.error((Object) ("staging table " + stageTableName + " was not dropped"), (Throwable) e);
                        }
                    }
                }
            } else if (!stageKept) {
                try {
                    this.connection.dropTable(stageTableName);
                    logger.info((Object) ("staging table " + stageTableName + " was dropped"));
                } catch (SQLException e3) {
                    logger.error((Object) ("staging table " + stageTableName + " was not dropped"), (Throwable) e3);
                }
            }
        }
        try {
            if (this.connection.isTableNonEmpty(errorTable1)) {
                logger.warn((Object) ("error table " + errorTable1 + " is not empty"), new Throwable());
            } else {
                try {
                    this.connection.dropTable(errorTable1);
                    logger.info((Object) ("error table " + errorTable1 + " was dropped"));
                } catch (SQLException e) {
                    logger.error((Object) ("error table " + errorTable1 + " was not dropped"), (Throwable) e);
                }
            }
        } catch (SQLException e) {
            logger.error((Object) e.getMessage());
            if (e.getErrorCode() != 3807) {
                try {
                    this.connection.dropTable(errorTable1);
                    logger.info((Object) ("error table " + errorTable1 + " was dropped"));
                } catch (SQLException e4) {
                    logger.error((Object) ("error table " + errorTable1 + " was not dropped"), (Throwable) e4);
                }
            }
        }
        try {
            if (this.connection.isTableNonEmpty(errorTable2)) {
                logger.warn((Object) ("error table " + errorTable2 + " is not empty"), new Throwable());
            } else {
                try {
                    this.connection.dropTable(errorTable2);
                    logger.info((Object) ("error table " + errorTable2 + " was dropped"));
                } catch (SQLException e) {
                    logger.error((Object) ("error table " + errorTable2 + " was not dropped"), (Throwable) e);
                }
            }
        } catch (SQLException e) {
            logger.error((Object) e.getMessage());
            if (e.getErrorCode() != 3807) {
                try {
                    this.connection.dropTable(errorTable2);
                    logger.info((Object) ("error table " + errorTable2 + " was dropped"));
                } catch (SQLException e4) {
                    logger.error((Object) ("error table " + errorTable2 + " was not dropped"), (Throwable) e4);
                }
            }
        }
        if (insertException != null) {
            throw insertException;
        }
    }

    @Override
    protected void validateConfiguration(final Configuration configuration, final TeradataConnection connection) throws ConnectorException {
        super.validateConfiguration(configuration, connection);
        int numMappers = 0;
        int numAmps = 0;
        try {
            numAmps = connection.getAMPCount();
        } catch (SQLException e) {
            throw new ConnectorException(e.getMessage(), e);
        }
        numMappers = ConnectorConfiguration.getNumMappers(configuration);
        if (numMappers == 0 || numMappers > numAmps) {
            numMappers = numAmps;
            ConnectorConfiguration.setNumMappers(configuration, numMappers);
        }
    }

    private String getErrorTable1Name(final int maxLength, final String tableName, final String bkupTableName) {
        final int tableNameLen = (tableName == null) ? 0 : tableName.length();
        final int bkupTableNameLen = (bkupTableName == null) ? 0 : bkupTableName.length();
        final String etExtension = "_ERR_1";
        final int etExtensionLen = etExtension.length();
        String errorTableName;
        if (tableNameLen != 0 && tableNameLen + etExtensionLen <= maxLength) {
            errorTableName = tableName + etExtension;
        } else if (bkupTableNameLen != 0 && bkupTableNameLen + etExtensionLen <= maxLength) {
            errorTableName = bkupTableName + etExtension;
        } else {
            final SimpleDateFormat sdf = new SimpleDateFormat("hhmmssSSS");
            final String identifier = sdf.format(new Date());
            errorTableName = "TDCH_ERROR_" + identifier + etExtension;
            errorTableName = errorTableName.substring(0, maxLength);
        }
        return errorTableName;
    }

    private String getErrorTable2Name(final int maxLength, final String tableName, final String bkupTableName) {
        final int tableNameLen = (tableName == null) ? 0 : tableName.length();
        final int bkupTableNameLen = (bkupTableName == null) ? 0 : bkupTableName.length();
        final String etExtension = "_ERR_2";
        final int etExtensionLen = etExtension.length();
        String errorTableName;
        if (tableNameLen != 0 && tableNameLen + etExtensionLen <= maxLength) {
            errorTableName = tableName + etExtension;
        } else if (bkupTableNameLen != 0 && bkupTableNameLen + etExtensionLen <= maxLength) {
            errorTableName = bkupTableName + etExtension;
        } else {
            final SimpleDateFormat sdf = new SimpleDateFormat("hhmmssSSS");
            final String identifier = sdf.format(new Date());
            errorTableName = "TDCH_ERROR_" + identifier + etExtension;
            errorTableName = errorTableName.substring(0, maxLength);
        }
        return errorTableName;
    }
}
