package com.teradata.connector.teradata.processor;

import com.teradata.connector.common.exception.ConnectorException;
import com.teradata.connector.common.exception.ConnectorException.ErrorCode;
import com.teradata.connector.common.utils.ConnectorConfiguration;
import com.teradata.connector.teradata.db.TeradataConnection;
import com.teradata.connector.teradata.schema.TeradataColumnDesc;
import com.teradata.connector.teradata.schema.TeradataTableDesc;
import com.teradata.connector.teradata.schema.TeradataViewDesc;
import com.teradata.connector.teradata.utils.TeradataPlugInConfiguration;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

import com.teradata.connector.teradata.utils.TeradataSchemaUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;


public class TeradataSplitByPartitionProcessor extends TeradataInputProcessor {
    private static Log logger = LogFactory.getLog((Class) TeradataSplitByPartitionProcessor.class);
    protected static final int VALUE_PARTITION_ID_TYPE = 4;
    protected static final String COLUMN_PARTITION_ID = "TDIN_PARTID";
    protected static final String COLUMN_PARTITION = "PARTITION";
    protected static final int PARTITION_RANGE_MIN = 10;

    @Override
    protected void setupDatabaseEnvironment(final Configuration configuration) throws ConnectorException {
        final String inputDatabase = TeradataPlugInConfiguration.getInputDatabase(configuration);
        String inputTableName = TeradataPlugInConfiguration.getInputTable(configuration);
        final String inputQuery = TeradataPlugInConfiguration.getInputQuery(configuration);
        final String inputCondition = TeradataPlugInConfiguration.getInputConditions(configuration);
        final String[] inputFieldNameArray = TeradataPlugInConfiguration.getInputFieldNamesArray(configuration);
        final String targetPartitionColName = ConnectorConfiguration.getOutputPartitionColumnNames(configuration);
        final String[] targetPartitionColNames = targetPartitionColName.isEmpty() ? new String[0] : targetPartitionColName.split(",");
        final List<String> inputFieldNameList = new ArrayList<String>();
        for (final String s : inputFieldNameArray) {
            inputFieldNameList.add(s);
        }
        final int[] posPartitionCols = TeradataSchemaUtils.getColumnMapping(inputFieldNameList, targetPartitionColNames);
        final int[] posFieldCols = TeradataSchemaUtils.getColumnMapping(inputFieldNameList, inputFieldNameArray);
        final boolean accessLock = TeradataPlugInConfiguration.getInputAccessLock(configuration);
        final String objectName = inputTableName;
        inputTableName = TeradataConnection.getQuotedEscapedName(inputDatabase, inputTableName);
        final long numPartitions = TeradataPlugInConfiguration.getInputNumPartitions(configuration);
        int numMappers = 1;
        if (numPartitions == 1L) {
            ConnectorConfiguration.setNumMappers(configuration, numMappers);
        } else {
            numMappers = ConnectorConfiguration.getNumMappers(configuration);
        }
        final boolean stagingEnabled = this.stagingTableEnabeled(configuration, inputTableName, inputQuery, numMappers);
        if (stagingEnabled) {
            final String stageDatabase = TeradataPlugInConfiguration.getInputStageDatabase(configuration);
            final String stageDatabaseForTable = TeradataPlugInConfiguration.getInputStageDatabaseForTable(configuration);
            final String stageDatabaseForView = TeradataPlugInConfiguration.getInputStageDatabaseForView(configuration);
            String stageTableName = TeradataPlugInConfiguration.getInputStageTableName(configuration);
            final int maxLength = this.connection.getMaxTableNameLength();
            if (numPartitions < numMappers) {
                numMappers = (int) numPartitions;
                ConnectorConfiguration.setNumMappers(configuration, numMappers);
            }
            if (!stageDatabase.isEmpty() && (!stageDatabaseForTable.isEmpty() || !stageDatabaseForView.isEmpty())) {
                throw new ConnectorException(15051);
            }
            if ((!stageDatabaseForTable.isEmpty() && stageDatabaseForView.isEmpty()) || (stageDatabaseForTable.isEmpty() && !stageDatabaseForView.isEmpty())) {
                throw new ConnectorException(15052);
            }
            if (stageTableName.isEmpty()) {
                stageTableName = TeradataSchemaUtils.getStageTableName(maxLength, objectName, "TDCSPSTAGE");
            } else if (stageTableName.length() > maxLength) {
                throw new ConnectorException(12013, new Object[]{maxLength});
            }
            try {
                TeradataColumnDesc[] fieldDescs;
                if (objectName != null && !objectName.isEmpty()) {
                    fieldDescs = TeradataSchemaUtils.tableDescFromText(TeradataPlugInConfiguration.getInputTableDesc(configuration)).getColumns();
                } else {
                    try {
                        final String charset = TeradataConnection.getURLParamValue(TeradataPlugInConfiguration.getInputJdbcUrl(configuration), "CHARSET");
                        fieldDescs = this.connection.getColumnDescsForSQLWithCharSet(inputQuery, charset);
                    } catch (SQLException e) {
                        throw new ConnectorException(e.getMessage(), e);
                    }
                }
                final TeradataTableDesc sourceTableDesc = new TeradataTableDesc();
                sourceTableDesc.setColumns(fieldDescs);
                final int columnDescsSize = fieldDescs.length;
                final TeradataColumnDesc[] columnDescs = fieldDescs;
                for (int i = 0; i < columnDescsSize; ++i) {
                    columnDescs[i].setName("c" + (i + 1));
                    columnDescs[i].setNullable(true);
                }
                final TeradataColumnDesc cpDesc = new TeradataColumnDesc();
                cpDesc.setName("TDIN_PARTID");
                cpDesc.setType(4);
                final TeradataTableDesc tableDesc = new TeradataTableDesc();
                tableDesc.setBlockSize(TeradataPlugInConfiguration.getInputStageTableBlocksize(configuration));
                tableDesc.setName(stageTableName);
                if (!stageDatabaseForTable.isEmpty()) {
                    tableDesc.setDatabaseName(stageDatabaseForTable);
                } else {
                    tableDesc.setDatabaseName(stageDatabase);
                }
                tableDesc.setColumns(columnDescs);
                if (posFieldCols.length > 0) {
                    final String[] fieldname = new String[posFieldCols.length];
                    int j = 0;
                    for (final int pos : posFieldCols) {
                        fieldname[j] = tableDesc.getColumn(pos).getName();
                        ++j;
                    }
                    TeradataPlugInConfiguration.setInputFieldNamesArray(configuration, fieldname);
                } else {
                    TeradataPlugInConfiguration.setInputFieldNamesArray(configuration, tableDesc.getColumnNames());
                }
                String insertStageTableSQL = null;
                tableDesc.addColumn(cpDesc);
                tableDesc.addPrimaryIndex("TDIN_PARTID");
                tableDesc.addPartitionColumn("TDIN_PARTID");
                TeradataPlugInConfiguration.setInputTableDesc(configuration, TeradataSchemaUtils.tableDescToJson(tableDesc));
                String selectExpression = TeradataConnection.getQuotedColumnNames(sourceTableDesc.getColumnsString()) + ", ";
                if (targetPartitionColNames.length == 0) {
                    selectExpression = selectExpression + "RANDOM(1, " + numPartitions + ")";
                } else {
                    selectExpression += "(";
                    for (int k = 0; k < posPartitionCols.length; ++k) {
                        if (k > 0) {
                            selectExpression += " + ";
                        }
                        selectExpression = selectExpression + "HASHBUCKET(HASHROW(" + TeradataConnection.getQuotedName(columnDescs[posPartitionCols[k]].getName()) + "))";
                    }
                    selectExpression = selectExpression + ") MOD " + numPartitions + " + 1";
                }
                final String stageView = "V" + stageTableName;
                final TeradataViewDesc viewDesc = new TeradataViewDesc();
                viewDesc.setName(stageView);
                if (!stageDatabaseForView.isEmpty()) {
                    viewDesc.setDatabaseName(stageDatabaseForView);
                } else {
                    viewDesc.setDatabaseName(stageDatabase);
                }
                if (inputQuery.isEmpty()) {
                    viewDesc.setQuery(TeradataConnection.getSelectSQL(inputTableName, inputFieldNameArray, inputCondition));
                } else {
                    viewDesc.setQuery(inputQuery);
                }
                if (accessLock) {
                    viewDesc.setAccessLock(true);
                }
                viewDesc.setColumns(columnDescs);
                insertStageTableSQL = TeradataConnection.getInsertSelectSQL(tableDesc.getQualifiedName(), tableDesc.getColumnsString(), viewDesc.getQualifiedName(), selectExpression);
                this.connection.createView(viewDesc);
                logger.info((Object) ("create stage view " + viewDesc.getQualifiedName()));
                this.connection.createTable(tableDesc);
                logger.info((Object) ("create stage table " + tableDesc.getQualifiedName()));
                logger.info((Object) ("insert from source table to staget table, the insert select sql " + insertStageTableSQL));
                final long startTime = System.currentTimeMillis();
                logger.info((Object) ("the insert select sql starts at: " + startTime));
                this.connection.executeUpdate(insertStageTableSQL);
                final long endTime = System.currentTimeMillis();
                logger.info((Object) ("the insert select sql ends at: " + endTime));
                logger.info((Object) ("the total elapsed time of the insert select sql  is: " + (endTime - startTime) / 1000L + "s"));
                if (!inputQuery.isEmpty()) {
                    TeradataPlugInConfiguration.setInputFinalQuery(configuration, inputQuery);
                    TeradataPlugInConfiguration.setInputQuery(configuration, "");
                } else {
                    TeradataPlugInConfiguration.setInputFinalTable(configuration, inputTableName);
                    TeradataPlugInConfiguration.setInputFinalDatabase(configuration, inputDatabase);
                    TeradataPlugInConfiguration.setInputFinalConditions(configuration, inputCondition);
                }
                TeradataPlugInConfiguration.setInputStageTableEnabled(configuration, stagingEnabled);
                TeradataPlugInConfiguration.setInputStageAreas(configuration, stageTableName);
                if (!stageDatabaseForTable.isEmpty()) {
                    TeradataPlugInConfiguration.setInputDatabase(configuration, stageDatabaseForTable);
                } else {
                    TeradataPlugInConfiguration.setInputDatabase(configuration, stageDatabase);
                }
                TeradataPlugInConfiguration.setInputTable(configuration, stageTableName);
                TeradataPlugInConfiguration.setInputConditions(configuration, "");
            } catch (SQLException e) {
                throw new ConnectorException(e.getMessage(), e);
            }
        }
    }

    @Override
    protected void cleanupDatabaseEnvironment(final Configuration configuration) throws ConnectorException {
        if (TeradataPlugInConfiguration.getInputStageTableEnabled(configuration)) {
            boolean isStageTableDeleted = false;
            boolean isStageViewDeleted = false;
            String stageTable = null;
            final String stageDatabase = TeradataPlugInConfiguration.getInputStageDatabase(configuration);
            final String stageDatabaseForTable = TeradataPlugInConfiguration.getInputStageDatabaseForTable(configuration);
            final String stageDatabaseForView = TeradataPlugInConfiguration.getInputStageDatabaseForView(configuration);
            final String stageTableName = TeradataPlugInConfiguration.getInputStageAreas(configuration);
            final TeradataTableDesc tableDesc = new TeradataTableDesc();
            tableDesc.setName(stageTableName);
            if (!stageDatabaseForTable.isEmpty()) {
                tableDesc.setDatabaseName(stageDatabaseForTable);
            } else {
                tableDesc.setDatabaseName(stageDatabase);
            }
            stageTable = tableDesc.getQualifiedName();
            try {
                this.connection.dropTable(stageTable);
                isStageTableDeleted = true;
                logger.info((Object) ("staging table " + stageTable + " is dropped"));
            } catch (SQLException e) {
                logger.debug((Object) e.getMessage());
            }
            final TeradataViewDesc viewDesc = new TeradataViewDesc();
            viewDesc.setName("V" + stageTableName);
            if (!stageDatabaseForView.isEmpty()) {
                viewDesc.setDatabaseName(stageDatabaseForView);
            } else {
                viewDesc.setDatabaseName(stageDatabase);
            }
            final String stageView = viewDesc.getQualifiedName();
            try {
                this.connection.dropView(stageView);
                isStageViewDeleted = true;
                logger.info((Object) ("staging view " + stageView + " is dropped"));
            } catch (SQLException e2) {
                logger.debug((Object) e2.getMessage());
            }
            if (!isStageViewDeleted) {
                logger.warn((Object) ("staging view " + stageView + " is not dropped"));
            }
            if (!isStageTableDeleted) {
                logger.warn((Object) ("staging table " + stageTable + " is not dropped"));
            }
        }
    }

    protected boolean stagingTableEnabeled(final Configuration configuration, final String inputTableName, final String inputQuery, final int numMappers) throws ConnectorException {
        if (TeradataPlugInConfiguration.getInputStageTableForced(configuration)) {
            return true;
        }
        if (numMappers == 1) {
            return false;
        }
        if (!inputQuery.isEmpty()) {
            return true;
        }
        try {
            if (!this.connection.isTablePPI(inputTableName)) {
                return true;
            }
        } catch (SQLException e) {
            throw new ConnectorException(e.getMessage(), e);
        }
        return false;
    }

    @Override
    protected void validateConfiguration(final Configuration configuration, final TeradataConnection connection) throws ConnectorException {
        super.validateConfiguration(configuration, connection);
        final long numPartitions = TeradataPlugInConfiguration.getInputNumPartitions(configuration);
        if (numPartitions < 1L) {
            throw new ConnectorException(12018);
        }
    }
}
