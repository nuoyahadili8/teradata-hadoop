package com.teradata.connector.sample.plugin.processor;

import com.teradata.connector.common.ConnectorInputProcessor;
import com.teradata.connector.common.ConnectorRecordSchema;
import com.teradata.connector.common.exception.ConnectorException;

import java.sql.Connection;
import java.sql.SQLException;

import com.teradata.connector.common.utils.ConnectorConfiguration;
import com.teradata.connector.common.utils.ConnectorSchemaUtils;
import com.teradata.connector.sample.plugin.utils.CommonDBConfiguration;
import com.teradata.connector.sample.plugin.utils.CommonDBSchemaUtils;
import com.teradata.connector.sample.plugin.utils.CommonDBUtils;
import com.teradata.connector.teradata.schema.TeradataColumnDesc;
import com.teradata.connector.teradata.schema.TeradataTableDesc;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.JobContext;


public abstract class CommonDBInputProcessor implements ConnectorInputProcessor {
    public abstract String getTableName(final Configuration p0) throws ConnectorException;

    public abstract Connection getConnection(final Configuration p0) throws ConnectorException;

    @Override
    public int inputPreProcessor(final JobContext context) {
        Connection connection = null;
        try {
            final Configuration configuration = context.getConfiguration();
            connection = this.getConnection(configuration);
            final String tableName = this.getTableName(configuration);
            String[] fieldNames = CommonDBConfiguration.getInputFieldNamesArray(configuration);
            final TeradataColumnDesc[] columnDesc = CommonDBSchemaUtils.getColumnDesc(tableName, fieldNames, connection);
            final TeradataTableDesc inputTableDesc = new TeradataTableDesc();
            inputTableDesc.setColumns(columnDesc);
            CommonDBConfiguration.setInputTableDesc(configuration, CommonDBSchemaUtils.tableDescToJson(inputTableDesc));
            ConnectorConfiguration.setInputSplit(configuration, "com.teradata.connector.sample.CommonDBInputFormat$CommonDBInputSplit");
            if (fieldNames.length == 0) {
                CommonDBConfiguration.setInputFieldNamesArray(configuration, inputTableDesc.getColumnNames());
                fieldNames = inputTableDesc.getColumnNames();
            }
            final ConnectorRecordSchema userRecordSchema = ConnectorSchemaUtils.recordSchemaFromString(ConnectorConfiguration.getInputConverterRecordSchema(configuration));
            if (userRecordSchema != null && fieldNames.length != userRecordSchema.getLength()) {
                throw new ConnectorException(14013);
            }
            final TeradataTableDesc sourceTableDesc = CommonDBSchemaUtils.tableDescFromText(CommonDBConfiguration.getInputTableDesc(configuration));
            final TeradataColumnDesc[] columnDescs = sourceTableDesc.getColumns();
            final ConnectorRecordSchema sourceRecordSchema = new ConnectorRecordSchema(fieldNames.length);
            int index = 0;
            boolean findField = false;
            for (final String fieldName : fieldNames) {
                findField = false;
                for (final TeradataColumnDesc column : columnDescs) {
                    if (fieldName.equalsIgnoreCase(column.getName())) {
                        sourceRecordSchema.setFieldType(index++, CommonDBSchemaUtils.tranformTeradataDataType(column.getType()));
                        findField = true;
                        break;
                    }
                }
                if (!findField) {
                    throw new ConnectorException(14005);
                }
            }
            if (userRecordSchema != null) {
                for (int columnCount = sourceRecordSchema.getLength(), i = 0; i < columnCount; ++i) {
                    if (userRecordSchema.getFieldType(i) != 1883 && userRecordSchema.getFieldType(i) != sourceRecordSchema.getFieldType(i)) {
                        throw new ConnectorException(14015);
                    }
                }
            }
            if (userRecordSchema == null) {
                ConnectorConfiguration.setInputConverterRecordSchema(configuration, ConnectorSchemaUtils.recordSchemaToString(ConnectorSchemaUtils.formalizeConnectorRecordSchema(sourceRecordSchema)));
            }
            return 0;
        } catch (ConnectorException e) {
            e.printStackTrace();
        } catch (SQLException e2) {
            e2.printStackTrace();
        } finally {
            CommonDBUtils.CloseConnection(connection);
        }
        return 0;
    }

    @Override
    public int inputPostProcessor(final JobContext context) {
        return 0;
    }
}
