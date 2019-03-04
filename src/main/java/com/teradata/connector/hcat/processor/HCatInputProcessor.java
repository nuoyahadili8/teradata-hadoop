package com.teradata.connector.hcat.processor;

import com.teradata.connector.common.ConnectorInputProcessor;
import com.teradata.connector.common.ConnectorRecordSchema;
import com.teradata.connector.common.exception.ConnectorException;
import com.teradata.connector.common.utils.ConnectorConfiguration;
import com.teradata.connector.common.utils.ConnectorSchemaUtils;
import com.teradata.connector.hcat.utils.HCatPlugInConfiguration;
import com.teradata.connector.hcat.utils.HCatSchemaUtils;
import com.teradata.connector.hive.utils.HiveUtils;

import java.io.IOException;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.JobContext;


/**
 * @author Administrator
 */
@Deprecated
public class HCatInputProcessor implements ConnectorInputProcessor {
    @Override
    public int inputPreProcessor(final JobContext context) throws ConnectorException {
        try {
            Configuration configuration = context.getConfiguration();
            HiveUtils.loadHiveConf(configuration, ConnectorConfiguration.direction.input);
            String databaseName = HCatPlugInConfiguration.getInputDatabase(configuration);
            if (databaseName.isEmpty()) {
                databaseName = HiveUtils.DEFAULT_DATABASE;
            }
            String tableName = HCatPlugInConfiguration.getInputTable(configuration);
            if (tableName.isEmpty()) {
                throw new ConnectorException((int) ConnectorException.ErrorCode.INPUT_SOURCE_TABLE_MISSING);
            }
            ConnectorConfiguration.setUseCombinedInputFormat(configuration, false);
            List<String> HCatSchemaFieldNames = HCatSchemaUtils.getHCatSchemaFieldNamesFromHCatInputFormat(configuration, databaseName, tableName);
            String[] fieldNames = HCatPlugInConfiguration.getInputFieldNamesArray(configuration);
            if (fieldNames.length == 0) {
                HCatPlugInConfiguration.setInputFieldNamesArray(configuration, (String[]) HCatSchemaFieldNames.toArray(new String[0]));
                fieldNames = HCatPlugInConfiguration.getInputFieldNamesArray(configuration);
            }
            String tableSchema = HCatSchemaUtils.getTableSchemaFromHCatInputFormat(configuration, databaseName, tableName);
            HCatPlugInConfiguration.setInputTableSchema(configuration, tableSchema);
            HCatPlugInConfiguration.setSourceSchemaFieldNamesArray(configuration, (String[]) HCatSchemaFieldNames.toArray(new String[0]));
            HCatPlugInConfiguration.setInputTableFieldTypes(configuration, (String[]) ConnectorSchemaUtils.parseColumnTypes(ConnectorSchemaUtils.parseColumns(tableSchema.toLowerCase())).toArray(new String[0]));
            if (!(!ConnectorConfiguration.getJobMapper(configuration).isEmpty())) {
                ConnectorRecordSchema sourceRecordSchema = HCatSchemaUtils.getRecordSchema(context, databaseName, tableName, fieldNames, ConnectorConfiguration.direction.input);
                ConnectorRecordSchema userRecordSchema = ConnectorSchemaUtils.recordSchemaFromString(ConnectorConfiguration.getInputConverterRecordSchema(configuration));
                if (userRecordSchema == null) {
                    ConnectorConfiguration.setInputConverterRecordSchema(configuration, ConnectorSchemaUtils.recordSchemaToString(ConnectorSchemaUtils.formalizeConnectorRecordSchema(sourceRecordSchema)));
                } else if (userRecordSchema.getLength() != sourceRecordSchema.getLength()) {
                    throw new ConnectorException((int) ConnectorException.ErrorCode.COLUMN_LENGTH_OF_SOURCE_RECORD_SCHEMA_MISMATCH_LENGTH_OF_FIELD_NAMES);
                } else {
                    int columnCount = sourceRecordSchema.getLength();
                    int i = 0;
                    while (i < columnCount) {
                        if (userRecordSchema.getFieldType(i) == 1883 || userRecordSchema.getFieldType(i) == sourceRecordSchema.getFieldType(i)) {
                            i++;
                        } else {
                            throw new ConnectorException((int) ConnectorException.ErrorCode.COLUMN_TYPE_OF_TARGET_RECORD_SCHEMA_MISMATCH_FIELD_TYPE_IN_SCHEMA);
                        }
                    }
                }
            }
            return 0;
        } catch (IOException e) {
            throw new ConnectorException(e.getMessage(), e);
        }
    }

    @Override
    public int inputPostProcessor(final JobContext context) throws ConnectorException {
        return 0;
    }
}
