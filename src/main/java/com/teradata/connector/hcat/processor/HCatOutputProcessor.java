package com.teradata.connector.hcat.processor;

import com.teradata.connector.common.ConnectorOutputProcessor;
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
public class HCatOutputProcessor implements ConnectorOutputProcessor {
    @Override
    public int outputPreProcessor(final JobContext context) throws ConnectorException {
        try {
            Configuration configuration = context.getConfiguration();
            HiveUtils.loadHiveConf(configuration, ConnectorConfiguration.direction.output);
            String databaseName = HCatPlugInConfiguration.getOutputDatabase(configuration);
            if (databaseName.isEmpty()) {
                databaseName = HiveUtils.DEFAULT_DATABASE;
            }
            String tableName = HCatPlugInConfiguration.getOutputTable(configuration);
            if (tableName.isEmpty()) {
                throw new ConnectorException((int) ConnectorException.ErrorCode.OUTPUT_TARGET_TABLE_MISSING);
            }
            List<String> HCatSchemaFieldNames = HCatSchemaUtils.getHCatSchemaFieldNamesFromHCatOutputFormat(context, databaseName, tableName);
            String[] fieldNames = HCatPlugInConfiguration.getOutputFieldNamesArray(configuration);
            if (fieldNames.length == 0) {
                HCatPlugInConfiguration.setOutputFieldNamesArray(configuration, (String[]) HCatSchemaFieldNames.toArray(new String[0]));
                fieldNames = HCatPlugInConfiguration.getOutputFieldNamesArray(configuration);
            }
            String tableSchema = HCatSchemaUtils.getTableSchemaFromHCatOutputFormat(context, databaseName, tableName);
            HCatPlugInConfiguration.setOutputTableSchema(configuration, tableSchema);
            HCatPlugInConfiguration.setOutputTableFieldTypes(configuration, (String[]) ConnectorSchemaUtils.parseColumnTypes(ConnectorSchemaUtils.parseColumns(tableSchema.toLowerCase())).toArray(new String[0]));
            HCatPlugInConfiguration.setOutputFieldTypeNamesArray(configuration, HCatSchemaUtils.getTargetFieldsTypeNameFromHCatOutputFormat(context, databaseName, tableName, fieldNames));
            HCatPlugInConfiguration.setTargetSchemaFieldNamesArray(configuration, (String[]) HCatSchemaFieldNames.toArray(new String[0]));
            ConnectorRecordSchema userRecordSchema = ConnectorSchemaUtils.recordSchemaFromString(ConnectorConfiguration.getOutputConverterRecordSchema(configuration));
            ConnectorRecordSchema targetRecordSchema = HCatSchemaUtils.getRecordSchema(context, databaseName, tableName, fieldNames, ConnectorConfiguration.direction.output);
            if (userRecordSchema == null) {
                ConnectorConfiguration.setOutputConverterRecordSchema(configuration, ConnectorSchemaUtils.recordSchemaToString(ConnectorSchemaUtils.formalizeConnectorRecordSchema(targetRecordSchema)));
            }
            return 0;
        } catch (IOException e) {
            throw new ConnectorException(e.getMessage(), e);
        }

    }

    @Override
    public int outputPostProcessor(final JobContext context) throws ConnectorException {
        return 0;
    }
}
