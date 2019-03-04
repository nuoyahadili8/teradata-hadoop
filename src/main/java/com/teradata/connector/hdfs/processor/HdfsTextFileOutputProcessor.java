package com.teradata.connector.hdfs.processor;

import com.teradata.connector.common.ConnectorOutputProcessor;
import com.teradata.connector.common.ConnectorRecordSchema;
import com.teradata.connector.common.exception.ConnectorException;
import com.teradata.connector.common.utils.ConnectorConfiguration;
import com.teradata.connector.common.utils.ConnectorSchemaUtils;
import com.teradata.connector.hdfs.utils.HdfsPlugInConfiguration;
import com.teradata.connector.hdfs.utils.HdfsSchemaUtils;

import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.JobContext;


/**
 * @author Administrator
 */
public class HdfsTextFileOutputProcessor implements ConnectorOutputProcessor {
    @Override
    public int outputPreProcessor(final JobContext context) throws ConnectorException {
        final Configuration configuration = context.getConfiguration();
        final String outputPaths = HdfsPlugInConfiguration.getOutputPaths(configuration);
        if (outputPaths.isEmpty()) {
            final String dir = configuration.get("mapred.output.dir", "");
            if (dir.isEmpty()) {
                throw new ConnectorException(12003);
            }
            HdfsPlugInConfiguration.setOutputPaths(configuration, dir);
        }
        final String targetSchema = HdfsPlugInConfiguration.getOutputSchema(configuration);
        final String[] targetFieldNames = HdfsPlugInConfiguration.getOutputFieldNamesArray(configuration);
        if (targetFieldNames.length > 0 && targetSchema.isEmpty()) {
            throw new ConnectorException(50009);
        }
        if (!targetSchema.isEmpty()) {
            final List<String> tableFields = ConnectorSchemaUtils.parseColumns(targetSchema.toLowerCase());
            final List<String> tableFieldNames = ConnectorSchemaUtils.parseColumnNames(tableFields);
            final List<String> tableFieldDataTypes = ConnectorSchemaUtils.parseColumnTypes(tableFields);
            final ConnectorRecordSchema userRecordSchema = ConnectorSchemaUtils.recordSchemaFromString(ConnectorConfiguration.getOutputConverterRecordSchema(configuration));
            ConnectorRecordSchema targetRecordSchema;
            if (targetFieldNames.length > 0) {
                final int[] mapping = HdfsSchemaUtils.getColumnMapping(tableFieldNames, targetFieldNames);
                targetRecordSchema = new ConnectorRecordSchema(targetFieldNames.length);
                for (int index = 0; index < targetFieldNames.length; ++index) {
                    targetRecordSchema.setFieldType(index, HdfsSchemaUtils.lookupHdfsAvroDatatype(tableFieldDataTypes.get(mapping[index])));
                }
            } else {
                targetRecordSchema = new ConnectorRecordSchema(tableFieldNames.size());
                for (int index2 = 0; index2 < tableFieldNames.size(); ++index2) {
                    targetRecordSchema.setFieldType(index2, HdfsSchemaUtils.lookupHdfsAvroDatatype(tableFieldDataTypes.get(index2)));
                }
            }
            if (userRecordSchema != null) {
                if (userRecordSchema.getLength() != targetRecordSchema.getLength()) {
                    throw new ConnectorException(14014);
                }
                for (int columnCount = targetRecordSchema.getLength(), i = 0; i < columnCount; ++i) {
                    if (userRecordSchema.getFieldType(i) != targetRecordSchema.getFieldType(i)) {
                        throw new ConnectorException(14016);
                    }
                }
            } else {
                ConnectorConfiguration.setOutputConverterRecordSchema(configuration, ConnectorSchemaUtils.recordSchemaToString(ConnectorSchemaUtils.formalizeConnectorRecordSchema(targetRecordSchema)));
            }
        }
        return 0;
    }

    @Override
    public int outputPostProcessor(final JobContext context) throws ConnectorException {
        return 0;
    }
}
