package com.teradata.connector.hdfs.processor;

import com.teradata.connector.common.ConnectorOutputProcessor;
import com.teradata.connector.common.ConnectorRecordSchema;
import com.teradata.connector.common.exception.ConnectorException;
import com.teradata.connector.common.utils.ConnectorConfiguration;
import com.teradata.connector.common.utils.ConnectorSchemaUtils;
import com.teradata.connector.hdfs.converter.HdfsAvroDataTypeDefinition;
import com.teradata.connector.hdfs.utils.HdfsAvroSchemaUtils;
import com.teradata.connector.hdfs.utils.HdfsPlugInConfiguration;
import java.util.List;
import org.apache.avro.Schema;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.JobContext;

/**
 * @author Administrator
 */
public class HdfsAvroOutputProcessor implements ConnectorOutputProcessor {
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
        final String avroSchemaText = HdfsPlugInConfiguration.getOutputAvroSchema(configuration);
        final String avroschemafile = HdfsPlugInConfiguration.getOutputAvroSchemaFile(configuration);
        Schema avroSchema = null;
        if (!avroSchemaText.isEmpty()) {
            avroSchema = new Schema.Parser().parse(avroSchemaText);
            if (avroSchema == null) {
                throw new ConnectorException(50011);
            }
            configuration.set("avro.schema.output.key", avroSchema.toString());
        } else {
            if (avroschemafile.isEmpty()) {
                throw new ConnectorException(50005);
            }
            avroSchema = HdfsAvroSchemaUtils.buildAvroSchemaFromFile(configuration, avroschemafile);
            if (avroSchema == null) {
                throw new ConnectorException(50011);
            }
            HdfsPlugInConfiguration.setOutputAvroSchema(configuration, avroSchema.toString());
            configuration.set("avro.schema.output.key", avroSchema.toString());
        }
        final String[] fieldNames = HdfsPlugInConfiguration.getOutputFieldNamesArray(configuration);
        if (fieldNames.length > 0 && avroSchema != null) {
            HdfsAvroSchemaUtils.checkFieldNamesInSchema(fieldNames, avroSchema);
        }
        final List<Schema.Field> fields = (List<Schema.Field>) avroSchema.getFields();
        int[] mappings;
        if (fieldNames.length != 0) {
            mappings = HdfsAvroSchemaUtils.getAvroColumnMapping(avroSchema, fieldNames);
        } else {
            final int size = fields.size();
            mappings = new int[size];
            for (int i = 0; i < size; ++i) {
                mappings[i] = i;
            }
        }
        final ConnectorRecordSchema userRecordSchema = ConnectorSchemaUtils.recordSchemaFromString(ConnectorConfiguration.getOutputConverterRecordSchema(configuration));
        final ConnectorRecordSchema targetRecordSchema = new ConnectorRecordSchema(mappings.length);
        for (int index = 0; index < mappings.length; ++index) {
            targetRecordSchema.setFieldType(index, HdfsAvroDataTypeDefinition.getAvroDataType(fields.get(mappings[index]).schema()));
        }
        HdfsAvroSchemaUtils.formalizeAvroRecordSchema(fields, targetRecordSchema, mappings);
        ConnectorSchemaUtils.formalizeConnectorRecordSchema(targetRecordSchema);
        if (userRecordSchema != null) {
            final int columnCount = targetRecordSchema.getLength();
            if (columnCount != userRecordSchema.getLength()) {
                throw new ConnectorException(14014);
            }
            for (int j = 0; j < columnCount; ++j) {
                if (userRecordSchema.getFieldType(j) != targetRecordSchema.getFieldType(j)) {
                    throw new ConnectorException(14016);
                }
            }
        } else {
            ConnectorConfiguration.setOutputConverterRecordSchema(configuration, ConnectorSchemaUtils.recordSchemaToString(targetRecordSchema));
        }
        return 0;
    }

    @Override
    public int outputPostProcessor(final JobContext context) throws ConnectorException {
        return 0;
    }
}
