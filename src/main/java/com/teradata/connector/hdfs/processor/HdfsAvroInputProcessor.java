package com.teradata.connector.hdfs.processor;

import com.teradata.connector.common.ConnectorInputProcessor;
import com.teradata.connector.common.ConnectorRecordSchema;
import com.teradata.connector.common.exception.ConnectorException;
import com.teradata.connector.common.exception.ConnectorException.ErrorCode;
import com.teradata.connector.common.exception.ConnectorException.ErrorMessage;
import com.teradata.connector.common.utils.ConnectorConfiguration;
import com.teradata.connector.common.utils.ConnectorSchemaUtils;
import com.teradata.connector.common.utils.HadoopConfigurationUtils;
import com.teradata.connector.hdfs.converter.HdfsAvroDataTypeDefinition;
import com.teradata.connector.hdfs.utils.HdfsAvroSchemaUtils;
import com.teradata.connector.hdfs.utils.HdfsPlugInConfiguration;
import java.io.IOException;
import java.util.List;
import org.apache.avro.Schema;
import org.apache.avro.Schema.Field;
import org.apache.avro.Schema.Parser;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.JobContext;

/**
 * @author Administrator
 */
public class HdfsAvroInputProcessor implements ConnectorInputProcessor {
    private Log logger;

    public HdfsAvroInputProcessor() {
        this.logger = LogFactory.getLog((Class) HdfsAvroInputProcessor.class);
    }

    @Override
    public int inputPreProcessor(final JobContext context) throws ConnectorException {
        final Configuration configuration = context.getConfiguration();
        String inputPaths = HdfsPlugInConfiguration.getInputPaths(configuration);
        if (inputPaths.isEmpty()) {
            final String dir = configuration.get("mapred.input.dir", "");
            if (dir.isEmpty()) {
                throw new ConnectorException(13005);
            }
            HdfsPlugInConfiguration.setInputPaths(configuration, dir);
        }
        try {
            inputPaths = HdfsPlugInConfiguration.getInputPaths(configuration);
            final String allPaths = HadoopConfigurationUtils.getAllFilePaths(configuration, new String[]{inputPaths});
            if (allPaths.isEmpty()) {
                this.logger.warn((Object) "HDFS input source path is empty with no data");
                return 1001;
            }
        } catch (IOException e) {
            throw new ConnectorException(e.getMessage(), e);
        }
        final String avroSchemaText = HdfsPlugInConfiguration.getInputAvroSchema(configuration);
        final String avroSchemaFile = HdfsPlugInConfiguration.getInputAvroSchemaFile(configuration);
        Schema avroSchema = null;
        if (!avroSchemaText.isEmpty()) {
            avroSchema = new Schema.Parser().parse(avroSchemaText);
            if (avroSchema == null) {
                throw new ConnectorException(50011);
            }
        } else if (!avroSchemaFile.isEmpty()) {
            avroSchema = HdfsAvroSchemaUtils.buildAvroSchemaFromFile(configuration, avroSchemaFile);
            if (avroSchema == null) {
                throw new ConnectorException(50011);
            }
            HdfsPlugInConfiguration.setInputAvroSchema(configuration, avroSchema.toString());
        } else {
            avroSchema = HdfsAvroSchemaUtils.fetchSchemaFromInputPath(configuration);
            if (avroSchema == null) {
                throw new ConnectorException(50005);
            }
            HdfsPlugInConfiguration.setInputAvroSchema(configuration, avroSchema.toString());
        }
        final String[] fieldNames = HdfsPlugInConfiguration.getInputFieldNamesArray(configuration);
        if (fieldNames.length > 0 && avroSchema != null) {
            HdfsAvroSchemaUtils.checkFieldNamesInSchema(fieldNames, avroSchema);
        }
        final boolean isUDMapper = !ConnectorConfiguration.getJobMapper(configuration).isEmpty();
        if (!isUDMapper) {
            final ConnectorRecordSchema userRecordSchema = ConnectorSchemaUtils.recordSchemaFromString(ConnectorConfiguration.getInputConverterRecordSchema(configuration));
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
            final ConnectorRecordSchema sourceRecordSchema = new ConnectorRecordSchema(mappings.length);
            for (int index = 0; index < mappings.length; ++index) {
                sourceRecordSchema.setFieldType(index, HdfsAvroDataTypeDefinition.getAvroDataType(fields.get(mappings[index]).schema()));
            }
            HdfsAvroSchemaUtils.formalizeAvroRecordSchema(fields, sourceRecordSchema, mappings);
            ConnectorSchemaUtils.formalizeConnectorRecordSchema(sourceRecordSchema);
            if (userRecordSchema != null) {
                final int columnCount = sourceRecordSchema.getLength();
                if (columnCount != userRecordSchema.getLength()) {
                    throw new ConnectorException(14013);
                }
                for (int j = 0; j < columnCount; ++j) {
                    if (userRecordSchema.getFieldType(j) != 1883 && userRecordSchema.getFieldType(j) != sourceRecordSchema.getFieldType(j)) {
                        throw new ConnectorException(14015);
                    }
                }
                HdfsAvroSchemaUtils.formalizeAvroRecordSchema(fields, userRecordSchema, mappings);
            } else {
                ConnectorConfiguration.setInputConverterRecordSchema(configuration, ConnectorSchemaUtils.recordSchemaToString(ConnectorSchemaUtils.formalizeConnectorRecordSchema(sourceRecordSchema)));
            }
        }
        return 0;
    }

    @Override
    public int inputPostProcessor(final JobContext context) throws ConnectorException {
        return 0;
    }
}
