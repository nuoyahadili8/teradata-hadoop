package com.teradata.connector.hdfs.processor;

import com.teradata.connector.common.ConnectorInputProcessor;
import com.teradata.connector.common.ConnectorRecordSchema;
import com.teradata.connector.common.exception.ConnectorException;
import com.teradata.connector.common.utils.ConnectorConfiguration;
import com.teradata.connector.common.utils.ConnectorSchemaUtils;
import com.teradata.connector.common.utils.HadoopConfigurationUtils;
import com.teradata.connector.hdfs.utils.HdfsPlugInConfiguration;
import com.teradata.connector.hdfs.utils.HdfsSchemaUtils;
import java.io.IOException;
import java.util.List;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.JobContext;

/**
 * @author Administrator
 */
public class HdfsTextFileInputProcessor implements ConnectorInputProcessor {
    private Log logger;

    public HdfsTextFileInputProcessor() {
        this.logger = LogFactory.getLog((Class) HdfsTextFileInputProcessor.class);
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
        final String[] fieldnames = HdfsPlugInConfiguration.getInputFieldNamesArray(configuration);
        final String sourceSchema = HdfsPlugInConfiguration.getInputSchema(configuration);
        if (fieldnames.length > 0 && sourceSchema.isEmpty()) {
            throw new ConnectorException(50009);
        }
        if (fieldnames.length > 0 && !sourceSchema.isEmpty()) {
            HdfsSchemaUtils.checkFieldNamesInSchema(fieldnames, sourceSchema);
        }
        final boolean isUDMapper = !ConnectorConfiguration.getJobMapper(configuration).isEmpty();
        if (!isUDMapper) {
            List<String> schemaFieldNames = null;
            List<String> schemaFieldDataTypes = null;
            List<String> schemaFields = null;
            if (!sourceSchema.isEmpty()) {
                schemaFields = ConnectorSchemaUtils.parseColumns(sourceSchema.toLowerCase());
                schemaFieldNames = ConnectorSchemaUtils.parseColumnNames(schemaFields);
                schemaFieldDataTypes = ConnectorSchemaUtils.parseColumnTypes(schemaFields);
            }
            final ConnectorRecordSchema userRecordSchema = ConnectorSchemaUtils.recordSchemaFromString(ConnectorConfiguration.getInputConverterRecordSchema(configuration));
            if (userRecordSchema != null) {
                if (sourceSchema.isEmpty()) {
                    throw new ConnectorException(50008);
                }
                if (fieldnames.length != 0) {
                    if (userRecordSchema.getLength() != fieldnames.length) {
                        throw new ConnectorException(14013);
                    }
                } else {
                    if (userRecordSchema.getLength() != schemaFields.size()) {
                        throw new ConnectorException(14013);
                    }
                    HdfsSchemaUtils.checkDataType(schemaFieldDataTypes, userRecordSchema.getFieldTypes());
                }
            } else {
                ConnectorRecordSchema sourceRecordSchema = null;
                if (schemaFieldNames != null && schemaFieldNames.size() > 0) {
                    if (fieldnames.length > 0) {
                        final int[] mapping = HdfsSchemaUtils.getColumnMapping(schemaFieldNames, fieldnames);
                        sourceRecordSchema = new ConnectorRecordSchema(fieldnames.length);
                        for (int index = 0; index < fieldnames.length; ++index) {
                            sourceRecordSchema.setFieldType(index, HdfsSchemaUtils.lookupHdfsAvroDatatype(schemaFieldDataTypes.get(mapping[index])));
                        }
                    } else {
                        sourceRecordSchema = new ConnectorRecordSchema(schemaFieldNames.size());
                        for (int index2 = 0; index2 < schemaFieldNames.size(); ++index2) {
                            sourceRecordSchema.setFieldType(index2, HdfsSchemaUtils.lookupHdfsAvroDatatype(schemaFieldDataTypes.get(index2)));
                        }
                    }
                    ConnectorConfiguration.setInputConverterRecordSchema(configuration, ConnectorSchemaUtils.recordSchemaToString(ConnectorSchemaUtils.formalizeConnectorRecordSchema(sourceRecordSchema)));
                }
            }
        }
        return 0;
    }

    @Override
    public int inputPostProcessor(final JobContext context) throws ConnectorException {
        return 0;
    }
}
