package com.teradata.connector.idatastream.processor;

import com.teradata.connector.common.ConnectorOutputProcessor;
import com.teradata.connector.common.ConnectorRecordSchema;
import com.teradata.connector.common.exception.ConnectorException;
import com.teradata.connector.common.exception.ConnectorException.ErrorCode;
import com.teradata.connector.common.utils.ConnectorConfiguration;
import com.teradata.connector.common.utils.ConnectorSchemaUtils;
import com.teradata.connector.idatastream.schema.IDataStreamColumnDesc;
import com.teradata.connector.idatastream.utils.IDataStreamPlugInConfiguration;
import com.teradata.connector.idatastream.utils.IDataStreamUtils;
import com.teradata.connector.teradata.utils.TeradataUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.JobContext;


/**
 * @author Administrator
 */
public class IDataStreamOutputProcessor implements ConnectorOutputProcessor {
    private static Log logger;

    @Override
    public int outputPreProcessor(final JobContext context) throws ConnectorException {
        final long startTime = System.currentTimeMillis();
        IDataStreamOutputProcessor.logger.info("output preprocessor " + this.getClass().getName() + " starts at:  " + startTime);
        final Configuration configuration = context.getConfiguration();
        this.validateConfiguration(configuration);
        final String tdchVersion = TeradataUtils.getTdchVersionNumber();
        IDataStreamOutputProcessor.logger.info("the teradata connector for hadoop version is: " + tdchVersion);
        configureTargetConnectorRecordSchema(configuration);
        IDataStreamOutputProcessor.logger.info("the number of mappers are " + ConnectorConfiguration.getNumMappers(configuration));
        final long endTime = System.currentTimeMillis();
        IDataStreamOutputProcessor.logger.info("output preprocessor " + this.getClass().getName() + " ends at:  " + endTime);
        IDataStreamOutputProcessor.logger.info("the total elapsed time of output preprocessor " + this.getClass().getName() + " is: " + (endTime - startTime) / 1000L + "s");
        return 0;
    }

    @Override
    public int outputPostProcessor(final JobContext context) throws ConnectorException {
        return 0;
    }

    public void validateConfiguration(final Configuration configuration) throws ConnectorException {
        final String outputSocketHost = IDataStreamPlugInConfiguration.getOutputSocketHost(configuration);
        final String outputSocketPort = IDataStreamPlugInConfiguration.getOutputSocketPort(configuration);
        String[] outputFieldNamesArray = IDataStreamPlugInConfiguration.getOutputFieldNamesArray(configuration);
        int outputFieldNamesCount = outputFieldNamesArray.length;
        String[] outputFieldTypesArray = IDataStreamPlugInConfiguration.getOutputFieldTypesArray(configuration);
        int outputFieldTypesCount = outputFieldTypesArray.length;
        if (outputSocketHost.isEmpty()) {
            throw new ConnectorException(60001);
        }
        if (outputSocketPort.isEmpty()) {
            throw new ConnectorException(60002);
        }
        try {
            final int i = Integer.parseInt(outputSocketPort);
            if (i <= 0) {
                throw new ConnectorException(60002);
            }
        } catch (Exception e) {
            throw new ConnectorException(60002);
        }
        if (outputFieldNamesCount == 0) {
            final String outputFieldNames = IDataStreamPlugInConfiguration.getOutputFieldNames(configuration);
            if (outputFieldNames.isEmpty()) {
                throw new ConnectorException(60003);
            }
            IDataStreamPlugInConfiguration.setOutputFieldNames(configuration, outputFieldNames);
            outputFieldNamesArray = IDataStreamPlugInConfiguration.getOutputFieldNamesArray(configuration);
            outputFieldNamesCount = outputFieldNamesArray.length;
        }
        if (outputFieldTypesCount == 0) {
            final String outputFieldTypes = IDataStreamPlugInConfiguration.getOutputFieldTypes(configuration);
            if (outputFieldTypes.isEmpty()) {
                throw new ConnectorException(60004);
            }
            IDataStreamPlugInConfiguration.setOutputFieldTypes(configuration, outputFieldTypes);
            outputFieldTypesArray = IDataStreamPlugInConfiguration.getOutputFieldTypesArray(configuration);
            outputFieldTypesCount = outputFieldTypesArray.length;
        }
        if (outputFieldTypesCount != outputFieldNamesCount) {
            throw new ConnectorException(60004);
        }
    }

    public static void configureTargetConnectorRecordSchema(final Configuration configuration) throws ConnectorException {
        final String[] outputFieldTypes = IDataStreamPlugInConfiguration.getOutputFieldTypesArray(configuration);
        final int outputFieldCount = outputFieldTypes.length;
        final ConnectorRecordSchema targetRecordSchema = new ConnectorRecordSchema(outputFieldCount);
        for (int i = 0; i < outputFieldCount; ++i) {
            final IDataStreamColumnDesc colDesc = IDataStreamUtils.getIDataStreamColumnDescFromString(outputFieldTypes[i]);
            targetRecordSchema.setFieldType(i, colDesc.getJDBCtype());
        }
        ConnectorConfiguration.setOutputConverterRecordSchema(configuration, ConnectorSchemaUtils.recordSchemaToString(ConnectorSchemaUtils.formalizeConnectorRecordSchema(targetRecordSchema)));
    }

    static {
        IDataStreamOutputProcessor.logger = LogFactory.getLog((Class) IDataStreamOutputProcessor.class);
    }
}
