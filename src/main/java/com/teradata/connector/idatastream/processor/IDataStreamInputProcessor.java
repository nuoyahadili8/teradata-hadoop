package com.teradata.connector.idatastream.processor;

import com.teradata.connector.common.ConnectorInputProcessor;
import com.teradata.connector.common.ConnectorRecordSchema;
import com.teradata.connector.common.exception.ConnectorException;
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
public class IDataStreamInputProcessor implements ConnectorInputProcessor {
    private static Log logger = LogFactory.getLog((Class) IDataStreamInputProcessor.class);;

    @Override
    public int inputPreProcessor(final JobContext context) throws ConnectorException {
        final long startTime = System.currentTimeMillis();
        IDataStreamInputProcessor.logger.info((Object) ("input preprocessor " + this.getClass().getName() + " starts at:  " + startTime));
        final Configuration configuration = context.getConfiguration();
        this.validateConfiguration(configuration);
        final String tdchVersion = TeradataUtils.getTdchVersionNumber();
        IDataStreamInputProcessor.logger.info((Object) ("the teradata connector for hadoop version is: " + tdchVersion));
        ConnectorConfiguration.setInputSplit(configuration, "com.teradata.connector.idatastream.IDataStreamInputFormat$IDataStreamInputSplit");
        configureSourceConnectorRecordSchema(configuration);
        IDataStreamInputProcessor.logger.info((Object) ("the number of mappers are " + ConnectorConfiguration.getNumMappers(configuration)));
        final long endTime = System.currentTimeMillis();
        IDataStreamInputProcessor.logger.info((Object) ("input preprocessor " + this.getClass().getName() + " ends at:  " + endTime));
        IDataStreamInputProcessor.logger.info((Object) ("the total elapsed time of input preprocessor " + this.getClass().getName() + " is: " + (endTime - startTime) / 1000L + "s"));
        return 0;
    }

    @Override
    public int inputPostProcessor(final JobContext context) throws ConnectorException {
        return 0;
    }

    public void validateConfiguration(final Configuration configuration) throws ConnectorException {
        final String inputSocketHost = IDataStreamPlugInConfiguration.getInputSocketHost(configuration);
        final String inputSocketPort = IDataStreamPlugInConfiguration.getInputSocketPort(configuration);
        String[] inputFieldNamesArray = IDataStreamPlugInConfiguration.getInputFieldNamesArray(configuration);
        int inputFieldNamesCount = inputFieldNamesArray.length;
        String[] inputFieldTypesArray = IDataStreamPlugInConfiguration.getInputFieldTypesArray(configuration);
        int inputFieldTypesCount = inputFieldTypesArray.length;
        if (inputSocketHost.isEmpty()) {
            throw new ConnectorException(60001);
        }
        if (inputSocketPort.isEmpty()) {
            throw new ConnectorException(60002);
        }
        try {
            final int i = Integer.parseInt(inputSocketPort);
            if (i <= 0) {
                throw new ConnectorException(60002);
            }
        } catch (Exception e) {
            throw new ConnectorException(60002);
        }
        if (inputFieldNamesCount == 0) {
            final String inputFieldNames = IDataStreamPlugInConfiguration.getInputFieldNames(configuration);
            if (inputFieldNames.isEmpty()) {
                throw new ConnectorException(60003);
            }
            IDataStreamPlugInConfiguration.setInputFieldNames(configuration, inputFieldNames);
            inputFieldNamesArray = IDataStreamPlugInConfiguration.getInputFieldNamesArray(configuration);
            inputFieldNamesCount = inputFieldNamesArray.length;
        }
        if (inputFieldTypesCount == 0) {
            final String inputFieldTypes = IDataStreamPlugInConfiguration.getInputFieldTypes(configuration);
            if (inputFieldTypes.isEmpty()) {
                throw new ConnectorException(60003);
            }
            IDataStreamPlugInConfiguration.setInputFieldTypes(configuration, inputFieldTypes);
            inputFieldTypesArray = IDataStreamPlugInConfiguration.getInputFieldTypesArray(configuration);
            inputFieldTypesCount = inputFieldTypesArray.length;
        }
        if (inputFieldTypesCount != inputFieldNamesCount) {
            throw new ConnectorException(60004);
        }
    }

    public static void configureSourceConnectorRecordSchema(final Configuration configuration) throws ConnectorException {
        final String[] inputFieldTypes = IDataStreamPlugInConfiguration.getInputFieldTypesArray(configuration);
        final int inputFieldCount = inputFieldTypes.length;
        final ConnectorRecordSchema sourceRecordSchema = new ConnectorRecordSchema(inputFieldCount);
        for (int i = 0; i < inputFieldCount; ++i) {
            final IDataStreamColumnDesc colDesc = IDataStreamUtils.getIDataStreamColumnDescFromString(inputFieldTypes[i]);
            sourceRecordSchema.setFieldType(i, colDesc.getJDBCtype());
        }
        ConnectorConfiguration.setInputConverterRecordSchema(configuration, ConnectorSchemaUtils.recordSchemaToString(ConnectorSchemaUtils.formalizeConnectorRecordSchema(sourceRecordSchema)));
    }
}
