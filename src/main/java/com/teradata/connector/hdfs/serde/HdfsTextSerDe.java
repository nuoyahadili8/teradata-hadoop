package com.teradata.connector.hdfs.serde;

import com.teradata.connector.common.ConnectorRecord;
import com.teradata.connector.common.ConnectorSerDe;
import com.teradata.connector.common.exception.ConnectorException;
import com.teradata.connector.common.utils.ConnectorConfiguration;
import com.teradata.connector.common.utils.ConnectorCsvParser;
import com.teradata.connector.common.utils.ConnectorCsvPrinter;
import com.teradata.connector.common.utils.ConnectorSchemaUtils;
import com.teradata.connector.common.utils.ConnectorStringUtils;
import com.teradata.connector.hdfs.utils.HdfsPlugInConfiguration;
import com.teradata.connector.hdfs.utils.HdfsSchemaUtils;
import com.teradata.connector.hdfs.utils.HdfsTextTransform;
import java.io.IOException;
import java.util.List;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.JobContext;

public class HdfsTextSerDe implements ConnectorSerDe {
    protected Configuration configuration;
    protected ConnectorRecord sourceConnectorRecord;
    protected String sourceSeparator;
    protected String sourceNullString;
    protected String sourceNullNonString;
    protected String sourceEscapedByString;
    protected String sourceEnclosedByString;
    protected int[] sourceDataTypes;
    protected ConnectorCsvParser csvParser;
    protected int[] sourceMapping;
    protected HdfsTextTransform[] sourceTransformer;
    protected String targetSeparator;
    protected String targetNullString;
    protected String targetNullNonString;
    protected String targetEscapedByString;
    protected String targetEnclosedByString;
    protected int[] targetDataTypes;
    protected int[] targetMappings;
    protected ConnectorCsvPrinter csvPrinter;
    protected Text outValue;
    protected StringBuilder outStringBuilder;
    protected String[] outStringArray;
    protected HdfsTextTransform[] targetTransformer;

    public HdfsTextSerDe() {
        this.sourceConnectorRecord = null;
        this.sourceSeparator = null;
        this.sourceNullString = null;
        this.sourceNullNonString = null;
        this.sourceEscapedByString = null;
        this.sourceEnclosedByString = null;
        this.sourceDataTypes = null;
        this.csvParser = null;
        this.sourceMapping = null;
        this.sourceTransformer = null;
        this.targetSeparator = null;
        this.targetNullString = null;
        this.targetNullNonString = null;
        this.targetEscapedByString = null;
        this.targetEnclosedByString = null;
        this.targetDataTypes = null;
        this.targetMappings = null;
        this.csvPrinter = null;
        this.outValue = null;
        this.outStringBuilder = new StringBuilder();
        this.outStringArray = null;
        this.targetTransformer = null;
    }

    @Override
    public void initialize(final JobContext context, final ConnectorConfiguration.direction direction) throws ConnectorException {
        this.configuration = context.getConfiguration();
        if (direction == ConnectorConfiguration.direction.input) {
            this.sourceSeparator = HdfsPlugInConfiguration.getInputSeparator(this.configuration);
            this.sourceNullString = HdfsPlugInConfiguration.getInputNullString(this.configuration);
            this.sourceNullNonString = HdfsPlugInConfiguration.getInputNullNonString(this.configuration);
            this.sourceEscapedByString = HdfsPlugInConfiguration.getInputEscapedBy(this.configuration);
            this.sourceEnclosedByString = HdfsPlugInConfiguration.getInputEnclosedBy(this.configuration);
            if (!this.sourceEscapedByString.isEmpty() || !this.sourceEnclosedByString.isEmpty()) {
                this.csvParser = new ConnectorCsvParser(this.sourceSeparator, this.sourceEnclosedByString, this.sourceEscapedByString);
            } else if (ConnectorStringUtils.isRegexSpecialChar(this.sourceSeparator)) {
                this.sourceSeparator = "\\" + this.sourceSeparator;
            }
            final String sourceSchema = HdfsPlugInConfiguration.getInputSchema(this.configuration);
            final String[] sourceFieldNames = HdfsPlugInConfiguration.getInputFieldNamesArray(this.configuration);
            if (!sourceSchema.isEmpty()) {
                final List<String> sourceColumnNames = ConnectorSchemaUtils.parseColumnNames(ConnectorSchemaUtils.parseColumns(sourceSchema));
                if (sourceFieldNames.length > 0) {
                    this.sourceConnectorRecord = new ConnectorRecord(sourceFieldNames.length);
                    this.sourceMapping = HdfsSchemaUtils.getColumnMapping(sourceColumnNames, sourceFieldNames);
                } else {
                    this.sourceConnectorRecord = new ConnectorRecord(sourceColumnNames.size());
                }
                this.sourceTransformer = HdfsTextTransform.lookupTextTransformClass(sourceSchema, this.sourceNullString, this.sourceNullNonString, this.configuration, direction);
            } else {
                this.sourceDataTypes = ConnectorSchemaUtils.recordSchemaFromString(ConnectorConfiguration.getOutputConverterRecordSchema(this.configuration)).getFieldTypes();
            }
        } else {
            this.targetSeparator = HdfsPlugInConfiguration.getOutputSeparator(this.configuration);
            this.targetNullString = HdfsPlugInConfiguration.getOutputNullString(this.configuration);
            this.targetNullNonString = HdfsPlugInConfiguration.getOutputNullNonString(this.configuration);
            this.targetEscapedByString = HdfsPlugInConfiguration.getOutputEscapedBy(this.configuration);
            this.targetEnclosedByString = HdfsPlugInConfiguration.getOutputEnclosedBy(this.configuration);
            final List<String> targetSchema = ConnectorSchemaUtils.parseColumns(HdfsPlugInConfiguration.getOutputSchema(this.configuration).toLowerCase());
            final String[] targetFieldNames = HdfsPlugInConfiguration.getOutputFieldNamesArray(this.configuration);
            if (!targetSchema.isEmpty()) {
                final List<String> targetColumnNames = ConnectorSchemaUtils.parseColumnNames(targetSchema);
                if (targetFieldNames.length > 0) {
                    this.targetMappings = HdfsSchemaUtils.getColumnMapping(targetColumnNames, targetFieldNames);
                    this.outStringArray = new String[targetFieldNames.length];
                    this.targetDataTypes = new int[targetFieldNames.length];
                    this.targetDataTypes = HdfsSchemaUtils.getDataTypes(targetSchema, this.targetMappings);
                } else {
                    this.outStringArray = new String[targetColumnNames.size()];
                    this.targetDataTypes = new int[targetColumnNames.size()];
                    this.targetDataTypes = HdfsSchemaUtils.getDataTypes(targetSchema);
                }
                this.targetTransformer = HdfsTextTransform.lookupTextTransformClass(HdfsPlugInConfiguration.getOutputSchema(this.configuration), this.targetNullString, this.targetNullNonString, this.configuration, direction);
            } else {
                this.targetTransformer = null;
                this.targetDataTypes = ConnectorSchemaUtils.recordSchemaFromString(ConnectorConfiguration.getInputConverterRecordSchema(this.configuration)).getFieldTypes();
            }
            if (!this.targetEnclosedByString.isEmpty() || !this.targetEscapedByString.isEmpty()) {
                this.csvPrinter = new ConnectorCsvPrinter(this.targetSeparator, this.targetEnclosedByString, this.targetEscapedByString);
            }
            this.outValue = new Text();
        }
    }

    @Override
    public Writable serialize(final ConnectorRecord connectorRecord) throws ConnectorException {
        if (this.outStringArray == null) {
            this.outStringArray = new String[connectorRecord.getAllObject().length];
        }
        final Object[] objects = connectorRecord.getAllObject();
        for (int l = objects.length, i = 0; i < l; ++i) {
            if (this.targetMappings != null && objects[i] != null) {
                this.outStringArray[i] = ((this.targetTransformer != null) ? this.targetTransformer[this.targetMappings[i]].toString(objects[i]) : objects[i].toString());
            } else {
                this.outStringArray[i] = ((objects[i] != null) ? ((this.targetTransformer != null) ? this.targetTransformer[i].toString(objects[i]) : objects[i].toString()) : null);
            }
        }
        if (!this.targetNullString.isEmpty() || !this.targetNullNonString.isEmpty()) {
            for (int i = 0; i < this.outStringArray.length; ++i) {
                if (this.outStringArray[i] == null) {
                    if (HdfsSchemaUtils.isCharType(this.targetDataTypes[i]) && !this.targetNullString.isEmpty()) {
                        this.outStringArray[i] = this.targetNullString;
                    } else if (!HdfsSchemaUtils.isCharType(this.targetDataTypes[i]) && !this.targetNullNonString.isEmpty()) {
                        this.outStringArray[i] = this.targetNullNonString;
                    }
                }
            }
        }
        if (this.csvPrinter != null) {
            try {
                this.outValue.set(this.csvPrinter.print(this.outStringArray));
                return (Writable) this.outValue;
            } catch (IOException e) {
                throw new ConnectorException(e.getMessage(), e);
            }
        }
        int colnum = 0;
        final int numcols = this.outStringArray.length;
        this.outStringBuilder.setLength(0);
        for (final String column : this.outStringArray) {
            if (column != null) {
                this.outStringBuilder.append(column);
            }
            if (++colnum != numcols) {
                this.outStringBuilder.append(this.targetSeparator);
            }
        }
        this.outValue.set(this.outStringBuilder.toString());
        return (Writable) this.outValue;
    }

    @Override
    public ConnectorRecord deserialize(final Writable writable) throws ConnectorException {
        final String text = ((Text) writable).toString();
        String[] values = null;
        if (this.sourceEscapedByString.isEmpty() && this.sourceEnclosedByString.isEmpty()) {
            values = text.split(this.sourceSeparator, -1);
        } else {
            try {
                values = this.csvParser.parse(text);
            } catch (IOException e) {
                throw new ConnectorException(e.getMessage(), e);
            }
        }
        if (this.sourceConnectorRecord == null) {
            this.sourceConnectorRecord = new ConnectorRecord(values.length);
        }
        if (this.sourceTransformer != null) {
            if (this.sourceMapping != null && this.sourceMapping.length > 0) {
                for (int l = this.sourceMapping.length, i = 0; i < l; ++i) {
                    this.sourceConnectorRecord.set(i, this.sourceTransformer[this.sourceMapping[i]].transform(values[this.sourceMapping[i]]));
                }
            } else {
                for (int l = this.sourceTransformer.length, i = 0; i < l; ++i) {
                    this.sourceConnectorRecord.set(i, this.sourceTransformer[i].transform(values[i]));
                }
            }
        } else {
            for (int j = 0; j < values.length; ++j) {
                if (this.sourceDataTypes == null || HdfsSchemaUtils.isCharType(this.sourceDataTypes[j])) {
                    if (values[j].isEmpty() || (!this.sourceNullString.isEmpty() && values[j].equals(this.sourceNullString))) {
                        this.sourceConnectorRecord.set(j, null);
                    } else {
                        this.sourceConnectorRecord.set(j, values[j]);
                    }
                } else if (values[j].isEmpty() || (!this.sourceNullNonString.isEmpty() && values[j].equals(this.sourceNullNonString))) {
                    this.sourceConnectorRecord.set(j, null);
                } else {
                    this.sourceConnectorRecord.set(j, values[j]);
                }
            }
        }
        return this.sourceConnectorRecord;
    }
}
