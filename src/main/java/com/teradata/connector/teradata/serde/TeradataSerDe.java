package com.teradata.connector.teradata.serde;

import com.teradata.connector.common.ConnectorRecord;
import com.teradata.connector.common.ConnectorSerDe;
import com.teradata.connector.common.exception.ConnectorException;
import com.teradata.connector.common.utils.ConnectorConfiguration;
import com.teradata.connector.teradata.TeradataObjectArrayWritable;
import com.teradata.connector.teradata.converter.TeradataDataType;
import com.teradata.connector.teradata.schema.TeradataTableDesc;
import com.teradata.connector.teradata.utils.TeradataPlugInConfiguration;
import com.teradata.connector.teradata.utils.TeradataSchemaUtils;

import java.util.HashMap;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.JobContext;


/**
 * @author Administrator
 */
public class TeradataSerDe implements ConnectorSerDe {
    TeradataObjectArrayWritable objectArrayWritable;
    String targetTableDescText;
    String sourceTableDescText;
    TeradataTableDesc targetTableDesc;
    String[] targetFieldNamesArray;
    String[] sourceFieldNamesArray;
    TeradataDataType[] inputSchemaDataTypes;
    int[] targetMappings;
    Object[] targetObjects;
    boolean sqlXmlExists;
    Configuration conf;

    public TeradataSerDe() {
        this.objectArrayWritable = null;
        this.targetTableDescText = "";
        this.sourceTableDescText = "";
        this.targetTableDesc = null;
        this.targetFieldNamesArray = null;
        this.sourceFieldNamesArray = null;
        this.inputSchemaDataTypes = null;
        this.targetMappings = null;
        this.targetObjects = null;
        this.sqlXmlExists = false;
    }

    @Override
    public void initialize(final JobContext context, final ConnectorConfiguration.direction direction) throws ConnectorException {
        Configuration configuration = context.getConfiguration();
        this.conf = context.getConfiguration();
        this.sourceFieldNamesArray = TeradataPlugInConfiguration.getInputFieldNamesArray(configuration);
        this.sourceTableDescText = TeradataPlugInConfiguration.getInputTableDesc(configuration);
        HashMap<String, Object> retHashMap = new HashMap();
        retHashMap = TeradataSchemaUtils.lookupTeradataDataTypes(this.sourceTableDescText, this.sourceFieldNamesArray);
        this.inputSchemaDataTypes = (TeradataDataType[]) retHashMap.get("TeradataDataType");
        this.sqlXmlExists = Boolean.parseBoolean(retHashMap.get("SQLXMLEXISTS").toString());
        if (direction == ConnectorConfiguration.direction.output) {
            this.targetTableDescText = TeradataPlugInConfiguration.getOutputTableDesc(configuration);
            this.targetTableDesc = TeradataSchemaUtils.tableDescFromText(this.targetTableDescText);
            this.targetFieldNamesArray = TeradataPlugInConfiguration.getOutputFieldNamesArray(configuration);
            int[] fieldTypes = new int[0];
            int[] fieldScales = new int[0];
            fieldTypes = TeradataSchemaUtils.lookupTypesFromTableDescText(this.targetTableDescText, this.targetFieldNamesArray);
            String[] fieldTypeNames = TeradataSchemaUtils.lookupTypeNamesFromTableDescText(this.targetTableDescText, this.targetFieldNamesArray);
            fieldScales = TeradataSchemaUtils.lookupFieldsSQLDataScales(this.targetTableDescText, this.targetFieldNamesArray);
            this.targetMappings = TeradataSchemaUtils.lookupMappingFromTableDescText(this.targetTableDescText, this.targetFieldNamesArray);
            this.objectArrayWritable = new TeradataObjectArrayWritable();
            this.objectArrayWritable.setNullJdbcTypes(fieldTypes);
            this.objectArrayWritable.setNullJdbcScales(fieldScales);
            this.objectArrayWritable.setRecordTypes(fieldTypeNames);
            int recordLength = this.targetTableDesc.getColumns().length;
            this.targetObjects = new Object[recordLength];
            for (int i = 0; i < recordLength; i++) {
                this.targetObjects[i] = null;
            }
        }
    }

    @Override
    public ConnectorRecord deserialize(final Writable writable) throws ConnectorException {
        final ConnectorRecord connectorRecord = (ConnectorRecord) writable;
        final TeradataDataType[] teradataDataType = this.inputSchemaDataTypes;
        final int recordLength = this.sourceFieldNamesArray.length;
        if (this.sqlXmlExists) {
            for (int i = 0; i < recordLength; ++i) {
                if (teradataDataType[i].equals(TeradataDataType.TeradataDataTypeImpl.SQLXML)) {
                    connectorRecord.set(i, teradataDataType[i].transform(connectorRecord.get(i)));
                }
            }
        }
        return connectorRecord;
    }

    @Override
    public Writable serialize(final ConnectorRecord connectorRecord) throws ConnectorException {
        int index = 0;
        for (final int position : this.targetMappings) {
            if (ConnectorConfiguration.getEnableHdfsLoggingFlag(this.conf) && connectorRecord.get(index) != null && connectorRecord.get(index).equals("skip")) {
                return null;
            }
            this.targetObjects[position] = connectorRecord.get(index++);
        }
        this.objectArrayWritable.setObjects(this.targetObjects);
        return (Writable) this.objectArrayWritable;
    }
}
