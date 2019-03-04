package com.teradata.connector.sample.plugin.serde;

import com.teradata.connector.common.ConnectorRecord;
import com.teradata.connector.common.ConnectorSerDe;
import com.teradata.connector.common.exception.ConnectorException;
import com.teradata.connector.common.utils.ConnectorConfiguration;
import com.teradata.connector.sample.CommonDBObjectArrayWritable;
import com.teradata.connector.sample.plugin.utils.CommonDBConfiguration;
import com.teradata.connector.sample.plugin.utils.CommonDBSchemaUtils;
import com.teradata.connector.teradata.schema.TeradataTableDesc;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.JobContext;


public class CommonDBSerDe implements ConnectorSerDe {
    CommonDBObjectArrayWritable objectArrayWritable;
    String targetTableDescText;
    TeradataTableDesc targetTableDesc;
    String[] targetFieldNamesArray;
    int[] mappings;
    Object[] targetObjects;

    public CommonDBSerDe() {
        this.objectArrayWritable = null;
        this.targetTableDescText = "";
        this.targetTableDesc = null;
        this.targetFieldNamesArray = null;
        this.mappings = null;
        this.targetObjects = null;
    }

    @Override
    public void initialize(final JobContext context, final ConnectorConfiguration.direction direction) throws ConnectorException {
        final Configuration configuration = context.getConfiguration();
        if (direction == ConnectorConfiguration.direction.output) {
            this.targetTableDescText = CommonDBConfiguration.getOutputTableDesc(configuration);
            this.targetTableDesc = CommonDBSchemaUtils.tableDescFromText(this.targetTableDescText);
            this.targetFieldNamesArray = CommonDBConfiguration.getOutputFieldNamesArray(configuration);
            int[] fieldTypes = new int[0];
            int[] fieldScales = new int[0];
            fieldTypes = CommonDBSchemaUtils.lookupTypesFromTableDescText(this.targetTableDescText, this.targetFieldNamesArray);
            fieldScales = CommonDBSchemaUtils.lookupFieldsSQLDataScales(this.targetTableDescText, this.targetFieldNamesArray);
            this.mappings = CommonDBSchemaUtils.lookupMappingFromTableDescText(this.targetTableDescText, this.targetFieldNamesArray);
            (this.objectArrayWritable = new CommonDBObjectArrayWritable()).setNullJdbcTypes(fieldTypes);
            this.objectArrayWritable.setNullJdbcScales(fieldScales);
            final int recordLength = this.targetTableDesc.getColumns().length;
            this.targetObjects = new Object[recordLength];
            for (int i = 0; i < recordLength; ++i) {
                this.targetObjects[i] = null;
            }
        }
    }

    @Override
    public ConnectorRecord deserialize(final Writable writable) throws ConnectorException {
        return (ConnectorRecord) writable;
    }

    @Override
    public Writable serialize(final ConnectorRecord connectorRecord) throws ConnectorException {
        int index = 0;
        for (final int position : this.mappings) {
            this.targetObjects[position] = connectorRecord.get(index++);
        }
        this.objectArrayWritable.setObjects(this.targetObjects);
        return (Writable) this.objectArrayWritable;
    }
}
