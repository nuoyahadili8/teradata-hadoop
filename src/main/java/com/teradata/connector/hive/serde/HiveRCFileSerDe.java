package com.teradata.connector.hive.serde;

import com.teradata.connector.common.ConnectorRecord;
import com.teradata.connector.common.exception.ConnectorException;
import com.teradata.connector.common.utils.ConnectorConfiguration;
import com.teradata.connector.hive.utils.HivePlugInConfiguration;
import com.teradata.connector.hive.utils.HiveSchemaUtils;
import java.util.Arrays;
import org.apache.hadoop.hive.serde2.SerDeException;
import org.apache.hadoop.hive.serde2.columnar.ColumnarSerDe;
import org.apache.hadoop.hive.serde2.columnar.ColumnarSerDeBase;
import org.apache.hadoop.hive.serde2.columnar.ColumnarStructBase;
import org.apache.hadoop.hive.serde2.objectinspector.StructObjectInspector;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.JobContext;

/**
 * @author Administrator
 */
public class HiveRCFileSerDe extends HiveConnectorSerDe {
    protected ColumnarSerDeBase serde=null;
    private StructObjectInspector objectInspector=null;
    protected static ColumnarStructBase cachedColumnarStruct;


    @Override
    public void initialize(final JobContext context, final ConnectorConfiguration.direction direction) throws ConnectorException {
        super.initialize(context, direction);
        if (direction.equals(ConnectorConfiguration.direction.input)) {
            final String sourceTableSchema = HivePlugInConfiguration.getInputTableSchema(this.configuration);
            try {
                this.serde = HiveSchemaUtils.initializeColumnarSerDe(this.configuration, sourceTableSchema, direction);
                if (this.serde instanceof ColumnarSerDe) {
                    this.inputSchemaDataTypes = HiveSchemaUtils.lookupLazyHiveDataTypes(HivePlugInConfiguration.getInputTableFieldTypes(this.configuration));
                }
                this.objectInspector = HiveSchemaUtils.createStructObjectInspector(sourceTableSchema);
            } catch (ConnectorException e) {
                e.printStackTrace();
            }
        } else {
            final String targetTableSchema = HivePlugInConfiguration.getOutputTableSchema(this.configuration);
            try {
                this.serde = HiveSchemaUtils.initializeColumnarSerDe(this.configuration, targetTableSchema, direction);
                this.objectInspector = HiveSchemaUtils.createStructObjectInspector(targetTableSchema);
            } catch (ConnectorException e) {
                e.printStackTrace();
            }
        }
    }

    @Override
    protected Writable doSerialize(final ConnectorRecord r) throws SerDeException {
        return this.serde.serialize(Arrays.asList(r.getAllObject()), this.objectInspector);
    }

    @Override
    protected ConnectorRecord doDeserialize(final Writable w) throws SerDeException {
        HiveRCFileSerDe.cachedColumnarStruct = (ColumnarStructBase) this.serde.deserialize(w);
        for (int i = 0; i < this.nonPartSize; ++i) {
            try {
                this.deserRecord.set(i, this.inputSchemaDataTypes[i].transform(HiveRCFileSerDe.cachedColumnarStruct.getField(i)));
            } catch (ConnectorException e) {
                e.printStackTrace();
            }
        }
        return this.deserRecord;
    }

    static {
        HiveRCFileSerDe.cachedColumnarStruct = null;
    }
}
