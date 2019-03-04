package com.teradata.connector.hive.serde;

import com.teradata.connector.common.ConnectorRecord;
import com.teradata.connector.common.exception.ConnectorException;
import com.teradata.connector.common.utils.ConnectorConfiguration;
import com.teradata.connector.hive.utils.HivePlugInConfiguration;
import com.teradata.connector.hive.utils.HiveSchemaUtils;

import java.util.Arrays;

import org.apache.hadoop.hive.serde2.SerDeException;
import org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe;
import org.apache.hadoop.hive.serde2.lazy.LazyStruct;
import org.apache.hadoop.hive.serde2.objectinspector.StructObjectInspector;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.JobContext;

/**
 * @author Administrator
 */
public class HiveTextFileSerDe extends HiveConnectorSerDe {
    private LazySimpleSerDe serde;
    private StructObjectInspector objectInspector;
    protected static LazyStruct cachedLazyStruct;

    public HiveTextFileSerDe() {
        this.serde = null;
        this.objectInspector = null;
    }

    @Override
    public void initialize(final JobContext context, final ConnectorConfiguration.direction direction) throws ConnectorException {
        super.initialize(context, direction);
        if (direction.equals(ConnectorConfiguration.direction.input)) {
            final String sourceTableSchema = HivePlugInConfiguration.getInputTableSchema(this.configuration);
            this.serde = HiveSchemaUtils.initializeLazySimpleSerde(this.configuration, sourceTableSchema, direction);
            this.inputSchemaDataTypes = HiveSchemaUtils.lookupLazyHiveDataTypes(HivePlugInConfiguration.getInputTableFieldTypes(this.configuration));
        } else {
            final String targetTableSchema = HivePlugInConfiguration.getOutputTableSchema(this.configuration);
            this.serde = HiveSchemaUtils.initializeLazySimpleSerde(this.configuration, targetTableSchema, direction);
            this.objectInspector = HiveSchemaUtils.createStructObjectInspector(targetTableSchema);
        }
    }

    @Override
    protected Writable doSerialize(final ConnectorRecord r) throws SerDeException {
        return this.serde.serialize(Arrays.asList(r.getAllObject()), this.objectInspector);
    }

    @Override
    protected ConnectorRecord doDeserialize(final Writable w) throws SerDeException {
        HiveTextFileSerDe.cachedLazyStruct = (LazyStruct) this.serde.deserialize(w);
        for (int i = 0; i < this.nonPartSize; ++i) {
            try {
                this.deserRecord.set(i, this.inputSchemaDataTypes[i].transform(HiveTextFileSerDe.cachedLazyStruct.getField(i)));
            } catch (ConnectorException e) {
                e.printStackTrace();
            }
        }
        return this.deserRecord;
    }

    static {
        HiveTextFileSerDe.cachedLazyStruct = null;
    }
}
