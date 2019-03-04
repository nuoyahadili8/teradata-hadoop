package com.teradata.connector.hive.serde;

import com.teradata.connector.common.ConnectorRecord;
import com.teradata.connector.common.exception.ConnectorException;
import com.teradata.connector.common.utils.ConnectorConfiguration;
import com.teradata.connector.common.utils.ConnectorSchemaUtils;
import com.teradata.connector.hive.utils.HivePlugInConfiguration;
import com.teradata.connector.hive.utils.HiveSchemaUtils;
import com.teradata.connector.hive.utils.HiveUtils;

import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Properties;

import org.apache.avro.generic.GenericData;
import org.apache.avro.util.Utf8;
import org.apache.hadoop.hive.serde2.SerDeException;
import org.apache.hadoop.hive.serde2.avro.AvroGenericRecordWritable;
import org.apache.hadoop.hive.serde2.avro.AvroSerDe;
import org.apache.hadoop.hive.serde2.lazy.LazyStruct;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.StructObjectInspector;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.ObjectWritable;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.JobContext;


public class HiveAvroFileSerDe extends HiveConnectorSerDe {
    protected static LazyStruct cachedLazyStruct;
    protected StructObjectInspector structInspector;
    protected AvroSerDe serde;

    public HiveAvroFileSerDe() {
        this.structInspector = null;
        this.serde = null;
    }

    @Override
    public void initialize(final JobContext context, final ConnectorConfiguration.direction direction) throws ConnectorException {
        super.initialize(context, direction);
        if (direction.equals(ConnectorConfiguration.direction.input)) {
            final String sourceTableSchema = HivePlugInConfiguration.getInputTableSchema(this.configuration);
            final List<String> columns = ConnectorSchemaUtils.parseColumns(sourceTableSchema.toLowerCase());
            final List<String> columnNames = ConnectorSchemaUtils.parseColumnNames(columns);
            final List<String> columnTypes = ConnectorSchemaUtils.parseColumnTypes(columns);
            HiveSchemaUtils.updateNonScaleDecimalColumns(columnTypes);
            this.serde = new AvroSerDe();
            final Properties props = new Properties();
            props.setProperty("columns", HiveUtils.listToString(columnNames));
            props.setProperty("columns.types", HiveUtils.listToString(columnTypes));
            try {
                this.serde.initialize(this.configuration, props);
            } catch (SerDeException e) {
                throw new ConnectorException(e.getMessage(), (Throwable) e);
            }
        } else {
            final String targetTableSchema = HivePlugInConfiguration.getOutputTableSchema(context.getConfiguration());
            final List<String> columns = ConnectorSchemaUtils.parseColumns(targetTableSchema.toLowerCase());
            final List<String> columnNames = ConnectorSchemaUtils.parseColumnNames(columns);
            final List<String> columnTypes = ConnectorSchemaUtils.parseColumnTypes(columns);
            final Properties props = new Properties();
            props.setProperty("columns", HiveUtils.listToString(columnNames));
            props.setProperty("columns.types", HiveUtils.listToString(columnTypes));
            this.serde = new AvroSerDe();
            try {
                this.serde.initialize(context.getConfiguration(), props);
                this.structInspector = (StructObjectInspector) this.serde.getObjectInspector();
            } catch (SerDeException e) {
                throw new ConnectorException(e.getMessage());
            }
        }
    }

    @Override
    protected Writable doSerialize(final ConnectorRecord r) throws SerDeException {
        try {
            return this.serde.serialize((Object) Arrays.asList(r.getAllObject()), (ObjectInspector) this.structInspector);
        } catch (Exception e) {
            e.printStackTrace();
            return null;
        }
    }

    public static final Object getDataTypes(final Object object) {
        if (object == null) {
            return null;
        }
        if (object instanceof Integer) {
            return object;
        }
        if (object instanceof Double) {
            return object;
        }
        if (object instanceof Long) {
            return object;
        }
        if (object instanceof Float) {
            return object;
        }
        if (object instanceof Utf8) {
            return object.toString();
        }
        if (object instanceof ByteBuffer) {
            final ByteBuffer objByteBuffer = (ByteBuffer) object;
            final byte[] byteArray = new byte[objByteBuffer.remaining()];
            if (byteArray.length == 0 || objByteBuffer == null) {
                return null;
            }
            return objByteBuffer.array();
        } else {
            if (object instanceof HashMap) {
                return object.toString().replace("=", ":");
            }
            if (object instanceof GenericData.Array) {
                return object.toString().replace("=", ":");
            }
            if (object instanceof GenericData.EnumSymbol) {
                return object.toString();
            }
            if (object instanceof NullWritable) {
                return null;
            }
            return object;
        }
    }

    @Override
    protected ConnectorRecord doDeserialize(final Writable w) throws SerDeException {
        final Object objectwritable = ((ObjectWritable) w).get();
        final AvroGenericRecordWritable value = (AvroGenericRecordWritable) objectwritable;
        for (int i = 0; i < this.nonPartSize; ++i) {
            try {
                this.deserRecord.set(i, getDataTypes(value.getRecord().get(i)));
            } catch (ConnectorException e) {
                e.printStackTrace();
            }
        }
        return this.deserRecord;
    }

    static {
        HiveAvroFileSerDe.cachedLazyStruct = null;
    }
}
