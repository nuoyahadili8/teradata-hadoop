package com.teradata.connector.hive.serde;

import com.teradata.connector.common.ConnectorRecord;
import com.teradata.connector.common.exception.ConnectorException;
import com.teradata.connector.common.utils.ConnectorConfiguration;
import com.teradata.connector.common.utils.ConnectorSchemaUtils;
import com.teradata.connector.hive.utils.HivePlugInConfiguration;
import com.teradata.connector.hive.utils.HiveSchemaUtils;
import com.teradata.connector.hive.utils.HiveUtils;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import org.apache.hadoop.hive.ql.io.orc.OrcSerde;
import org.apache.hadoop.hive.serde2.SerDeException;
import org.apache.hadoop.hive.serde2.io.ByteWritable;
import org.apache.hadoop.hive.serde2.io.DoubleWritable;
import org.apache.hadoop.hive.serde2.io.HiveDecimalWritable;
import org.apache.hadoop.hive.serde2.io.ShortWritable;
import org.apache.hadoop.hive.serde2.io.TimestampWritable;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.StructField;
import org.apache.hadoop.hive.serde2.objectinspector.StructObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorUtils;
import org.apache.hadoop.io.BooleanWritable;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.ObjectWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.JobContext;

/**
 * @author Administrator
 */
public class HiveORCFileSerDe extends HiveConnectorSerDe {
    protected StructObjectInspector structInspector = null;
    protected OrcSerde serde = null;

    public HiveORCFileSerDe() {
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
            this.serde = new OrcSerde();
            final Properties props = new Properties();
            props.setProperty("columns", HiveUtils.listToString(columnNames));
            props.setProperty("columns.types", HiveUtils.listToString(columnTypes));
            this.serde.initialize(this.configuration, props);
            try {
                this.structInspector = (StructObjectInspector) this.serde.getObjectInspector();
            } catch (SerDeException e) {
                throw new ConnectorException(e.getMessage(), e);
            }
        } else {
            this.serde = new OrcSerde();
            final String targetTableSchema = HivePlugInConfiguration.getOutputTableSchema(this.configuration);
            this.structInspector = HiveSchemaUtils.createStructObjectInspector(targetTableSchema);
        }
    }

    @Override
    protected Writable doSerialize(final ConnectorRecord r) throws SerDeException {
        return this.serde.serialize(Arrays.asList(r.getAllObject()), this.structInspector);
    }

    public ObjectInspector getObjectInspector(final String filedtype) {
        return PrimitiveObjectInspectorFactory.getPrimitiveJavaObjectInspector(PrimitiveObjectInspectorUtils.getTypeEntryFromTypeName(filedtype).primitiveCategory);
    }

    public static final Object getWritableToPrimitiveTypes(final Object object) {
        if (object == null) {
            return null;
        }
        if (object instanceof BooleanWritable) {
            return ((BooleanWritable) object).get();
        }
        if (object instanceof ByteWritable) {
            return ((ByteWritable) object).get();
        }
        if (object instanceof ShortWritable) {
            return ((ShortWritable) object).get();
        }
        if (object instanceof LongWritable) {
            return ((LongWritable) object).get();
        }
        if (object instanceof IntWritable) {
            return ((IntWritable) object).get();
        }
        if (object instanceof HiveDecimalWritable) {
            return ((HiveDecimalWritable) object).getHiveDecimal().bigDecimalValue();
        }
        if (object instanceof FloatWritable) {
            return ((FloatWritable) object).get();
        }
        if (object instanceof DoubleWritable) {
            return ((DoubleWritable) object).get();
        }
        if (object instanceof Text) {
            return ((Text) object).toString();
        }
        if (object instanceof Timestamp) {
            return object;
        }
        if (object instanceof TimestampWritable) {
            return ((TimestampWritable) object).getTimestamp();
        }
        if (object instanceof BytesWritable) {
            return ((BytesWritable) object).getBytes();
        }
        if (object instanceof HashMap) {
            return parseMap(object);
        }
        if (object instanceof ArrayList) {
            return parseList(object);
        }
        return object.toString();
    }

    private static final Object parseMap(final Object obj) {
        final Map<Object, Object> map = (Map<Object, Object>) obj;
        final Map<Object, Object> newMap = new HashMap<>();
        for (final Map.Entry<Object, Object> entry : map.entrySet()) {
            newMap.put(getWritableToPrimitiveTypes(entry.getKey()), getWritableToPrimitiveTypes(entry.getValue()));
        }
        return newMap;
    }

    private static final Object parseList(final Object obj) {
        final ArrayList<Object> list = (ArrayList<Object>) obj;
        final ArrayList<Object> newList = new ArrayList<Object>(list.size());
        for (int i = 0; i < list.size(); ++i) {
            newList.add(getWritableToPrimitiveTypes(list.get(i)));
        }
        return newList;
    }

    @Override
    protected ConnectorRecord doDeserialize(final Writable w) throws SerDeException {
        final ObjectWritable value = (ObjectWritable) w;
        final List<? extends StructField> fields = this.structInspector.getAllStructFieldRefs();
        for (int i = 0; i < this.nonPartSize; ++i) {
            final Object obj = this.structInspector.getStructFieldData(value.get(), (StructField) fields.get(i));
            try {
                this.deserRecord.set(i, getWritableToPrimitiveTypes(obj));
            } catch (ConnectorException e) {
                e.printStackTrace();
            }
        }
        return this.deserRecord;
    }
}
