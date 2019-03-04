package com.teradata.connector.hive.serde;

import com.teradata.connector.common.ConnectorRecord;
import com.teradata.connector.common.exception.ConnectorException;
import com.teradata.connector.common.utils.ConnectorConfiguration;
import com.teradata.connector.common.utils.ConnectorConfiguration.direction;
import com.teradata.connector.common.utils.ConnectorSchemaUtils;
import com.teradata.connector.hive.utils.HivePlugInConfiguration;
import com.teradata.connector.hive.utils.HiveSchemaUtils;
import com.teradata.connector.hive.utils.HiveUtils;
import java.io.IOException;
import java.util.List;
import java.util.Properties;
import org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe;
import org.apache.hadoop.hive.serde2.SerDeException;
import org.apache.hadoop.hive.serde2.io.ByteWritable;
import org.apache.hadoop.hive.serde2.io.DoubleWritable;
import org.apache.hadoop.hive.serde2.io.ShortWritable;
import org.apache.hadoop.hive.serde2.lazy.LazyStruct;
import org.apache.hadoop.hive.serde2.objectinspector.StructField;
import org.apache.hadoop.hive.serde2.objectinspector.StructObjectInspector;
import org.apache.hadoop.io.ArrayWritable;
import org.apache.hadoop.io.BooleanWritable;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.JobContext;


/**
 * @author Administrator
 */
public class HiveParquetFileSerDe extends HiveConnectorSerDe {
    protected static LazyStruct cachedLazyStruct;
    protected StructObjectInspector structInspector=null;
    protected ParquetHiveSerDe serde=null;

    @Override
    public void initialize(final JobContext context, final ConnectorConfiguration.direction direction) throws ConnectorException {
        super.initialize(context, direction);
        if (direction.equals(ConnectorConfiguration.direction.input)) {
            final String sourceTableSchema = HivePlugInConfiguration.getInputTableSchema(this.configuration);
            final List<String> columns = ConnectorSchemaUtils.parseColumns(sourceTableSchema.toLowerCase());
            final List<String> columnNames = ConnectorSchemaUtils.parseColumnNames(columns);
            final List<String> columnTypes = ConnectorSchemaUtils.parseColumnTypes(columns);
            HiveSchemaUtils.updateNonScaleDecimalColumns(columnTypes);
            this.serde = new ParquetHiveSerDe();
            final Properties props = new Properties();
            props.setProperty("columns", HiveUtils.listToString(columnNames));
            props.setProperty("columns.types", HiveUtils.listToString(columnTypes));
            try {
                this.serde.initialize(this.configuration, props);
                this.structInspector = (StructObjectInspector) this.serde.getObjectInspector();
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
            this.serde = new ParquetHiveSerDe();
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
            final String targetTableSchema = HivePlugInConfiguration.getOutputTableSchema(this.context.getConfiguration());
            final List<String> columns = ConnectorSchemaUtils.parseColumns(targetTableSchema.toLowerCase());
            return this.serde.serialize(new ArrayWritable(Writable.class, r.getAllWritableObject(columns)), this.structInspector);
        } catch (IOException e) {
            e.printStackTrace();
            return null;
        }
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
        if (object instanceof FloatWritable) {
            return ((FloatWritable) object).get();
        }
        if (object instanceof DoubleWritable) {
            return ((DoubleWritable) object).get();
        }
        if (object instanceof Text) {
            return object.toString();
        }
        if (object instanceof BytesWritable) {
            return ((BytesWritable) object).getBytes();
        }
        if (object instanceof NullWritable) {
            return null;
        }
        return object.toString();
    }

    @Override
    protected ConnectorRecord doDeserialize(final Writable w) throws SerDeException {
        final ArrayWritable value = (ArrayWritable) w;
        final List<? extends StructField> fields = (List<? extends StructField>) this.structInspector.getAllStructFieldRefs();
        final Object row = this.serde.deserialize((Writable) value);
        for (int i = 0; i < this.nonPartSize; ++i) {
            final Object obj = this.structInspector.getStructFieldData(row, (StructField) fields.get(i));
            try {
                this.deserRecord.set(i, getWritableToPrimitiveTypes(obj));
            } catch (ConnectorException e) {
                e.printStackTrace();
            }
        }
        return this.deserRecord;
    }

    static {
        HiveParquetFileSerDe.cachedLazyStruct = null;
    }
}
