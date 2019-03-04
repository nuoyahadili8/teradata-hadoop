package com.teradata.connector.hive;

import com.teradata.connector.common.exception.ConnectorException;
import com.teradata.connector.common.utils.ConnectorSchemaUtils;
import com.teradata.connector.hive.utils.HivePlugInConfiguration;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.serde2.io.ByteWritable;
import org.apache.hadoop.hive.serde2.io.DoubleWritable;
import org.apache.hadoop.hive.serde2.io.HiveDecimalWritable;
import org.apache.hadoop.hive.serde2.io.ShortWritable;
import org.apache.hadoop.io.*;

import java.util.List;
import java.util.regex.Pattern;

/**
 * @author Administrator
 */
public class HiveParquetPlainToWritable {
    protected static final String PATTERN_VARCHAR_TYPE = "\\s*VARCHAR\\s*\\(\\s*(\\d+)\\s*\\)";
    protected static final String PATTERN_CHAR_TYPE = "\\s*CHAR\\s*\\(\\s*(\\d+)\\s*\\)";

    ArrayWritable getArrayWritable(final HiveParquetReadSupportExt.PlainParquetExt value, final Configuration configuration) throws ConnectorException {
        final String sourceTableSchema = HivePlugInConfiguration.getInputTableSchema (configuration);
        final List<String> columns = ConnectorSchemaUtils.parseColumns (sourceTableSchema.toLowerCase ());
        final List<String> columnNames = ConnectorSchemaUtils.parseColumnNames (columns);
        final List<String> columnTypes = ConnectorSchemaUtils.parseColumnTypes (columns);
        final int length = columnNames.size ();
        final HiveParquetReadSupportExt.PlainParquetExt PlainParquetExtValue = value;
        final Writable[] arr = new Writable[length];
        boolean nullCheckFlag = true;
        for (int i = 0; i < length; ++i) {
            final String typeName = columnTypes.get (i).toUpperCase ();

            String strVal = "null";
            if (nullCheckFlag) {
                try {
                    strVal = PlainParquetExtValue.getString (i, 0);
                } catch (Exception e) {
                    i -= 1;
                    nullCheckFlag = false;
                    continue;
                }
            }
            if (strVal == null) {
                arr[i] = NullWritable.get ();
            } else if (typeName.equals ("INT") || typeName.equals ("INTEGER")) {
                arr[i] = (Writable) new IntWritable (PlainParquetExtValue.getInteger (i, 0));
            } else if (typeName.equals ("BIGINT") || typeName.equals ("LONG")) {
                arr[i] = (Writable) new LongWritable (PlainParquetExtValue.getLong (i, 0));
            } else if (typeName.equals ("SMALLINT")) {
                arr[i] = (Writable) new ShortWritable ((short) PlainParquetExtValue.getInteger (i, 0));
            } else if (typeName.equals ("TINYINT")) {
                final Integer in = new Integer (PlainParquetExtValue.getInteger (i, 0));
                arr[i] = (Writable) new ByteWritable (in.byteValue ());
            } else if (typeName.equals ("STRING")) {
                arr[i] = (Writable) new Text (PlainParquetExtValue.getString (i, 0));
            } else if (Pattern.matches ("\\s*VARCHAR\\s*\\(\\s*(\\d+)\\s*\\)", typeName)) {
                arr[i] = (Writable) new Text (PlainParquetExtValue.getString (i, 0));
            } else if (Pattern.matches ("\\s*CHAR\\s*\\(\\s*(\\d+)\\s*\\)", typeName)) {
                arr[i] = (Writable) new Text (PlainParquetExtValue.getString (i, 0));
            } else if (typeName.equals ("DOUBLE") || typeName.equals ("DOUBLE PRECISION")) {
                arr[i] = (Writable) new DoubleWritable (PlainParquetExtValue.getDouble (i, 0));
            } else if (typeName.equals ("BOOLEAN")) {
                arr[i] = (Writable) new BooleanWritable (PlainParquetExtValue.getBoolean (i, 0));
            } else if (typeName.equals ("FLOAT")) {
                arr[i] = (Writable) new FloatWritable (PlainParquetExtValue.getFloat (i, 0));
            }
            /***add by anliang ***/
            else if (typeName.contains ("DECIMAL")) {
                int scale = 0;
                if (typeName.contains (",")) {
                    String strScale = typeName.substring (typeName.indexOf (",") + 1, typeName.lastIndexOf (")"));
                    scale = Integer.parseInt (strScale.trim ());
                }
                arr[i] = new HiveDecimalWritable (PlainParquetExtValue.getBinary (i, 0).getBytes (), scale);
            } else if (typeName.equals ("BINARY")) {
                arr[i] = new BytesWritable (PlainParquetExtValue.getBinary (i, 0).getBytes ());
            } else {
                throw new ConnectorException (14006, new Object[]{typeName});
            }
            nullCheckFlag = true;
        }
        final ArrayWritable arrWritable = new ArrayWritable ((Class) Writable.class, arr);
        return arrWritable;
    }
}