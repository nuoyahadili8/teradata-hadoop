package com.teradata.connector.hdfs.converter;

import com.teradata.connector.hdfs.converter.HdfsAvroDataTypeConverter.AvroObjectToJsonString;
import java.math.BigDecimal;
import java.nio.ByteBuffer;
import java.sql.Date;
import java.sql.Time;
import java.sql.Timestamp;
import java.util.HashMap;
import java.util.List;
import org.apache.avro.Schema;
import org.apache.avro.Schema.Field;
import org.apache.avro.generic.GenericData.Array;
import org.apache.avro.generic.GenericData.EnumSymbol;
import org.apache.avro.generic.GenericData.Fixed;
import org.apache.avro.generic.GenericData.Record;


/**
 * @author Administrator
 */
public class HdfsAvroDataTypeDefinition {
    public static final String ARRAY_TD_NULL_VALUE = "";
    public static final BigDecimal BIGDECIMAL_FALSE_VALUE = BigDecimal.valueOf(0);
    public static final BigDecimal BIGDECIMAL_NULL_VALUE = new BigDecimal(0);
    public static final BigDecimal BIGDECIMAL_TRUE_VALUE = BigDecimal.valueOf(1);
    public static final Long BIGINT_NULL_VALUE = Long.valueOf(0);
    public static final Boolean BOOLEAN_NULL_VALUE = Boolean.valueOf(false);
    public static final Byte BYTE_FALSE_VALUE = Byte.valueOf((byte) 0);
    public static final ByteBuffer BYTE_NULL_VALUE = ByteBuffer.allocate(0);
    public static final Byte BYTE_TRUE_VALUE = Byte.valueOf((byte) 1);
    public static final Date DATE_NULL_VALUE = Date.valueOf("1999-12-31");
    public static final Double DOUBLE_FALSE_VALUE = Double.valueOf(0.0d);
    public static final Double DOUBLE_NULL_VALUE = Double.valueOf(0.0d);
    public static final Double DOUBLE_TRUE_VALUE = Double.valueOf(1.0d);
    public static final Float FLOAT_FALSE_VALUE = Float.valueOf(0.0f);
    public static final Float FLOAT_NULL_VALUE = Float.valueOf(0.0f);
    public static final Float FLOAT_TRUE_VALUE = Float.valueOf(1.0f);
    public static final Integer INTEGER_FALSE_VALUE = Integer.valueOf(0);
    public static final Integer INTEGER_NULL_VALUE = Integer.valueOf(0);
    public static final Integer INTEGER_TRUE_VALUE = Integer.valueOf(1);
    public static final String INTERVAL_TD_NULL_VALUE = "";
    public static final Long LONG_FALSE_VALUE = Long.valueOf(0);
    public static final Long LONG_TRUE_VALUE = Long.valueOf(1);
    public static final String PERIOD_TD_NULL_VALUE = "";
    public static final Short SHORT_FALSE_VALUE = Short.valueOf((short) 0);
    public static final Short SHORT_TRUE_VALUE = Short.valueOf((short) 1);
    public static final Short SMALLINT_NULL_VALUE = Short.valueOf((short) 0);
    public static final String STRING_NULL_VALUE = "";
    public static final Timestamp TIMESTAMP_NULL_VALUE = Timestamp.valueOf("1999-12-31 00:00:00");
    public static final Time TIME_NULL_VALUE = Time.valueOf("00:00:00");
    public static final Byte TINYINT_NULL_VALUE = Byte.valueOf((byte) 0);
    public static final int TYPE_AVRO_ARRAY = -2003;
    public static final int TYPE_AVRO_ENUM = -2002;
    public static final int TYPE_AVRO_FIXED = -2006;
    public static final int TYPE_AVRO_MAP = -2004;
    public static final int TYPE_AVRO_NULL = -2000;
    public static final int TYPE_AVRO_RECORD = -2001;
    public static final int TYPE_AVRO_UNION = -2005;


    public static int getAvroDataType(Schema s) {
        String typeName = s.getType().name().toLowerCase();
        if (typeName.equals("null")) {
            return TYPE_AVRO_NULL;
        }
        if (typeName.equals("enum")) {
            return TYPE_AVRO_ENUM;
        }
        if (typeName.equals("array")) {
            return TYPE_AVRO_ARRAY;
        }
        if (typeName.equals("map")) {
            return TYPE_AVRO_MAP;
        }
        if (typeName.equals("union")) {
            return TYPE_AVRO_UNION;
        }
        if (typeName.equals("fixed")) {
            return TYPE_AVRO_FIXED;
        }
        if (typeName.equals("record")) {
            return -2001;
        }
        if (typeName.equals("string")) {
            return 12;
        }
        if (typeName.equals("bytes")) {
            return -2;
        }
        if (typeName.equals("int")) {
            return 4;
        }
        if (typeName.equals("long")) {
            return -5;
        }
        if (typeName.equals("float")) {
            return 6;
        }
        if (typeName.equals("double")) {
            return 8;
        }
        if (typeName.equals("boolean")) {
            return 16;
        }
        return 0;
    }

    public static String getAvroDefaultNullJson(final Schema s) {
        final Object defNul = getAvroDefaultNullValue(s);
        if (defNul == null) {
            return "";
        }
        return (String) new HdfsAvroDataTypeConverter.AvroObjectToJsonString(s).convert(defNul);
    }

    public static Object getAvroDefaultNullValue(final Schema s) {
        String typeName = s.getType().name().toLowerCase();
        if (typeName.equals("enum")) {
            return new EnumSymbol(s, (String) s.getEnumSymbols().get(0));
        }
        if (typeName.equals("array")) {
            return new Array(0, s);
        }
        if (typeName.equals("map")) {
            return new HashMap(0);
        }
        if (typeName.equals("union")) {
            return getAvroDefaultNullValue((Schema) s.getTypes().get(0));
        }
        if (typeName.equals("fixed")) {
            return new Fixed(s);
        }
        if (typeName.equals("string")) {
            return "";
        }
        if (typeName.equals("bytes")) {
            return ByteBuffer.allocate(0);
        }
        if (typeName.equals("int")) {
            return INTEGER_NULL_VALUE;
        }
        if (typeName.equals("long")) {
            return BIGINT_NULL_VALUE;
        }
        if (typeName.equals("float")) {
            return FLOAT_NULL_VALUE;
        }
        if (typeName.equals("double")) {
            return DOUBLE_NULL_VALUE;
        }
        if (typeName.equals("boolean")) {
            return BOOLEAN_NULL_VALUE;
        }
        if (!typeName.equals("record")) {
            return null;
        }
        Record r = new Record(s);
        List<Field> fields = s.getFields();
        for (int i = 0; i < fields.size(); i++) {
            Field f = (Field) fields.get(i);
            r.put(f.name(), getAvroDefaultNullValue(f.schema()));
        }
        return r;
    }
}
