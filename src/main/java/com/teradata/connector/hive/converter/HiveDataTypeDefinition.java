package com.teradata.connector.hive.converter;

import java.math.BigDecimal;
import java.sql.Date;
import java.sql.Time;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * @author Administrator
 */
public class HiveDataTypeDefinition {
    public static final ArrayList<Object> ARRAY_NULL_VALUE = new ArrayList();
    public static final String ARRAY_TD_NULL_VALUE = "";
    public static final BigDecimal BIGDECIMAL_FALSE_VALUE = BigDecimal.valueOf(0);
    public static final BigDecimal BIGDECIMAL_NULL_VALUE = new BigDecimal(0);
    public static final BigDecimal BIGDECIMAL_TRUE_VALUE = BigDecimal.valueOf(1);
    public static final Long BIGINT_NULL_VALUE = Long.valueOf(0);
    public static final Byte[] BINARY_NULL_VALUE = new Byte[0];
    public static final Boolean BOOLEAN_NULL_VALUE = Boolean.valueOf(false);
    public static final Byte BYTE_FALSE_VALUE = Byte.valueOf((byte) 0);
    public static final byte[] BYTE_NULL_VALUE = new byte[0];
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
    public static final Map<Object, Object> MAP_NULL_VALUE = new HashMap();
    public static final String PERIOD_TD_NULL_VALUE = "";
    public static final Short SHORT_FALSE_VALUE = Short.valueOf((short) 0);
    public static final Short SHORT_TRUE_VALUE = Short.valueOf((short) 1);
    public static final Short SMALLINT_NULL_VALUE = Short.valueOf((short) 0);
    public static final String STRING_NULL_VALUE = "";
    public static final List<Object> STRUCT_NULL_VALUE = new ArrayList();
    public static final Timestamp TIMESTAMP_NULL_VALUE = Timestamp.valueOf("1999-12-31 00:00:00");
    public static final Time TIME_NULL_VALUE = Time.valueOf("00:00:00");
    public static final Byte TINYINT_NULL_VALUE = Byte.valueOf((byte) 0);
    public static final int TYPE_ARRAY = -1001;
    public static final int TYPE_MAP = -1000;
    public static final int TYPE_STRUCT = -1002;
    public static final int TYPE_VARCHAR = -2001;
}
