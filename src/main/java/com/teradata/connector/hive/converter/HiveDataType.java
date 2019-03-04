package com.teradata.connector.hive.converter;

import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.hive.serde2.io.ByteWritable;
import org.apache.hadoop.hive.serde2.io.DateWritable;
import org.apache.hadoop.hive.serde2.io.DoubleWritable;
import org.apache.hadoop.hive.serde2.io.HiveCharWritable;
import org.apache.hadoop.hive.serde2.io.HiveDecimalWritable;
import org.apache.hadoop.hive.serde2.io.HiveVarcharWritable;
import org.apache.hadoop.hive.serde2.io.ShortWritable;
import org.apache.hadoop.hive.serde2.io.TimestampWritable;
import org.apache.hadoop.hive.serde2.lazy.LazyArray;
import org.apache.hadoop.hive.serde2.lazy.LazyBinary;
import org.apache.hadoop.hive.serde2.lazy.LazyBoolean;
import org.apache.hadoop.hive.serde2.lazy.LazyByte;
import org.apache.hadoop.hive.serde2.lazy.LazyDate;
import org.apache.hadoop.hive.serde2.lazy.LazyDouble;
import org.apache.hadoop.hive.serde2.lazy.LazyFloat;
import org.apache.hadoop.hive.serde2.lazy.LazyHiveChar;
import org.apache.hadoop.hive.serde2.lazy.LazyHiveDecimal;
import org.apache.hadoop.hive.serde2.lazy.LazyHiveVarchar;
import org.apache.hadoop.hive.serde2.lazy.LazyInteger;
import org.apache.hadoop.hive.serde2.lazy.LazyLong;
import org.apache.hadoop.hive.serde2.lazy.LazyMap;
import org.apache.hadoop.hive.serde2.lazy.LazyShort;
import org.apache.hadoop.hive.serde2.lazy.LazyString;
import org.apache.hadoop.hive.serde2.lazy.LazyStruct;
import org.apache.hadoop.hive.serde2.lazy.LazyTimestamp;
import org.apache.hadoop.hive.serde2.lazybinary.LazyBinaryArray;
import org.apache.hadoop.hive.serde2.lazybinary.LazyBinaryMap;
import org.apache.hadoop.hive.serde2.lazybinary.LazyBinaryStruct;
import org.apache.hadoop.io.BooleanWritable;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;


/**
 * @author Administrator
 */
public interface HiveDataType {
    int getType();

    Object transform(final Object p0);

    public enum HiveDataTypeLazyImpl implements HiveDataType {
        BOOLEAN(1) {
            @Override
            public int getType() {
                return 16;
            }

            @Override
            public Object transform(final Object object) {
                return (object == null) ? null : Boolean.valueOf(((BooleanWritable) ((LazyBoolean) object).getWritableObject()).get());
            }
        },
        INTEGER(2) {
            @Override
            public final int getType() {
                return 4;
            }

            @Override
            public Object transform(final Object object) {
                return (object == null) ? null : Integer.valueOf(((IntWritable) ((LazyInteger) object).getWritableObject()).get());
            }
        },
        BIGINT(3) {
            @Override
            public final int getType() {
                return -5;
            }

            @Override
            public Object transform(final Object object) {
                return (object == null) ? null : Long.valueOf(((LongWritable) ((LazyLong) object).getWritableObject()).get());
            }
        },
        SMALLINT(4) {
            @Override
            public final int getType() {
                return 5;
            }

            @Override
            public Object transform(final Object object) {
                return (object == null) ? null : Short.valueOf(((ShortWritable) ((LazyShort) object).getWritableObject()).get());
            }
        },
        TINYINT(5) {
            @Override
            public final int getType() {
                return -6;
            }

            @Override
            public Object transform(final Object object) {
                return (object == null) ? null : Byte.valueOf(((ByteWritable) ((LazyByte) object).getWritableObject()).get());
            }
        },
        DECIMAL(6) {
            @Override
            public final int getType() {
                return 3;
            }

            @Override
            public Object transform(final Object object) {
                return (object == null) ? null : ((LazyHiveDecimal) object).getWritableObject().getHiveDecimal().bigDecimalValue();
            }
        },
        FLOAT(7) {
            @Override
            public final int getType() {
                return 6;
            }

            @Override
            public Object transform(final Object object) {
                return (object == null) ? null : Float.valueOf(((FloatWritable) ((LazyFloat) object).getWritableObject()).get());
            }
        },
        BINARY(8) {
            @Override
            public final int getType() {
                return -2;
            }

            @Override
            public Object transform(final Object object) {
                return (object == null) ? new byte[0] : ((BytesWritable) ((LazyBinary) object).getWritableObject()).getBytes();
            }
        },
        TIMESTAMP(9) {
            @Override
            public final int getType() {
                return 93;
            }

            @Override
            public Object transform(final Object object) {
                return (object == null) ? null : ((LazyTimestamp) object).getWritableObject().getTimestamp();
            }
        },
        STRING(10) {
            @Override
            public final int getType() {
                return 12;
            }

            @Override
            public Object transform(final Object object) {
                return (object == null) ? null : ((Text) ((LazyString) object).getWritableObject()).toString();
            }
        },
        DOUBLE(11) {
            @Override
            public final int getType() {
                return 8;
            }

            @Override
            public Object transform(final Object object) {
                return (object == null) ? null : Double.valueOf(((DoubleWritable) ((LazyDouble) object).getWritableObject()).get());
            }
        },
        MAP(12) {
            @Override
            public int getType() {
                return -1000;
            }

            @Override
            public Object transform(final Object object) {
                if (object == null) {
                    return null;
                }
                final Map<Object, Object> lazyObjectsMap = (Map<Object, Object>) ((LazyMap) object).getMap();
                final Map<Object, Object> javaObjectsMap = new HashMap<Object, Object>();
                for (final Map.Entry<Object, Object> entry : lazyObjectsMap.entrySet()) {
                    javaObjectsMap.put(this.parseLazyObject(entry.getKey()), this.parseLazyObject(entry.getValue()));
                }
                return javaObjectsMap;
            }
        },
        ARRAY(13) {
            @Override
            public final int getType() {
                return -1001;
            }

            @Override
            public Object transform(final Object object) {
                if (object == null) {
                    return null;
                }
                final List<Object> lazyObjectsList = (List<Object>) ((LazyArray) object).getList();
                final List<Object> javaObjectsList = new ArrayList<Object>();
                for (final Object item : lazyObjectsList) {
                    javaObjectsList.add(this.parseLazyObject(item));
                }
                return javaObjectsList;
            }
        },
        STRUCT(14) {
            private boolean lazyStructReflectionInitd;
            private Method LazyStructGetFieldsAsListMethod;

            {
                this.lazyStructReflectionInitd = false;
                this.LazyStructGetFieldsAsListMethod = null;
            }

            @Override
            public int getType() {
                return -1002;
            }

            @Override
            public Object transform(final Object object) {
                if (object == null) {
                    return null;
                }
                if (!this.lazyStructReflectionInitd) {
                    this.initializeLazyStructReflection();
                }
                List<Object> lazyObjectsList = null;
                try {
                    lazyObjectsList = (List<Object>) this.LazyStructGetFieldsAsListMethod.invoke(object, new Object[0]);
                } catch (Exception e) {
                    throw new RuntimeException(e.getMessage());
                }
                final List<Object> javaObjectsList = new ArrayList<Object>();
                for (final Object item : lazyObjectsList) {
                    javaObjectsList.add(this.parseLazyObject(item));
                }
                return javaObjectsList;
            }

            private void initializeLazyStructReflection() {
                try {
                    final Class<?> LazyStructClass = Class.forName("org.apache.hadoop.hive.serde2.lazy.LazyStruct");
                    this.LazyStructGetFieldsAsListMethod = LazyStructClass.getMethod("getFieldsAsList", (Class<?>[]) new Class[0]);
                } catch (Exception e) {
                    throw new RuntimeException(e.getMessage());
                }
            }
        },
        CHAR(15) {
            @Override
            public final int getType() {
                return 1;
            }

            @Override
            public Object transform(final Object object) {
                return (object == null) ? null : ((HiveCharWritable) ((LazyHiveChar) object).getWritableObject()).toString();
            }
        },
        DATE(16) {
            @Override
            public final int getType() {
                return 91;
            }

            @Override
            public Object transform(final Object object) {
                return (object == null) ? null : ((DateWritable) ((LazyDate) object).getWritableObject()).get();
            }
        },
        VARCHAR(17) {
            @Override
            public final int getType() {
                return -2001;
            }

            @Override
            public Object transform(final Object object) {
                return (object == null) ? null : ((HiveVarcharWritable) ((LazyHiveVarchar) object).getWritableObject()).toString();
            }
        },
        OTHER(99) {
            @Override
            public final int getType() {
                return 1882;
            }

            @Override
            public Object transform(final Object object) {
                return (object == null) ? null : object.toString();
            }
        };

        private static boolean lazyHiveDecimalAvailable;
        private int index;

        private HiveDataTypeLazyImpl(final int index) {
            this.index = 0;
            this.index = index;
        }

        public int getIndex() {
            return this.index;
        }

        protected Object parseLazyObject(final Object object) {
            if (object instanceof LazyInteger) {
                return HiveDataTypeLazyImpl.INTEGER.transform(object);
            }
            if (object instanceof LazyLong) {
                return HiveDataTypeLazyImpl.BIGINT.transform(object);
            }
            if (object instanceof LazyShort) {
                return HiveDataTypeLazyImpl.SMALLINT.transform(object);
            }
            if (object instanceof LazyByte) {
                return HiveDataTypeLazyImpl.TINYINT.transform(object);
            }
            if (object instanceof LazyBinary) {
                return HiveDataTypeLazyImpl.BINARY.transform(object);
            }
            if (object instanceof LazyHiveChar) {
                return HiveDataTypeLazyImpl.CHAR.transform(object);
            }
            if (object instanceof LazyBoolean) {
                return HiveDataTypeLazyImpl.BOOLEAN.transform(object);
            }
            if (HiveDataTypeLazyImpl.lazyHiveDecimalAvailable && object instanceof LazyHiveDecimal) {
                return HiveDataTypeLazyImpl.DECIMAL.transform(object);
            }
            if (object instanceof LazyFloat) {
                return HiveDataTypeLazyImpl.FLOAT.transform(object);
            }
            if (object instanceof LazyDouble) {
                return HiveDataTypeLazyImpl.DOUBLE.transform(object);
            }
            if (object instanceof LazyString) {
                return HiveDataTypeLazyImpl.STRING.transform(object);
            }
            if (object instanceof LazyHiveVarchar) {
                return HiveDataTypeLazyImpl.VARCHAR.transform(object);
            }
            if (object instanceof LazyDate) {
                return HiveDataTypeLazyImpl.DATE.transform(object);
            }
            if (object instanceof LazyTimestamp) {
                return HiveDataTypeLazyImpl.TIMESTAMP.transform(object);
            }
            if (object instanceof LazyMap) {
                return HiveDataTypeLazyImpl.MAP.transform(object);
            }
            if (object instanceof LazyArray) {
                return HiveDataTypeLazyImpl.ARRAY.transform(object);
            }
            if (object instanceof LazyStruct) {
                return HiveDataTypeLazyImpl.STRUCT.transform(object);
            }
            return null;
        }

        static {
            HiveDataTypeLazyImpl.lazyHiveDecimalAvailable = false;
            try {
                Class.forName("org.apache.hadoop.hive.serde2.lazy.LazyHiveDecimal");
                HiveDataTypeLazyImpl.lazyHiveDecimalAvailable = true;
            } catch (ClassNotFoundException ex) {
            }
        }
    }

    public enum HiveDataTypeWritableImpl implements HiveDataType {
        BOOLEAN(1) {
            @Override
            public int getType() {
                return 16;
            }

            @Override
            public Object transform(final Object object) {
                return (object == null) ? null : Boolean.valueOf(((BooleanWritable) object).get());
            }
        },
        INTEGER(2) {
            @Override
            public final int getType() {
                return 4;
            }

            @Override
            public Object transform(final Object object) {
                return (object == null) ? null : Integer.valueOf(((IntWritable) object).get());
            }
        },
        BIGINT(3) {
            @Override
            public final int getType() {
                return -5;
            }

            @Override
            public Object transform(final Object object) {
                return (object == null) ? null : Long.valueOf(((LongWritable) object).get());
            }
        },
        SMALLINT(4) {
            @Override
            public final int getType() {
                return 5;
            }

            @Override
            public Object transform(final Object object) {
                return (object == null) ? null : Short.valueOf(((ShortWritable) object).get());
            }
        },
        TINYINT(5) {
            @Override
            public final int getType() {
                return -6;
            }

            @Override
            public Object transform(final Object object) {
                return (object == null) ? null : Byte.valueOf(((ByteWritable) object).get());
            }
        },
        DECIMAL(6) {
            @Override
            public final int getType() {
                return 3;
            }

            @Override
            public Object transform(final Object object) {
                return (object == null) ? null : ((HiveDecimalWritable) object).getHiveDecimal().bigDecimalValue();
            }
        },
        FLOAT(7) {
            @Override
            public final int getType() {
                return 6;
            }

            @Override
            public Object transform(final Object object) {
                return (object == null) ? null : Float.valueOf(((FloatWritable) object).get());
            }
        },
        BINARY(8) {
            @Override
            public final int getType() {
                return -2;
            }

            @Override
            public Object transform(final Object object) {
                return (object == null) ? null : ((BytesWritable) object).get();
            }
        },
        TIMESTAMP(9) {
            @Override
            public final int getType() {
                return 93;
            }

            @Override
            public Object transform(final Object object) {
                return (object == null) ? null : ((TimestampWritable) object).getTimestamp();
            }
        },
        STRING(10) {
            @Override
            public final int getType() {
                return 12;
            }

            @Override
            public Object transform(final Object object) {
                return (object == null) ? null : ((Text) object).toString();
            }
        },
        DOUBLE(11) {
            @Override
            public final int getType() {
                return 8;
            }

            @Override
            public Object transform(final Object object) {
                return (object == null) ? null : Double.valueOf(((DoubleWritable) object).get());
            }
        },
        MAP(12) {
            @Override
            public int getType() {
                return -1000;
            }

            @Override
            public Object transform(final Object object) {
                if (object == null) {
                    return null;
                }
                final Map<Object, Object> lazyObjectsMap = (Map<Object, Object>) ((LazyBinaryMap) object).getMap();
                final Map<Object, Object> javaObjectsMap = new HashMap<Object, Object>();
                for (final Map.Entry<Object, Object> entry : lazyObjectsMap.entrySet()) {
                    javaObjectsMap.put(this.parseWritableObject(entry.getKey()), this.parseWritableObject(entry.getValue()));
                }
                return javaObjectsMap;
            }
        },
        ARRAY(13) {
            @Override
            public final int getType() {
                return -1001;
            }

            @Override
            public Object transform(final Object object) {
                if (object == null) {
                    return null;
                }
                final List<Object> lazyObjectsList = (List<Object>) ((LazyBinaryArray) object).getList();
                final List<Object> javaObjectsList = new ArrayList<Object>();
                for (final Object item : lazyObjectsList) {
                    javaObjectsList.add(this.parseWritableObject(item));
                }
                return javaObjectsList;
            }
        },
        STRUCT(14) {
            @Override
            public int getType() {
                return -1002;
            }

            @Override
            public Object transform(final Object object) {
                if (object == null) {
                    return null;
                }
                final List<Object> lazyObjectsList = (List<Object>) ((LazyBinaryStruct) object).getFieldsAsList();
                final List<Object> javaObjectsList = new ArrayList<Object>();
                for (final Object item : lazyObjectsList) {
                    javaObjectsList.add(this.parseWritableObject(item));
                }
                return javaObjectsList;
            }
        },
        CHAR(15) {
            @Override
            public final int getType() {
                return 1;
            }

            @Override
            public Object transform(final Object object) {
                if (object instanceof HiveCharWritable) {
                    HiveCharWritable hvw = new HiveCharWritable();
                    hvw = (HiveCharWritable) object;
                    final String strChar = hvw.getHiveChar().getValue();
                    return (object == null) ? null : strChar;
                }
                return (object == null) ? null : ((HiveCharWritable) ((LazyHiveChar) object).getWritableObject());
            }
        },
        DATE(16) {
            @Override
            public final int getType() {
                return 91;
            }

            @Override
            public Object transform(final Object object) {
                return (object == null) ? null : ((DateWritable) object).get();
            }
        },
        VARCHAR(17) {
            @Override
            public final int getType() {
                return -2001;
            }

            @Override
            public Object transform(final Object object) {
                if (object instanceof HiveVarcharWritable) {
                    HiveVarcharWritable hvw = new HiveVarcharWritable();
                    hvw = (HiveVarcharWritable) object;
                    final String strVarchar = hvw.getHiveVarchar().getValue();
                    return (object == null) ? null : strVarchar;
                }
                return (object == null) ? null : ((HiveVarcharWritable) ((LazyHiveVarchar) object).getWritableObject());
            }
        },
        OTHER(99) {
            @Override
            public final int getType() {
                return 1882;
            }

            @Override
            public Object transform(final Object object) {
                return (object == null) ? null : object.toString();
            }
        };

        private static boolean hiveDecimalWritableAvailable;
        private int index;

        private HiveDataTypeWritableImpl(final int index) {
            this.index = 0;
            this.index = index;
        }

        public int getIndex() {
            return this.index;
        }

        protected Object parseWritableObject(final Object object) {
            if (object instanceof IntWritable) {
                return HiveDataTypeWritableImpl.INTEGER.transform(object);
            }
            if (object instanceof LongWritable) {
                return HiveDataTypeWritableImpl.BIGINT.transform(object);
            }
            if (object instanceof ShortWritable) {
                return HiveDataTypeWritableImpl.SMALLINT.transform(object);
            }
            if (object instanceof ByteWritable) {
                return HiveDataTypeWritableImpl.TINYINT.transform(object);
            }
            if (object instanceof BytesWritable) {
                return HiveDataTypeWritableImpl.BINARY.transform(object);
            }
            if (object instanceof BooleanWritable) {
                return HiveDataTypeWritableImpl.BOOLEAN.transform(object);
            }
            if (HiveDataTypeWritableImpl.hiveDecimalWritableAvailable && object instanceof HiveDecimalWritable) {
                return HiveDataTypeWritableImpl.DECIMAL.transform(object);
            }
            if (object instanceof FloatWritable) {
                return HiveDataTypeWritableImpl.FLOAT.transform(object);
            }
            if (object instanceof DateWritable) {
                return HiveDataTypeWritableImpl.DATE.transform(object);
            }
            if (object instanceof LazyHiveChar) {
                return HiveDataTypeWritableImpl.CHAR.transform(object);
            }
            if (object instanceof LazyHiveVarchar) {
                return HiveDataTypeWritableImpl.VARCHAR.transform(object);
            }
            if (object instanceof DoubleWritable) {
                return HiveDataTypeWritableImpl.DOUBLE.transform(object);
            }
            if (object instanceof Text) {
                return HiveDataTypeWritableImpl.STRING.transform(object);
            }
            if (object instanceof TimestampWritable) {
                return HiveDataTypeWritableImpl.TIMESTAMP.transform(object);
            }
            if (object instanceof LazyBinaryMap) {
                return HiveDataTypeWritableImpl.MAP.transform(object);
            }
            if (object instanceof LazyBinaryArray) {
                return HiveDataTypeWritableImpl.ARRAY.transform(object);
            }
            if (object instanceof LazyBinaryStruct) {
                return HiveDataTypeWritableImpl.STRUCT.transform(object);
            }
            return null;
        }

        static {
            HiveDataTypeWritableImpl.hiveDecimalWritableAvailable = false;
            try {
                Class.forName("org.apache.hadoop.hive.serde2.io.HiveDecimalWritable");
                HiveDataTypeWritableImpl.hiveDecimalWritableAvailable = true;
            } catch (ClassNotFoundException ex) {
            }
        }
    }
}
