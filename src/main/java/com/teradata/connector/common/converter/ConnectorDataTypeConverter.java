package com.teradata.connector.common.converter;

import com.teradata.connector.common.exception.ConnectorException.ErrorCode;
import com.teradata.connector.common.exception.ConnectorException.ErrorMessage;
import com.teradata.connector.common.utils.ConnectorConfiguration;
import com.teradata.connector.common.utils.ConnectorSchemaUtils;

import java.io.IOException;
import java.io.InputStream;
import java.io.StringWriter;
import java.math.BigDecimal;
import java.math.MathContext;
import java.math.RoundingMode;
import java.sql.Blob;
import java.sql.Clob;
import java.sql.Date;
import java.sql.SQLException;
import java.sql.Time;
import java.sql.Timestamp;
import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.TimeZone;

import org.apache.hadoop.conf.Configuration;
import org.codehaus.jackson.map.ObjectMapper;
import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;
import org.joda.time.format.DateTimeFormat;
import org.joda.time.format.DateTimeFormatter;

public abstract class ConnectorDataTypeConverter {
    private static final String DefaultDateFormat = "yyyy-MM-dd";
    private static final String DefaultTimeFormat = "HH:mm:ss";
    private static final String DefaultTimestampFormat = "yyyy-MM-dd HH:mm:ss.SSS";
    private static final String DefaultTimestampFormatStr = "yyyy-MM-dd HH:mm:ss.SSSSSS";
    private static final String DefaultTimestampFormatTZ = "yyyy-MM-dd HH:mm:ss.SSSZ";
    private static final int UNBOUNDED_NUMBER_SCALE_DEFAULT = 10;
    protected Object defaultValue = null;
    protected boolean nullable = false;
    protected ObjectMapper objectMapper = new ObjectMapper();

    public static final class BigDecimalToBigDecimal extends ConnectorDataTypeConverter {
        protected int scale;

        public void setScale(int scale) {
            this.scale = scale;
        }

        public final Object convert(Object object) {
            if (object == null) {
                return this.nullable ? null : (BigDecimal) this.defaultValue;
            } else {
                return (BigDecimal) object;
            }
        }
    }

    /***modify by anliang***/
    public static final class BigDecimalToBigDecimalWithScale extends ConnectorDataTypeConverter {
        protected int scale;

        public void setScale(int scale) {
            this.scale = scale;
        }

        public final Object convert(Object object) {
            if (object == null) {
                return this.nullable ? null : ((BigDecimal) this.defaultValue).setScale(this.scale, RoundingMode.HALF_UP);
            } else {
                //return ((BigDecimal) object).setScale(this.scale, RoundingMode.HALF_UP);
                return (new BigDecimal(object.toString())).setScale(this.scale, RoundingMode.HALF_UP);
            }
        }
    }

    public static final class BigDecimalToBoolean extends ConnectorDataTypeConverter {
        public final Object convert(Object object) {
            if (object == null) {
                return this.nullable ? null : this.defaultValue;
            } else {
                return Boolean.valueOf(((BigDecimal) object).unscaledValue().longValue() != 0);
            }
        }
    }

    public static final class BigDecimalToDouble extends ConnectorDataTypeConverter {
        @Override
        public final Object convert(Object object) {
            if (object == null) {
                return this.nullable ? null : this.defaultValue;
            } else {
                return Double.valueOf(((BigDecimal) object).doubleValue());
            }
        }
    }

    public static final class BigDecimalToFloat extends ConnectorDataTypeConverter {
        @Override
        public final Object convert(Object object) {
            if (object == null) {
                return this.nullable ? null : this.defaultValue;
            } else {
                return Float.valueOf(((BigDecimal) object).floatValue());
            }
        }
    }

    public static final class BigDecimalToInteger extends ConnectorDataTypeConverter {
        @Override
        public final Object convert(Object object) {
            if (object == null) {
                return this.nullable ? null : this.defaultValue;
            } else {
                return Integer.valueOf(((BigDecimal) object).intValue());
            }
        }
    }

    public static final class BigDecimalToLong extends ConnectorDataTypeConverter {
        @Override
        public final Object convert(Object object) {
            if (object == null) {
                return this.nullable ? null : this.defaultValue;
            } else {
                return Long.valueOf(((BigDecimal) object).longValue());
            }
        }
    }

    public static final class BigDecimalToShort extends ConnectorDataTypeConverter {
        @Override
        public final Object convert(Object object) {
            if (object == null) {
                return this.nullable ? null : this.defaultValue;
            } else {
                return Short.valueOf(((BigDecimal) object).shortValue());
            }
        }
    }

    public static final class BigDecimalToString extends ConnectorDataTypeConverter {
        @Override
        public final Object convert(Object object) {
            if (object == null) {
                return this.nullable ? null : this.defaultValue;
            } else {
                return ((BigDecimal) object).toPlainString();
            }
        }
    }

    public static final class BigDecimalToTinyInt extends ConnectorDataTypeConverter {
        @Override
        public final Object convert(Object object) {
            if (object == null) {
                return this.nullable ? null : this.defaultValue;
            } else {
                return Byte.valueOf(((BigDecimal) object).byteValue());
            }
        }
    }

    public static final class BinaryToBinary extends ConnectorDataTypeConverter {
        @Override
        public final Object convert(Object object) {
            return object == null ? this.defaultValue : object;
        }
    }

    public static final class BinaryToString extends ConnectorDataTypeConverter {
        private StringBuilder builder = new StringBuilder();

        @Override
        public Object convert(Object object) {
            if (object == null) {
                return null;
            }
            this.builder.setLength(0);
            for (byte b : (byte[]) object) {
                String hex = Integer.toHexString(b & 255);
                if (hex.length() == 1) {
                    hex = "0" + hex;
                }
                this.builder.append(hex);
            }
            return this.builder.toString();
        }
    }

    public static final class BlobToBinary extends ConnectorDataTypeConverter {
        private static final byte[] NULLBYTES = new byte[0];
        private StringBuilder builder = new StringBuilder();

        @Override
        public final Object convert(Object object) {
            if (object == null) {
                return NULLBYTES;
            }
            int size = 100000;
            int offset = 0;
            Blob blobObj = (Blob) object;
            byte[] buf = new byte[ErrorCode.UNKNOW_ERROR];
            byte[] data = new byte[100000];
            InputStream stream = null;
            this.builder.setLength(0);
            try {
                stream = blobObj.getBinaryStream();
                while (true) {
                    int nbytes = stream.read(buf, 0, ErrorCode.UNKNOW_ERROR);
                    if (nbytes <= 0) {
                        break;
                    }
                    if (offset + nbytes > size) {
                        size *= 2;
                        byte[] newdata = new byte[size];
                        System.arraycopy(data, 0, newdata, 0, offset);
                        data = newdata;
                    }
                    System.arraycopy(buf, 0, data, offset, nbytes);
                    offset += nbytes;
                }
                if (stream != null) {
                    try {
                        stream.close();
                    } catch (IOException e) {
                    }
                }
            } catch (IOException e2) {
                if (stream != null) {
                    try {
                        stream.close();
                    } catch (IOException e3) {
                    }
                }
            } catch (SQLException e4) {
                if (stream != null) {
                    try {
                        stream.close();
                    } catch (IOException e5) {
                    }
                }
            } catch (Throwable th) {
                if (stream != null) {
                    try {
                        stream.close();
                    } catch (IOException e6) {
                    }
                }
            }
            if (offset == 0) {
                return NULLBYTES;
            }
            Object lob = new byte[offset];
            System.arraycopy(data, 0, lob, 0, offset);
            return lob;
        }
    }

    public static final class BlobToString extends ConnectorDataTypeConverter {
        private StringBuilder builder = new StringBuilder();

        @Override
        public Object convert(Object object) {
            if (object == null) {
                return "";
            }
            int nbytes;
            int size = 100000;
            int offset = 0;
            Blob blobObj = (Blob) object;
            byte[] buf = new byte[ErrorCode.UNKNOW_ERROR];
            byte[] data = new byte[100000];
            InputStream stream = null;
            this.builder.setLength(0);
            try {
                stream = blobObj.getBinaryStream();
                while (true) {
                    nbytes = stream.read(buf, 0, ErrorCode.UNKNOW_ERROR);
                    if (nbytes <= 0) {
                        break;
                    }
                    if (offset + nbytes > size) {
                        size *= 2;
                        byte[] newdata = new byte[size];
                        System.arraycopy(data, 0, newdata, 0, offset);
                        data = newdata;
                    }
                    System.arraycopy(buf, 0, data, offset, nbytes);
                    offset += nbytes;
                }
                if (stream != null) {
                    try {
                        stream.close();
                    } catch (IOException e) {
                    }
                }
            } catch (IOException e2) {
                if (stream != null) {
                    try {
                        stream.close();
                    } catch (IOException e3) {
                    }
                }
            } catch (SQLException e4) {
                if (stream != null) {
                    try {
                        stream.close();
                    } catch (IOException e5) {
                    }
                }
            } catch (Throwable th) {
                if (stream != null) {
                    try {
                        stream.close();
                    } catch (IOException e6) {
                    }
                }
            }
            if (offset == 0) {
                return "";
            }
            byte[] bytesData = new byte[offset];
            System.arraycopy(data, 0, bytesData, 0, offset);
            this.builder.setLength(0);
            for (byte b : bytesData) {
                String hex = Integer.toHexString(b & 255);
                if (hex.length() == 1) {
                    hex = "0" + hex;
                }
                this.builder.append(hex);
            }
            return this.builder.toString();
        }
    }

    public static final class BooleanToBigDecimal extends ConnectorDataTypeConverter {
        Object falseDefaultValue;
        int precision;
        int scale;

        public void setScale(int scale) {
            this.scale = scale;
        }

        public void setPrecision(int precision) {
            this.precision = precision;
        }

        public void setFalseDefaultValue(Object falseDefaultValue) {
            this.falseDefaultValue = falseDefaultValue;
        }

        @Override
        public final Object convert(Object object) {
            if (object == null) {
                return this.nullable ? null : (BigDecimal) this.defaultValue;
            } else {
                if (((Boolean) object).booleanValue()) {
                    return (BigDecimal) this.defaultValue;
                }
                return (BigDecimal) this.falseDefaultValue;
            }
        }
    }

    public static final class BooleanToBigDecimalWithScale extends ConnectorDataTypeConverter {
        Object falseDefaultValue;
        int precision;
        int scale;

        public void setScale(int scale) {
            this.scale = scale;
        }

        public void setPrecision(int precision) {
            this.precision = precision;
        }

        public void setFalseDefaultValue(Object falseDefaultValue) {
            this.falseDefaultValue = falseDefaultValue;
        }

        @Override
        public final Object convert(Object object) {
            if (object == null) {
                return this.nullable ? null : ((BigDecimal) this.defaultValue).setScale(this.scale, RoundingMode.HALF_UP);
            } else {
                if (((Boolean) object).booleanValue()) {
                    return ((BigDecimal) this.defaultValue).setScale(this.scale, RoundingMode.HALF_UP);
                }
                return ((BigDecimal) this.falseDefaultValue).setScale(this.scale, RoundingMode.HALF_UP);
            }
        }
    }

    public static final class BooleanToBoolean extends ConnectorDataTypeConverter {
        Object falseDefaultValue;

        public void setFalseDefaultValue(Object falseDefaultValue) {
            this.falseDefaultValue = falseDefaultValue;
        }

        @Override
        public final Object convert(Object object) {
            if (object == null) {
                return this.nullable ? null : this.defaultValue;
            } else {
                return object;
            }
        }
    }

    public static final class BooleanToDouble extends ConnectorDataTypeConverter {
        Object falseDefaultValue;

        public void setFalseDefaultValue(Object falseDefaultValue) {
            this.falseDefaultValue = falseDefaultValue;
        }

        @Override
        public final Object convert(Object object) {
            return object == null ? this.nullable ? null : this.defaultValue : ((Boolean) object).booleanValue() ? this.defaultValue : this.falseDefaultValue;
        }
    }

    public static final class BooleanToFloat extends ConnectorDataTypeConverter {
        Object falseDefaultValue;

        public void setFalseDefaultValue(Object falseDefaultValue) {
            this.falseDefaultValue = falseDefaultValue;
        }

        @Override
        public final Object convert(Object object) {
            return object == null ? this.nullable ? null : this.defaultValue : ((Boolean) object).booleanValue() ? this.defaultValue : this.falseDefaultValue;
        }
    }

    public static final class BooleanToInteger extends ConnectorDataTypeConverter {
        Object falseDefaultValue;

        public void setFalseDefaultValue(Object falseDefaultValue) {
            this.falseDefaultValue = falseDefaultValue;
        }

        @Override
        public final Object convert(Object object) {
            return object == null ? this.nullable ? null : this.defaultValue : ((Boolean) object).booleanValue() ? this.defaultValue : this.falseDefaultValue;
        }
    }

    public static final class BooleanToLong extends ConnectorDataTypeConverter {
        Object falseDefaultValue;

        public void setFalseDefaultValue(Object falseDefaultValue) {
            this.falseDefaultValue = falseDefaultValue;
        }

        @Override
        public final Object convert(Object object) {
            return object == null ? this.nullable ? null : this.defaultValue : ((Boolean) object).booleanValue() ? this.defaultValue : this.falseDefaultValue;
        }
    }

    public static final class BooleanToShort extends ConnectorDataTypeConverter {
        Object falseDefaultValue;

        public void setFalseDefaultValue(Object falseDefaultValue) {
            this.falseDefaultValue = falseDefaultValue;
        }

        @Override
        public final Object convert(Object object) {
            return object == null ? this.nullable ? null : this.defaultValue : ((Boolean) object).booleanValue() ? this.defaultValue : this.falseDefaultValue;
        }
    }

    public static final class BooleanToString extends ConnectorDataTypeConverter {
        @Override
        public final Object convert(Object object) {
            if (object == null) {
                return this.nullable ? null : this.defaultValue;
            } else {
                return object.toString();
            }
        }
    }

    public static final class BooleanToTinyInt extends ConnectorDataTypeConverter {
        Object falseDefaultValue;

        public void setFalseDefaultValue(Object falseDefaultValue) {
            this.falseDefaultValue = falseDefaultValue;
        }

        @Override
        public final Object convert(Object object) {
            return object == null ? this.nullable ? null : this.defaultValue : ((Boolean) object).booleanValue() ? this.defaultValue : this.falseDefaultValue;
        }
    }

    public static final class BytesToString extends ConnectorDataTypeConverter {
        public static String bytesToString(byte[] bytes) {
            char[] buffer = new char[(bytes.length >> 1)];
            for (int i = 0; i < buffer.length; i++) {
                int bpos = i << 1;
                buffer[i] = (char) (((bytes[bpos] & 255) << 8) + (bytes[bpos + 1] & 255));
            }
            return new String(buffer);
        }

        @Override
        public Object convert(Object object) {
            if (object == null) {
                return null;
            }
            return bytesToString((byte[]) object);
        }
    }

    public static final class CalendarToCalendar extends ConnectorDataTypeConverter {
        private Calendar targetCalendar;

        public CalendarToCalendar(String targetTimezoneId) {
            if (targetTimezoneId == null || targetTimezoneId.isEmpty()) {
                targetTimezoneId = TimeZone.getDefault().getID();
            }
            this.targetCalendar = Calendar.getInstance(TimeZone.getTimeZone(targetTimezoneId));
        }

        @Override
        public final Object convert(Object object) {
            if (object == null) {
                return this.nullable ? null : this.defaultValue;
            } else {
                this.targetCalendar.setTimeInMillis(((Calendar) object).getTimeInMillis());
                return this.targetCalendar;
            }
        }
    }

    public static final class CalendarToDateTZ extends ConnectorDataTypeConverter {
        TimeZone targetTimezone;

        public CalendarToDateTZ(String targetTimezoneId) {
            if (targetTimezoneId == null || targetTimezoneId.isEmpty()) {
                targetTimezoneId = TimeZone.getDefault().getID();
            }
            this.targetTimezone = TimeZone.getTimeZone(targetTimezoneId);
        }

        @Override
        public final Object convert(Object object) {
            if (object == null) {
                return this.nullable ? null : this.defaultValue;
            } else {
                return new Date(ConnectorDataTypeConverter.convertMillisecFromDefaultToTargetTZ(((Calendar) object).getTimeInMillis(), this.targetTimezone));
            }
        }
    }

    public static final class CalendarToInteger extends ConnectorDataTypeConverter {
        @Override
        public final Object convert(Object object) {
            if (object == null) {
                return this.nullable ? null : this.defaultValue;
            } else {
                return Integer.valueOf((int) (((Calendar) object).getTimeInMillis() / 1000));
            }
        }
    }

    public static final class CalendarToLong extends ConnectorDataTypeConverter {
        @Override
        public final Object convert(Object object) {
            if (object == null) {
                return this.nullable ? null : this.defaultValue;
            } else {
                return Long.valueOf(((Calendar) object).getTimeInMillis());
            }
        }
    }

    public static final class CalendarToStringFMTTZ extends ConnectorDataTypeConverter {
        private DateFormat df;
        private TimeZone targetTimezone;

        public CalendarToStringFMTTZ(String targetTimezoneId, String targetFormat) {
            if (targetTimezoneId == null || targetTimezoneId.isEmpty()) {
                targetTimezoneId = TimeZone.getDefault().getID();
            }
            this.targetTimezone = TimeZone.getTimeZone(targetTimezoneId);
            if (targetFormat == null || targetFormat.isEmpty()) {
                this.df = ConnectorDataTypeConverter.createDateFormat(this.targetTimezone, ConnectorDataTypeConverter.DefaultTimestampFormatTZ);
            } else {
                this.df = ConnectorDataTypeConverter.createDateFormat(this.targetTimezone, targetFormat);
            }
        }

        @Override
        public final Object convert(Object object) {
            if (object == null) {
                return this.nullable ? null : this.defaultValue;
            } else {
                return this.df.format(Long.valueOf(((Calendar) object).getTimeInMillis()));
            }
        }
    }

    public static final class CalendarToTimeTZ extends ConnectorDataTypeConverter {
        TimeZone targetTimezone;

        public CalendarToTimeTZ(String targetTimezoneId) {
            if (targetTimezoneId == null || targetTimezoneId.isEmpty()) {
                targetTimezoneId = TimeZone.getDefault().getID();
            }
            this.targetTimezone = TimeZone.getTimeZone(targetTimezoneId);
        }

        @Override
        public final Object convert(Object object) {
            if (object == null) {
                return this.nullable ? null : this.defaultValue;
            } else {
                return new Time(ConnectorDataTypeConverter.convertMillisecFromDefaultToTargetTZ(((Calendar) object).getTimeInMillis(), this.targetTimezone));
            }
        }
    }

    public static final class CalendarToTimestampTZ extends ConnectorDataTypeConverter {
        TimeZone targetTimezone;

        public CalendarToTimestampTZ(String targetTimezoneId) {
            if (targetTimezoneId == null || targetTimezoneId.isEmpty()) {
                targetTimezoneId = TimeZone.getDefault().getID();
            }
            this.targetTimezone = TimeZone.getTimeZone(targetTimezoneId);
        }

        @Override
        public final Object convert(Object object) {
            if (object == null) {
                return this.nullable ? null : this.defaultValue;
            } else {
                return new Timestamp(ConnectorDataTypeConverter.convertMillisecFromDefaultToTargetTZ(((Calendar) object).getTimeInMillis(), this.targetTimezone));
            }
        }
    }

    public static final class ClobToString
            extends ConnectorDataTypeConverter {
        private StringBuilder builder = new StringBuilder();

        @Override
        public Object convert(Object object) {
            if (object == null) {
                return null;
            }
            int bufsize = 10000;

            Clob clobObj = (Clob) object;
            byte[] bytesData = new byte[bufsize];
            InputStream stream = null;
            this.builder.setLength(0);
            try {
                stream = clobObj.getAsciiStream();
                int nbytes;
                byte b;
                while ((nbytes = stream.read(bytesData, 0, bufsize)) > 0) {
                    for (int i = 0; i < nbytes; i++) {
                        b = bytesData[i];
                        int c = b & 0xFF;
                        this.builder.append((char) c);
                    }
                }

                return this.builder.toString();
            } catch (IOException e) {
                return "";
            } catch (SQLException e) {
                return "";
            } finally {
                try {
                    if (stream != null) {
                        stream.close();
                    }
                } catch (IOException localIOException4) {
                }
            }
        }
    }

    @Deprecated
    public static final class DateTZToDateTZ extends ConnectorDataTypeConverter {
        private int offset;
        private Calendar targetCalendar;

        public DateTZToDateTZ(String sourceTimezoneId, String targetTimezoneId) {
            if (sourceTimezoneId == null || sourceTimezoneId.isEmpty()) {
                sourceTimezoneId = TimeZone.getDefault().getID();
            }
            if (targetTimezoneId == null || targetTimezoneId.isEmpty()) {
                targetTimezoneId = TimeZone.getDefault().getID();
            }
            TimeZone sourceTimezone = TimeZone.getTimeZone(sourceTimezoneId);
            TimeZone targetTimezone = TimeZone.getTimeZone(targetTimezoneId);
            this.offset = targetTimezone.getOffset(System.currentTimeMillis()) - sourceTimezone.getOffset(System.currentTimeMillis());
            this.targetCalendar = Calendar.getInstance(targetTimezone);
        }

        public final Object convert(Object object) {
            if (object == null) {
                return this.nullable ? null : this.defaultValue;
            } else {
                this.targetCalendar.setTimeInMillis(((Date) object).getTime());
                this.targetCalendar.add(14, this.offset);
                return new Date(this.targetCalendar.getTimeInMillis());
            }
        }
    }

    public static final class DateTZToInteger extends ConnectorDataTypeConverter {
        private int offset;
        private TimeZone sourceTimezone;
        private boolean timezoneIsDST;

        public DateTZToInteger(String sourceTimezoneId) {
            if (sourceTimezoneId == null || sourceTimezoneId.isEmpty()) {
                sourceTimezoneId = TimeZone.getDefault().getID();
            }
            this.sourceTimezone = TimeZone.getTimeZone(sourceTimezoneId);
            if (this.sourceTimezone.useDaylightTime()) {
                this.timezoneIsDST = true;
                this.offset = 0;
                return;
            }
            this.timezoneIsDST = false;
            this.offset = this.sourceTimezone.getOffset(System.currentTimeMillis());
        }

        public final Object convert(Object object) {
            if (object == null) {
                return this.nullable ? null : this.defaultValue;
            } else {
                long timestamp = ((Date) object).getTime();
                if (this.timezoneIsDST) {
                    this.offset = this.sourceTimezone.getOffset(timestamp);
                }
                return Integer.valueOf((int) ((timestamp - ((long) this.offset)) / 1000));
            }
        }
    }

    public static final class DateTZToLong extends ConnectorDataTypeConverter {
        private int offset;
        private TimeZone sourceTimezone;
        private boolean timezoneIsDST;

        public DateTZToLong(String sourceTimezoneId) {
            if (sourceTimezoneId == null || sourceTimezoneId.isEmpty()) {
                sourceTimezoneId = TimeZone.getDefault().getID();
            }
            this.sourceTimezone = TimeZone.getTimeZone(sourceTimezoneId);
            if (this.sourceTimezone.useDaylightTime()) {
                this.timezoneIsDST = true;
                this.offset = 0;
                return;
            }
            this.timezoneIsDST = false;
            this.offset = this.sourceTimezone.getOffset(System.currentTimeMillis());
        }

        public final Object convert(Object object) {
            if (object == null) {
                return this.nullable ? null : this.defaultValue;
            } else {
                long timestamp = ((Date) object).getTime();
                if (this.timezoneIsDST) {
                    this.offset = this.sourceTimezone.getOffset(timestamp);
                }
                return Long.valueOf(timestamp - ((long) this.offset));
            }
        }
    }

    @Deprecated
    public static final class DateTZToStringFMT extends ConnectorDataTypeConverter {
        DateFormat df;
        private int offset;
        private Calendar targetCalendar;

        public DateTZToStringFMT(String sourceTimezoneId, String dateFormat) {
            if (dateFormat == null || dateFormat.isEmpty()) {
                this.df = null;
            } else {
                this.df = new SimpleDateFormat(dateFormat);
            }
            if (sourceTimezoneId == null || sourceTimezoneId.isEmpty()) {
                sourceTimezoneId = TimeZone.getDefault().getID();
            }
            TimeZone sourceTimezone = TimeZone.getTimeZone(sourceTimezoneId);
            TimeZone targetTimezone = TimeZone.getDefault();
            this.offset = targetTimezone.getOffset(System.currentTimeMillis()) - sourceTimezone.getOffset(System.currentTimeMillis());
            this.targetCalendar = Calendar.getInstance(targetTimezone);
        }

        public final Object convert(Object object) {
            if (object == null) {
                return this.nullable ? null : this.defaultValue;
            } else {
                this.targetCalendar.setTimeInMillis(((Date) object).getTime());
                this.targetCalendar.add(14, this.offset);
                java.util.Date d = new java.util.Date(this.targetCalendar.getTimeInMillis());
                if (this.df != null) {
                    return this.df.format(d);
                }
                return d.toString();
            }
        }
    }

    public static final class DateTZToTimestampTZ extends ConnectorDataTypeConverter {
        private int offset;
        private Calendar targetCalendar;

        public DateTZToTimestampTZ(String sourceTimezoneId, String targetTimezoneId) {
            if (sourceTimezoneId == null || sourceTimezoneId.isEmpty()) {
                sourceTimezoneId = TimeZone.getDefault().getID();
            }
            if (targetTimezoneId == null || targetTimezoneId.isEmpty()) {
                targetTimezoneId = TimeZone.getDefault().getID();
            }
            TimeZone sourceTimezone = TimeZone.getTimeZone(sourceTimezoneId);
            TimeZone targetTimezone = TimeZone.getTimeZone(targetTimezoneId);
            this.offset = targetTimezone.getOffset(System.currentTimeMillis()) - sourceTimezone.getOffset(System.currentTimeMillis());
            this.targetCalendar = Calendar.getInstance(targetTimezone);
        }

        public final Object convert(Object object) {
            if (object == null) {
                return this.nullable ? null : this.defaultValue;
            } else {
                this.targetCalendar.setTimeInMillis(((Date) object).getTime());
                this.targetCalendar.add(14, this.offset);
                return new Timestamp(this.targetCalendar.getTimeInMillis());
            }
        }
    }

    public static final class DateToCalendar extends ConnectorDataTypeConverter {
        private Calendar targetCalendar;

        public DateToCalendar(String targetTimezoneId) {
            if (targetTimezoneId == null || targetTimezoneId.isEmpty()) {
                targetTimezoneId = TimeZone.getDefault().getID();
            }
            this.targetCalendar = Calendar.getInstance(TimeZone.getTimeZone(targetTimezoneId));
        }

        public final Object convert(Object object) {
            if (object == null) {
                return this.nullable ? null : this.defaultValue;
            } else {
                this.targetCalendar.setTimeInMillis(((Date) object).getTime());
                return this.targetCalendar;
            }
        }
    }

    public static final class DateToDate extends ConnectorDataTypeConverter {
        public final Object convert(Object object) {
            if (object == null) {
                return this.nullable ? null : this.defaultValue;
            } else {
                return object;
            }
        }
    }

    @Deprecated
    public static final class DateToInteger extends ConnectorDataTypeConverter {
        public final Object convert(Object object) {
            if (object == null) {
                return this.nullable ? null : this.defaultValue;
            } else {
                return Integer.valueOf((int) (((Date) object).getTime() / 1000));
            }
        }
    }

    public static final class DateToLong extends ConnectorDataTypeConverter {
        public final Object convert(Object object) {
            if (object == null) {
                return this.nullable ? null : this.defaultValue;
            } else {
                return Long.valueOf(((Date) object).getTime());
            }
        }
    }

    @Deprecated
    public static final class DateToString extends ConnectorDataTypeConverter {
        public final Object convert(Object object) {
            if (object == null) {
                return this.nullable ? null : this.defaultValue;
            } else {
                return object.toString();
            }
        }
    }

    public static final class DateToStringFMT extends ConnectorDataTypeConverter {
        DateFormat df;

        public DateToStringFMT(String dateFormat) {
            if (dateFormat == null || dateFormat.isEmpty()) {
                this.df = null;
            } else {
                this.df = new SimpleDateFormat(dateFormat);
            }
        }

        public final Object convert(Object object) {
            if (object == null) {
                return this.nullable ? null : this.defaultValue;
            } else {
                if (this.df != null) {
                    return this.df.format(object);
                }
                return object.toString();
            }
        }
    }

    public static final class DateToStringWithFormat extends ConnectorDataTypeConverter {
        String dateFormat;

        public DateToStringWithFormat(String dateFormat) {
            this.dateFormat = dateFormat;
        }

        public final Object convert(Object object) {
            if (object == null) {
                return this.nullable ? null : this.defaultValue;
            } else {
                return new SimpleDateFormat(this.dateFormat).format((Date) object);
            }
        }
    }

    public static final class DateToTimestamp extends ConnectorDataTypeConverter {
        public final Object convert(Object object) {
            if (object == null) {
                return this.nullable ? null : this.defaultValue;
            } else {
                return new Timestamp(((Date) object).getTime());
            }
        }
    }

    public static final class DoubleToBigDecimal extends ConnectorDataTypeConverter {
        protected int precision;
        protected int scale;

        public void setScale(int scale) {
            this.scale = scale;
        }

        public void setPrecision(int precision) {
            this.precision = precision;
        }

        public final Object convert(Object object) {
            if (object == null) {
                return this.nullable ? null : (BigDecimal) this.defaultValue;
            } else {
                return new BigDecimal(((Double) object).doubleValue());
            }
        }
    }

    public static final class DoubleToBigDecimalWithScale extends ConnectorDataTypeConverter {
        protected int precision;
        protected int scale;

        public void setScale(int scale) {
            this.scale = scale;
        }

        public void setPrecision(int precision) {
            this.precision = precision;
        }

        public final Object convert(Object object) {
            if (object == null) {
                return this.nullable ? null : ((BigDecimal) this.defaultValue).setScale(this.scale, RoundingMode.HALF_UP);
            } else {
                return new BigDecimal(((Double) object).doubleValue(), new MathContext(this.precision, RoundingMode.HALF_UP)).setScale(this.scale, RoundingMode.HALF_UP);
            }
        }
    }

    public static final class DoubleToBoolean extends ConnectorDataTypeConverter {
        public final Object convert(Object object) {
            if (object == null) {
                return this.nullable ? null : this.defaultValue;
            } else {
                return Boolean.valueOf(((Double) object).doubleValue() != 0.0d);
            }
        }
    }

    public static final class DoubleToDouble extends ConnectorDataTypeConverter {
        public final Object convert(Object object) {
            if (object == null) {
                return this.nullable ? null : this.defaultValue;
            } else {
                return object;
            }
        }
    }

    public static final class DoubleToFloat extends ConnectorDataTypeConverter {
        public final Object convert(Object object) {
            if (object == null) {
                return this.nullable ? null : this.defaultValue;
            } else {
                return Float.valueOf(((Double) object).floatValue());
            }
        }
    }

    public static final class DoubleToInteger extends ConnectorDataTypeConverter {
        public final Object convert(Object object) {
            if (object == null) {
                return this.nullable ? null : this.defaultValue;
            } else {
                return Integer.valueOf(((Double) object).intValue());
            }
        }
    }

    public static final class DoubleToLong extends ConnectorDataTypeConverter {
        public final Object convert(Object object) {
            if (object == null) {
                return this.nullable ? null : this.defaultValue;
            } else {
                return Long.valueOf(((Double) object).longValue());
            }
        }
    }

    public static final class DoubleToShort extends ConnectorDataTypeConverter {
        public final Object convert(Object object) {
            if (object == null) {
                return this.nullable ? null : this.defaultValue;
            } else {
                return Short.valueOf(((Double) object).shortValue());
            }
        }
    }

    public static final class DoubleToString extends ConnectorDataTypeConverter {
        public final Object convert(Object object) {
            if (object == null) {
                return this.nullable ? null : this.defaultValue;
            } else {
                return object.toString();
            }
        }
    }

    public static final class DoubleToTinyInt extends ConnectorDataTypeConverter {
        public final Object convert(Object object) {
            if (object == null) {
                return this.nullable ? null : this.defaultValue;
            } else {
                return Byte.valueOf(((Double) object).byteValue());
            }
        }
    }

    public static final class FloatToBigDecimal extends ConnectorDataTypeConverter {
        protected int precision;
        protected int scale;

        public void setScale(int scale) {
            this.scale = scale;
        }

        public void setPrecision(int precision) {
            this.precision = precision;
        }

        public final Object convert(Object object) {
            if (object == null) {
                return this.nullable ? null : (BigDecimal) this.defaultValue;
            } else {
                return new BigDecimal((double) ((Float) object).floatValue());
            }
        }
    }

    public static final class FloatToBigDecimalWithScale extends ConnectorDataTypeConverter {
        protected int precision;
        protected int scale;

        public void setScale(int scale) {
            this.scale = scale;
        }

        public void setPrecision(int precision) {
            this.precision = precision;
        }

        public final Object convert(Object object) {
            if (object == null) {
                return this.nullable ? null : ((BigDecimal) this.defaultValue).setScale(this.scale, RoundingMode.HALF_UP);
            } else {
                return new BigDecimal((double) ((Float) object).floatValue(), new MathContext(this.precision, RoundingMode.HALF_UP)).setScale(this.scale, RoundingMode.HALF_UP);
            }
        }
    }

    public static final class FloatToBoolean extends ConnectorDataTypeConverter {
        public final Object convert(Object object) {
            if (object == null) {
                return this.nullable ? null : this.defaultValue;
            } else {
                return Boolean.valueOf(((double) ((Float) object).floatValue()) != 0.0d);
            }
        }
    }

    public static final class FloatToDouble extends ConnectorDataTypeConverter {
        public final Object convert(Object object) {
            if (object == null) {
                return this.nullable ? null : this.defaultValue;
            } else {
                return Double.valueOf(((Float) object).doubleValue());
            }
        }
    }

    public static final class FloatToFloat extends ConnectorDataTypeConverter {
        public final Object convert(Object object) {
            if (object == null) {
                return this.nullable ? null : this.defaultValue;
            } else {
                return object;
            }
        }
    }

    public static final class FloatToInteger extends ConnectorDataTypeConverter {
        public final Object convert(Object object) {
            if (object == null) {
                return this.nullable ? null : this.defaultValue;
            } else {
                return Integer.valueOf(((Float) object).intValue());
            }
        }
    }

    public static final class FloatToLong extends ConnectorDataTypeConverter {
        public final Object convert(Object object) {
            if (object == null) {
                return this.nullable ? null : this.defaultValue;
            } else {
                return Long.valueOf(((Float) object).longValue());
            }
        }
    }

    public static final class FloatToShort extends ConnectorDataTypeConverter {
        public final Object convert(Object object) {
            if (object == null) {
                return this.nullable ? null : this.defaultValue;
            } else {
                return Short.valueOf(((Float) object).shortValue());
            }
        }
    }

    public static final class FloatToString extends ConnectorDataTypeConverter {
        public final Object convert(Object object) {
            if (object == null) {
                return this.nullable ? null : this.defaultValue;
            } else {
                return object.toString();
            }
        }
    }

    public static final class FloatToTinyInt extends ConnectorDataTypeConverter {
        public final Object convert(Object object) {
            if (object == null) {
                return this.nullable ? null : this.defaultValue;
            } else {
                return Byte.valueOf(((Float) object).byteValue());
            }
        }
    }

    public static final class IntegerToBigDecimal extends ConnectorDataTypeConverter {
        protected int scale;

        public void setScale(int scale) {
            this.scale = scale;
        }

        public final Object convert(Object object) {
            if (object == null) {
                return this.nullable ? null : (BigDecimal) this.defaultValue;
            } else {
                return new BigDecimal(((Integer) object).intValue());
            }
        }
    }

    public static final class IntegerToBigDecimalWithScale extends ConnectorDataTypeConverter {
        protected int scale;

        public void setScale(int scale) {
            this.scale = scale;
        }

        public final Object convert(Object object) {
            if (object == null) {
                return this.nullable ? null : ((BigDecimal) this.defaultValue).setScale(this.scale, RoundingMode.HALF_UP);
            } else {
                return new BigDecimal(((Integer) object).intValue()).setScale(this.scale, RoundingMode.HALF_UP);
            }
        }
    }

    public static final class IntegerToBoolean extends ConnectorDataTypeConverter {
        public final Object convert(Object object) {
            if (object == null) {
                return this.nullable ? null : this.defaultValue;
            } else {
                return Boolean.valueOf(((Integer) object).intValue() != 0);
            }
        }
    }

    public static final class IntegerToCalendar extends ConnectorDataTypeConverter {
        private Calendar targetCalendar;

        public IntegerToCalendar(String targetTimezoneId) {
            if (targetTimezoneId == null || targetTimezoneId.isEmpty()) {
                targetTimezoneId = TimeZone.getDefault().getID();
            }
            this.targetCalendar = Calendar.getInstance(TimeZone.getTimeZone(targetTimezoneId));
        }

        public final Object convert(Object object) {
            if (object == null) {
                return this.nullable ? null : this.defaultValue;
            } else {
                this.targetCalendar.setTimeInMillis(((Integer) object).longValue() * 1000);
                return this.targetCalendar;
            }
        }
    }

    public static final class IntegerToDateTZ extends ConnectorDataTypeConverter {
        private int offset;
        private TimeZone targetTimezone;
        private boolean timezoneIsDST;

        public IntegerToDateTZ(String targetTimezoneId) {
            if (targetTimezoneId == null || targetTimezoneId.isEmpty()) {
                targetTimezoneId = TimeZone.getDefault().getID();
            }
            this.targetTimezone = TimeZone.getTimeZone(targetTimezoneId);
            if (this.targetTimezone.useDaylightTime()) {
                this.timezoneIsDST = true;
                this.offset = 0;
                return;
            }
            this.timezoneIsDST = false;
            this.offset = this.targetTimezone.getOffset(System.currentTimeMillis());
        }

        public final Object convert(Object object) {
            if (object == null) {
                return this.nullable ? null : this.defaultValue;
            } else {
                long timestamp = ((Integer) object).longValue() * 1000;
                if (this.timezoneIsDST) {
                    this.offset = this.targetTimezone.getOffset(timestamp);
                }
                return new Date(((long) this.offset) + timestamp);
            }
        }
    }

    public static final class IntegerToDouble extends ConnectorDataTypeConverter {
        public final Object convert(Object object) {
            if (object == null) {
                return this.nullable ? null : this.defaultValue;
            } else {
                return Double.valueOf(((Integer) object).doubleValue());
            }
        }
    }

    public static final class IntegerToFloat extends ConnectorDataTypeConverter {
        public final Object convert(Object object) {
            if (object == null) {
                return this.nullable ? null : this.defaultValue;
            } else {
                return Float.valueOf(((Integer) object).floatValue());
            }
        }
    }

    public static final class IntegerToInteger extends ConnectorDataTypeConverter {
        public final Object convert(Object object) {
            if (object == null) {
                return this.nullable ? null : this.defaultValue;
            } else {
                return object;
            }
        }
    }

    public static final class IntegerToLong extends ConnectorDataTypeConverter {
        public final Object convert(Object object) {
            if (object == null) {
                return this.nullable ? null : this.defaultValue;
            } else {
                return Long.valueOf(((Integer) object).longValue());
            }
        }
    }

    public static final class IntegerToShort extends ConnectorDataTypeConverter {
        public final Object convert(Object object) {
            if (object == null) {
                return this.nullable ? null : this.defaultValue;
            } else {
                return Short.valueOf(object.toString());
            }
        }
    }

    public static final class IntegerToString extends ConnectorDataTypeConverter {
        public final Object convert(Object object) {
            if (object == null) {
                return this.nullable ? null : this.defaultValue;
            } else {
                return object.toString();
            }
        }
    }

    public static final class IntegerToTimeTZ extends ConnectorDataTypeConverter {
        TimeZone targetTimezone;

        public IntegerToTimeTZ(String targetTimezoneId) {
            if (targetTimezoneId == null || targetTimezoneId.isEmpty()) {
                targetTimezoneId = TimeZone.getDefault().getID();
            }
            this.targetTimezone = TimeZone.getTimeZone(targetTimezoneId);
        }

        public Object convert(Object object) {
            if (object == null) {
                return this.nullable ? null : this.defaultValue;
            } else {
                return new Time(ConnectorDataTypeConverter.convertMillisecFromDefaultToTargetTZ(((Integer) object).longValue() * 1000, this.targetTimezone));
            }
        }
    }

    public static final class IntegerToTimestampTZ extends ConnectorDataTypeConverter {
        TimeZone targetTimezone;

        public IntegerToTimestampTZ(String targetTimezoneId) {
            if (targetTimezoneId == null || targetTimezoneId.isEmpty()) {
                targetTimezoneId = TimeZone.getDefault().getID();
            }
            this.targetTimezone = TimeZone.getTimeZone(targetTimezoneId);
        }

        public Object convert(Object object) {
            if (object == null) {
                return this.nullable ? null : this.defaultValue;
            } else {
                return new Timestamp(ConnectorDataTypeConverter.convertMillisecFromDefaultToTargetTZ(((Integer) object).longValue() * 1000, this.targetTimezone));
            }
        }
    }

    public static final class IntegerToTinyInt extends ConnectorDataTypeConverter {
        public final Object convert(Object object) {
            if (object == null) {
                return this.nullable ? null : this.defaultValue;
            } else {
                return Byte.valueOf(((Integer) object).byteValue());
            }
        }
    }

    public static final class IntervalToString extends ConnectorDataTypeConverter {
        public final Object convert(Object object) {
            if (object == null) {
                return this.nullable ? null : this.defaultValue;
            } else {
                return object.toString();
            }
        }
    }

    public static final class LongToBigDecimal extends ConnectorDataTypeConverter {
        protected int scale;

        public void setScale(int scale) {
            this.scale = scale;
        }

        public final Object convert(Object object) {
            if (object == null) {
                return this.nullable ? null : (BigDecimal) this.defaultValue;
            } else {
                return new BigDecimal(((Long) object).longValue());
            }
        }
    }

    public static final class LongToBigDecimalWithScale extends ConnectorDataTypeConverter {
        protected int scale;

        public void setScale(int scale) {
            this.scale = scale;
        }

        public final Object convert(Object object) {
            if (object == null) {
                return this.nullable ? null : ((BigDecimal) this.defaultValue).setScale(this.scale, RoundingMode.HALF_UP);
            } else {
                return new BigDecimal(((Long) object).longValue()).setScale(this.scale, RoundingMode.HALF_UP);
            }
        }
    }

    public static final class LongToBoolean extends ConnectorDataTypeConverter {
        public final Object convert(Object object) {
            if (object == null) {
                return this.nullable ? null : this.defaultValue;
            } else {
                return Boolean.valueOf(((Long) object).longValue() != 0);
            }
        }
    }

    public static final class LongToCalendar extends ConnectorDataTypeConverter {
        private Calendar targetCalendar;

        public LongToCalendar(String targetTimezoneId) {
            if (targetTimezoneId == null || targetTimezoneId.isEmpty()) {
                targetTimezoneId = TimeZone.getDefault().getID();
            }
            this.targetCalendar = Calendar.getInstance(TimeZone.getTimeZone(targetTimezoneId));
        }

        public final Object convert(Object object) {
            if (object == null) {
                return this.nullable ? null : this.defaultValue;
            } else {
                this.targetCalendar.setTimeInMillis(((Long) object).longValue());
                return this.targetCalendar;
            }
        }
    }

    public static final class LongToDateTZ extends ConnectorDataTypeConverter {
        private int offset;
        private TimeZone targetTimezone;
        private boolean timezoneIsDST;

        public LongToDateTZ(String targetTimezoneId) {
            if (targetTimezoneId == null || targetTimezoneId.isEmpty()) {
                targetTimezoneId = TimeZone.getDefault().getID();
            }
            this.targetTimezone = TimeZone.getTimeZone(targetTimezoneId);
            if (this.targetTimezone.useDaylightTime()) {
                this.timezoneIsDST = true;
                this.offset = 0;
                return;
            }
            this.timezoneIsDST = false;
            this.offset = this.targetTimezone.getOffset(System.currentTimeMillis());
        }

        public final Object convert(Object object) {
            if (object == null) {
                return this.nullable ? null : this.defaultValue;
            } else {
                long timestamp = ((Long) object).longValue();
                if (this.timezoneIsDST) {
                    this.offset = this.targetTimezone.getOffset(timestamp);
                }
                return new Date(((long) this.offset) + timestamp);
            }
        }
    }

    public static final class LongToDouble extends ConnectorDataTypeConverter {
        public final Object convert(Object object) {
            if (object == null) {
                return this.nullable ? null : this.defaultValue;
            } else {
                return Double.valueOf(((Long) object).doubleValue());
            }
        }
    }

    public static final class LongToFloat extends ConnectorDataTypeConverter {
        public final Object convert(Object object) {
            if (object == null) {
                return this.nullable ? null : this.defaultValue;
            } else {
                return Float.valueOf(((Long) object).floatValue());
            }
        }
    }

    public static final class LongToInteger extends ConnectorDataTypeConverter {
        public final Object convert(Object object) {
            if (object == null) {
                return this.nullable ? null : this.defaultValue;
            } else {
                return Integer.valueOf(object.toString());
            }
        }
    }

    public static final class LongToLong extends ConnectorDataTypeConverter {
        public final Object convert(Object object) {
            if (object == null) {
                return this.nullable ? null : this.defaultValue;
            } else {
                return object;
            }
        }
    }

    public static final class LongToShort extends ConnectorDataTypeConverter {
        public final Object convert(Object object) {
            if (object == null) {
                return this.nullable ? null : this.defaultValue;
            } else {
                return Short.valueOf(object.toString());
            }
        }
    }

    public static final class LongToString extends ConnectorDataTypeConverter {
        public final Object convert(Object object) {
            if (object == null) {
                return this.nullable ? null : this.defaultValue;
            } else {
                return object.toString();
            }
        }
    }

    public static final class LongToTimeTZ extends ConnectorDataTypeConverter {
        TimeZone targetTimezone;

        public LongToTimeTZ(String targetTimezoneId) {
            if (targetTimezoneId == null || targetTimezoneId.isEmpty()) {
                targetTimezoneId = TimeZone.getDefault().getID();
            }
            this.targetTimezone = TimeZone.getTimeZone(targetTimezoneId);
        }

        public Object convert(Object object) {
            if (object == null) {
                return this.nullable ? null : this.defaultValue;
            } else {
                return new Time(ConnectorDataTypeConverter.convertMillisecFromDefaultToTargetTZ(((Long) object).longValue(), this.targetTimezone));
            }
        }
    }

    @Deprecated
    public static final class LongToTimestamp extends ConnectorDataTypeConverter {
        Timestamp ts = new Timestamp(System.currentTimeMillis());

        public final Object convert(Object object) {
            if (object == null) {
                return this.nullable ? null : this.defaultValue;
            } else {
                this.ts.setTime(((Long) object).longValue());
                return this.ts;
            }
        }
    }

    public static final class LongToTimestampTZ extends ConnectorDataTypeConverter {
        TimeZone targetTimezone;

        public LongToTimestampTZ(String targetTimezoneId) {
            if (targetTimezoneId == null || targetTimezoneId.isEmpty()) {
                targetTimezoneId = TimeZone.getDefault().getID();
            }
            this.targetTimezone = TimeZone.getTimeZone(targetTimezoneId);
        }

        public Object convert(Object object) {
            if (object == null) {
                return this.nullable ? null : this.defaultValue;
            } else {
                return new Timestamp(ConnectorDataTypeConverter.convertMillisecFromDefaultToTargetTZ(((Long) object).longValue(), this.targetTimezone));
            }
        }
    }

    public static final class LongToTinyInt extends ConnectorDataTypeConverter {
        public final Object convert(Object object) {
            if (object == null) {
                return this.nullable ? null : this.defaultValue;
            } else {
                return Byte.valueOf(((Long) object).byteValue());
            }
        }
    }

    public static class ObjectToAnyType extends ConnectorDataTypeConverter {
        Map<Long, ConnectorDataTypeConverter> map;
        int targetType;

        public Object convert(Object object) {
            int sourceType = ConnectorSchemaUtils.getGenericObjectType(object);
            if (sourceType == ConnectorDataTypeDefinition.TYPE_NULL) {
                sourceType = 12;
            }
            return ((ConnectorDataTypeConverter) this.map.get(Long.valueOf(ConnectorSchemaUtils.genConnectorMapKey(sourceType, this.targetType)))).convert(object);
        }

        public ObjectToAnyType(Map<Long, ConnectorDataTypeConverter> map, int targetType) {
            this.map = map;
            this.targetType = targetType;
        }

        public void setNullable(boolean nullable) {
            super.setNullable(nullable);
            for (Entry<Long, ConnectorDataTypeConverter> entry : this.map.entrySet()) {
                ((ConnectorDataTypeConverter) entry.getValue()).setNullable(nullable);
            }
        }
    }

    public static final class ObjectToJsonString extends ConnectorDataTypeConverter {
        public final Object convert(Object object) {
            if (object == null) {
                return this.nullable ? null : this.defaultValue;
            } else {
                StringWriter writer = new StringWriter();
                try {
                    this.objectMapper.writeValue(writer, object);
                    return writer.toString();
                } catch (Exception e) {
                    throw new RuntimeException(e.getMessage(), e);
                }
            }
        }
    }

    public static final class ObjectToString extends ConnectorDataTypeConverter {
        public final Object convert(Object object) {
            if (object == null) {
                return this.nullable ? null : this.defaultValue;
            } else {
                return object.toString();
            }
        }
    }

    public static final class Others extends ConnectorDataTypeConverter {
        public final Object convert(Object object) {
            return object == null ? null : object;
        }
    }

    public static final class PeriodToString extends ConnectorDataTypeConverter {
        public final Object convert(Object object) {
            if (object != null) {
                String result = object.toString();
                int bracketPos = result.indexOf(91);
                if (bracketPos > 0) {
                    result = result.substring(bracketPos + 1);
                    bracketPos = result.lastIndexOf(93);
                    if (bracketPos > 0) {
                        result = "(" + result.substring(0, bracketPos) + ")";
                    }
                }
                return result;
            } else if (this.nullable) {
                return null;
            } else {
                return this.defaultValue;
            }
        }
    }

    public static final class ShortToBigDecimal extends ConnectorDataTypeConverter {
        protected int scale;

        public void setScale(int scale) {
            this.scale = scale;
        }

        public final Object convert(Object object) {
            if (object == null) {
                return this.nullable ? null : (BigDecimal) this.defaultValue;
            } else {
                return new BigDecimal(((Short) object).shortValue());
            }
        }
    }

    public static final class ShortToBigDecimalWithScale extends ConnectorDataTypeConverter {
        protected int scale;

        public void setScale(int scale) {
            this.scale = scale;
        }

        public final Object convert(Object object) {
            if (object == null) {
                return this.nullable ? null : ((BigDecimal) this.defaultValue).setScale(this.scale, RoundingMode.HALF_UP);
            } else {
                return new BigDecimal(((Short) object).shortValue()).setScale(this.scale, RoundingMode.HALF_UP);
            }
        }
    }

    public static final class ShortToBoolean extends ConnectorDataTypeConverter {
        public final Object convert(Object object) {
            if (object == null) {
                return this.nullable ? null : this.defaultValue;
            } else {
                return Boolean.valueOf(((Short) object).shortValue() != (short) 0);
            }
        }
    }

    public static final class ShortToDouble extends ConnectorDataTypeConverter {
        public final Object convert(Object object) {
            if (object == null) {
                return this.nullable ? null : this.defaultValue;
            } else {
                return Double.valueOf(((Short) object).doubleValue());
            }
        }
    }

    public static final class ShortToFloat extends ConnectorDataTypeConverter {
        public final Object convert(Object object) {
            if (object == null) {
                return this.nullable ? null : this.defaultValue;
            } else {
                return Float.valueOf(((Short) object).floatValue());
            }
        }
    }

    public static final class ShortToInteger extends ConnectorDataTypeConverter {
        public final Object convert(Object object) {
            if (object == null) {
                return this.nullable ? null : this.defaultValue;
            } else {
                return Integer.valueOf(((Short) object).intValue());
            }
        }
    }

    public static final class ShortToLong extends ConnectorDataTypeConverter {
        public final Object convert(Object object) {
            if (object == null) {
                return this.nullable ? null : this.defaultValue;
            } else {
                return Long.valueOf(((Short) object).longValue());
            }
        }
    }

    public static final class ShortToShort extends ConnectorDataTypeConverter {
        public final Object convert(Object object) {
            if (object == null) {
                return this.nullable ? null : this.defaultValue;
            } else {
                return (Short) object;
            }
        }
    }

    public static final class ShortToString extends ConnectorDataTypeConverter {
        public final Object convert(Object object) {
            if (object == null) {
                return this.nullable ? null : this.defaultValue;
            } else {
                return object.toString();
            }
        }
    }

    public static final class ShortToTinyInt extends ConnectorDataTypeConverter {
        public final Object convert(Object object) {
            if (object == null) {
                return this.nullable ? null : this.defaultValue;
            } else {
                return Byte.valueOf(((Short) object).byteValue());
            }
        }
    }

    public static final class StringFMTTZToCalendarTime extends ConnectorDataTypeConverter {
        private List<DateFormat> backupDateFormat;
        private DateFormat df;
        private TimeZone sourceTimezone;
        private Calendar targetCalendar;
        private TimeZone targetTimezone;

        public ConnectorDataTypeConverter setupBackupDateFormat(Configuration conf, String confName) {
            List<String> values = ConnectorConfiguration.getAllConfigurationItems(conf, confName);
            this.backupDateFormat = new ArrayList();
            for (int i = 0; i < values.size(); i++) {
                this.backupDateFormat.add(ConnectorDataTypeConverter.createDateFormat(this.sourceTimezone, (String) values.get(i)));
            }
            return this;
        }

        public StringFMTTZToCalendarTime(String sourceFormat, String sourceTimezoneId, String targetTimezoneId) {
            if (sourceTimezoneId == null || sourceTimezoneId.isEmpty()) {
                sourceTimezoneId = TimeZone.getDefault().getID();
            }
            if (targetTimezoneId == null || targetTimezoneId.isEmpty()) {
                targetTimezoneId = TimeZone.getDefault().getID();
            }
            this.sourceTimezone = TimeZone.getTimeZone(sourceTimezoneId);
            this.targetTimezone = TimeZone.getTimeZone(targetTimezoneId);
            this.targetCalendar = Calendar.getInstance(this.targetTimezone);
            if (sourceFormat == null || sourceFormat.isEmpty()) {
                this.df = ConnectorDataTypeConverter.createDateFormat(this.sourceTimezone, ConnectorDataTypeConverter.DefaultTimeFormat);
            } else {
                this.df = ConnectorDataTypeConverter.createDateFormat(this.sourceTimezone, sourceFormat);
            }
        }

        public final Object convert(Object object) {
            java.util.Date d;
            if (object != null && !((String) object).isEmpty()) {
                try {
                    d = this.df.parse((String) object);
                } catch (ParseException e) {
                    d = ConnectorDataTypeConverter.parseStringWithFormatArray(this.backupDateFormat, (String) object);
                    if (d == null) {
                        throw new RuntimeException(e.getMessage(), e);
                    }
                }
                this.targetCalendar.setTimeInMillis(d.getTime());
                return this.targetCalendar;
            } else if (this.nullable) {
                return null;
            } else {
                return this.defaultValue;
            }
        }
    }

    public static final class StringFMTTZToCalendarTimestamp extends ConnectorDataTypeConverter {
        private List<DateFormat> backupDateFormat;
        private DateFormat df;
        private TimeZone sourceTimezone;
        private Calendar targetCalendar;
        private TimeZone targetTimezone;

        public ConnectorDataTypeConverter setupBackupDateFormat(Configuration conf, String confName) {
            List<String> values = ConnectorConfiguration.getAllConfigurationItems(conf, confName);
            this.backupDateFormat = new ArrayList();
            for (int i = 0; i < values.size(); i++) {
                this.backupDateFormat.add(ConnectorDataTypeConverter.createDateFormat(this.sourceTimezone, (String) values.get(i)));
            }
            return this;
        }

        public StringFMTTZToCalendarTimestamp(String sourceFormat, String sourceTimezoneId, String targetTimezoneId) {
            if (sourceTimezoneId == null || sourceTimezoneId.isEmpty()) {
                sourceTimezoneId = TimeZone.getDefault().getID();
            }
            if (targetTimezoneId == null || targetTimezoneId.isEmpty()) {
                targetTimezoneId = TimeZone.getDefault().getID();
            }
            this.sourceTimezone = TimeZone.getTimeZone(sourceTimezoneId);
            this.targetTimezone = TimeZone.getTimeZone(targetTimezoneId);
            this.targetCalendar = Calendar.getInstance(this.targetTimezone);
            if (sourceFormat == null || sourceFormat.isEmpty()) {
                this.df = null;
            } else {
                this.df = ConnectorDataTypeConverter.createDateFormat(this.sourceTimezone, sourceFormat);
            }
        }

        public final Object convert(Object object) {
            java.util.Date d;
            if (object != null && !((String) object).isEmpty()) {
                long millTime;
                if (this.df == null) {
                    try {
                        millTime = Timestamp.valueOf((String) object).getTime();
                    } catch (IllegalArgumentException e) {
                        try {
                            millTime = Date.valueOf((String) object).getTime();
                        } catch (IllegalArgumentException e2) {
                            try {
                                millTime = Time.valueOf((String) object).getTime();
                            } catch (IllegalArgumentException e22) {
                                throw e22;
                            }
                        }
                    }
                    millTime = ConnectorDataTypeConverter.convertMillisecFromSourceToDefaultTZ(millTime, this.sourceTimezone);
                } else {
                    try {
                        d = this.df.parse((String) object);
                    } catch (ParseException e3) {
                        d = ConnectorDataTypeConverter.parseStringWithFormatArray(this.backupDateFormat, (String) object);
                        if (d == null) {
                            throw new RuntimeException(e3.getMessage(), e3);
                        }
                    }
                    millTime = d.getTime();
                }
                this.targetCalendar.setTimeInMillis(millTime);
                return this.targetCalendar;
            } else if (this.nullable) {
                return null;
            } else {
                return this.defaultValue;
            }
        }
    }

    public static final class StringFMTTZToDateTZ extends ConnectorDataTypeConverter {
        private List<DateFormat> backupDateFormat = null;
        private DateFormat sourceDateFormat;
        private TimeZone sourceTimezone;
        private TimeZone targetTimezone;

        public StringFMTTZToDateTZ(String inputFormat, String sourceTimezoneId, String targetTimezoneId) {
            if (inputFormat == null || inputFormat.isEmpty()) {
                inputFormat = ConnectorDataTypeConverter.DefaultDateFormat;
            }
            if (sourceTimezoneId == null || sourceTimezoneId.isEmpty()) {
                sourceTimezoneId = TimeZone.getDefault().getID();
            }
            if (targetTimezoneId == null || targetTimezoneId.isEmpty()) {
                targetTimezoneId = TimeZone.getDefault().getID();
            }
            this.sourceTimezone = TimeZone.getTimeZone(sourceTimezoneId);
            this.targetTimezone = TimeZone.getTimeZone(targetTimezoneId);
            this.sourceDateFormat = ConnectorDataTypeConverter.createDateFormat(this.sourceTimezone, inputFormat);
        }

        public ConnectorDataTypeConverter setupBackupDateFormat(Configuration conf) {
            List<String> values = ConnectorConfiguration.getAllConfigurationItems(conf, ConnectorConfiguration.TDCH_INPUT_DATE_FORMAT);
            this.backupDateFormat = new ArrayList();
            for (int i = 0; i < values.size(); i++) {
                this.backupDateFormat.add(ConnectorDataTypeConverter.createDateFormat(this.sourceTimezone, (String) values.get(i)));
            }
            return this;
        }

        public final Object convert(Object object) {
            java.util.Date d;
            if (object != null && !((String) object).isEmpty()) {
                try {
                    d = this.sourceDateFormat.parse((String) object);
                } catch (ParseException e) {
                    d = ConnectorDataTypeConverter.parseStringWithFormatArray(this.backupDateFormat, (String) object);
                    if (d == null) {
                        throw new RuntimeException(e.getMessage(), e);
                    }
                }
                return new Date(ConnectorDataTypeConverter.convertMillisecFromDefaultToTargetTZ(d.getTime(), this.targetTimezone));
            } else if (this.nullable) {
                return null;
            } else {
                return this.defaultValue;
            }
        }
    }

    public static final class StringFMTTZToTimeTZ extends ConnectorDataTypeConverter {
        private List<DateFormat> backupDateFormat;
        private DateFormat sourceDateFormat;
        private TimeZone sourceTimezone;
        private TimeZone targetTimezone;

        public StringFMTTZToTimeTZ(String inputFormat, String sourceTimezoneId, String targetTimezoneId) {
            if (inputFormat == null || inputFormat.isEmpty()) {
                inputFormat = ConnectorDataTypeConverter.DefaultTimeFormat;
            }
            if (sourceTimezoneId == null || sourceTimezoneId.isEmpty()) {
                sourceTimezoneId = TimeZone.getDefault().getID();
            }
            if (targetTimezoneId == null || targetTimezoneId.isEmpty()) {
                targetTimezoneId = TimeZone.getDefault().getID();
            }
            this.sourceTimezone = TimeZone.getTimeZone(sourceTimezoneId);
            this.targetTimezone = TimeZone.getTimeZone(targetTimezoneId);
            this.sourceDateFormat = ConnectorDataTypeConverter.createDateFormat(this.sourceTimezone, inputFormat);
        }

        public ConnectorDataTypeConverter setupBackupDateFormat(Configuration conf) {
            List<String> values = ConnectorConfiguration.getAllConfigurationItems(conf, ConnectorConfiguration.TDCH_INPUT_TIME_FORMAT);
            this.backupDateFormat = new ArrayList();
            for (int i = 0; i < values.size(); i++) {
                this.backupDateFormat.add(ConnectorDataTypeConverter.createDateFormat(this.sourceTimezone, (String) values.get(i)));
            }
            return this;
        }

        public final Object convert(Object object) {
            java.util.Date d;
            if (object != null && !((String) object).isEmpty()) {
                try {
                    d = this.sourceDateFormat.parse((String) object);
                } catch (ParseException e) {
                    d = ConnectorDataTypeConverter.parseStringWithFormatArray(this.backupDateFormat, (String) object);
                    if (d == null) {
                        throw new RuntimeException(e.getMessage(), e);
                    }
                }
                return new Time(ConnectorDataTypeConverter.convertMillisecFromDefaultToTargetTZ(d.getTime(), this.targetTimezone));
            } else if (this.nullable) {
                return null;
            } else {
                return this.defaultValue;
            }
        }
    }

    public static final class StringFMTTZToTimestampTZ extends ConnectorDataTypeConverter {
        private TimeZone sourceTimezone = null;
        private TimeZone targetTimezone = null;
        private List<TimestampParser> tsParsers = null;

        private interface TimestampParser {
            Timestamp parse(String str, TimeZone timeZone, TimeZone timeZone2) throws ParseException;
        }

        public static class TimestampParsers {

            private static class DateFormatTimestampParser implements TimestampParser {
                private int numfracseconds;
                private DateFormat sourceDateFormat;

                private DateFormatTimestampParser(String sourceFMT) {
                    int i;
                    int i2 = 0;
                    this.numfracseconds = 0;
                    this.sourceDateFormat = null;
                    if (sourceFMT.indexOf(83) == -1) {
                        i = 0;
                    } else {
                        i = (sourceFMT.lastIndexOf(83) - sourceFMT.indexOf(83)) + 1;
                    }
                    this.numfracseconds = i;
                    if (this.numfracseconds != 3) {
                        i2 = this.numfracseconds;
                    }
                    this.numfracseconds = i2;
                    this.sourceDateFormat = new SimpleDateFormat(sourceFMT);
                }

                public Timestamp parse(String source, TimeZone inputTZ, TimeZone outputTZ) throws ParseException {
                    this.sourceDateFormat.setTimeZone(inputTZ);
                    long milliSec = ConnectorDataTypeConverter.convertMillisecFromDefaultToTargetTZ(this.sourceDateFormat.parse(source).getTime(), outputTZ);
                    Timestamp ts = new Timestamp(milliSec);
                    if (this.numfracseconds != 0) {
                        ts.setNanos((int) (((double) Math.abs(milliSec % 1000)) * Math.pow(10.0d, (double) (9 - this.numfracseconds))));
                    }
                    String nanos = ts.getNanos() + "";
                    int nanLength = nanos.length();
                    if (nanLength == 8) {
                        nanos = nanos + "0";
                    } else if (nanLength == 7) {
                        nanos = nanos + "00";
                    }
                    ts.setNanos(Integer.parseInt(nanos));
                    return ts;
                }
            }

            private static class JodaTimestampParser implements TimestampParser {
                private boolean containsZone;
                private DateTimeFormatter dtf;
                private int numfracseconds;

                private JodaTimestampParser(String sourceFMT) {
                    boolean z = false;
                    this.containsZone = false;
                    this.numfracseconds = 0;
                    this.dtf = null;
                    if (sourceFMT.contains("Z")) {
                        z = true;
                    }
                    this.containsZone = z;
                    this.numfracseconds = (sourceFMT.lastIndexOf(83) - sourceFMT.indexOf("SSSS")) + 1;
                    this.dtf = DateTimeFormat.forPattern(sourceFMT);
                }

                public Timestamp parse(String sourceTS, TimeZone sourceTZ, TimeZone targetTZ) {
                    DateTime dt;
                    if (this.containsZone) {
                        dt = this.dtf.parseDateTime(sourceTS);
                    } else {
                        dt = this.dtf.parseLocalDateTime(sourceTS).toDateTime(DateTimeZone.forTimeZone(sourceTZ));
                    }
                    long time = dt.getMillis();
                    String millis = "" + (time % 1000);
                    String fracseconds = sourceTS.substring(sourceTS.indexOf(millis), sourceTS.indexOf(millis) + this.numfracseconds);
                    Timestamp ts = new Timestamp(ConnectorDataTypeConverter.convertMillisecFromDefaultToTargetTZ(time, targetTZ));
                    ts.setNanos(Integer.parseInt(fracseconds) * ((int) Math.pow(10.0d, (double) (9 - this.numfracseconds))));
                    return ts;
                }
            }

            private TimestampParsers() {
            }

            public static TimestampParser createTimestampParser(String sourceFMT) {
                if (sourceFMT.matches(".*S{4,}.*")) {
                    return new JodaTimestampParser(sourceFMT);
                }
                return new DateFormatTimestampParser(sourceFMT);
            }
        }

        public StringFMTTZToTimestampTZ(String inputFormat, String sourceTimezoneId, String targetTimezoneId) {
            if (inputFormat == null || inputFormat.isEmpty()) {
                inputFormat = ConnectorDataTypeConverter.DefaultTimestampFormat;
            }
            if (sourceTimezoneId == null || sourceTimezoneId.isEmpty()) {
                sourceTimezoneId = TimeZone.getDefault().getID();
            }
            if (targetTimezoneId == null || targetTimezoneId.isEmpty()) {
                targetTimezoneId = TimeZone.getDefault().getID();
            }
            this.sourceTimezone = TimeZone.getTimeZone(sourceTimezoneId);
            this.targetTimezone = TimeZone.getTimeZone(targetTimezoneId);
            this.tsParsers = new ArrayList();
            this.tsParsers.add(TimestampParsers.createTimestampParser(inputFormat));
        }

        public ConnectorDataTypeConverter setupBackupDateFormat(Configuration conf) {
            List<String> values = ConnectorConfiguration.getAllConfigurationItems(conf, ConnectorConfiguration.TDCH_INPUT_TIMESTAMP_FORMAT);
            for (int i = 0; i < values.size(); i++) {
                this.tsParsers.add(TimestampParsers.createTimestampParser((String) values.get(i)));
            }
            return this;
        }

        public final Object convert(Object object) {
            if (object == null || ((String) object).isEmpty()) {
                Object obj;
                if (this.nullable) {
                    obj = null;
                } else {
                    obj = this.defaultValue;
                }
                return obj;
            }
            String source = (String) object;
            Object ts = null;
            Exception parseException = null;
            for (TimestampParser parser : this.tsParsers) {
                try {
                    ts = parser.parse(source, this.sourceTimezone, this.targetTimezone);
                    break;
                } catch (ParseException e) {
                    parseException = e;
                } catch (IllegalArgumentException e2) {
                    parseException = e2;
                } catch (Exception e22) {
                    parseException = e22;
                }
            }
            if (ts != null) {
                return ts;
            }
            throw new RuntimeException(parseException.getMessage(), parseException);
        }
    }

    public static final class StringToBigDecimal extends ConnectorDataTypeConverter {
        protected int scale;

        public void setScale(int scale) {
            this.scale = scale;
        }

        public final Object convert(Object object) {
            if (object == null || ((String) object).isEmpty()) {
                return this.nullable ? null : (BigDecimal) this.defaultValue;
            } else {
                return new BigDecimal((String) object);
            }
        }
    }

    public static final class StringToBigDecimalUnbounded extends ConnectorDataTypeConverter {
        protected int length;
        protected int precision;
        protected int scale;

        public void setScale(int scale) {
            this.scale = scale;
        }

        public void setPrecision(int precision) {
            this.precision = precision;
        }

        public void setLength(int length) {
            this.length = length;
        }

        public final Object convert(Object object) {
            if (object == null || ((String) object).isEmpty() || ((String) object).toUpperCase().equals("NULL")) {
                return this.nullable ? null : ((BigDecimal) this.defaultValue).setScale(this.scale, RoundingMode.HALF_UP);
            } else {
                if (this.precision == 40 && this.length == 47 && this.scale == 0) {
                    this.scale = 10;
                }
                return new BigDecimal((String) object).setScale(this.scale, RoundingMode.HALF_UP);
            }
        }
    }

    public static final class StringToBigDecimalWithScale extends ConnectorDataTypeConverter {
        protected int scale;

        public void setScale(int scale) {
            this.scale = scale;
        }

        public final Object convert(Object object) {
            if (object == null || ((String) object).isEmpty()) {
                return this.nullable ? null : ((BigDecimal) this.defaultValue).setScale(this.scale, RoundingMode.HALF_UP);
            } else {
                return new BigDecimal((String) object).setScale(this.scale, RoundingMode.HALF_UP);
            }
        }
    }

    public static final class StringToBinary extends ConnectorDataTypeConverter {
        public final Object convert(Object object) {
            if (object == null || ((String) object).isEmpty()) {
                Object obj;
                if (this.nullable) {
                    obj = null;
                } else {
                    obj = this.defaultValue;
                }
                return obj;
            }
            String value = object.toString();
            int len = value.length();
            switch (len) {
                case 0:
                    return this.defaultValue;
                case 1:
                    return new byte[]{(byte) Character.digit(value.charAt(0), 16)};
                default:
                    byte[] bytesData = new byte[(len / 2)];
                    for (int i = 0; i < len; i += 2) {
                        bytesData[i / 2] = (byte) ((Character.digit(value.charAt(i), 16) << 4) + Character.digit(value.charAt(i + 1), 16));
                    }
                    return bytesData;
            }
        }
    }

    public static final class StringToBoolean extends ConnectorDataTypeConverter {
        public final Object convert(Object object) {
            if (object == null || ((String) object).isEmpty()) {
                return this.nullable ? null : this.defaultValue;
            } else {
                return Boolean.valueOf((String) object);
            }
        }
    }

    public static final class StringToBytes extends ConnectorDataTypeConverter {
        public byte[] toBytes(String str) {
            char[] buffer = str.toCharArray();
            byte[] b = new byte[(buffer.length << 1)];
            for (int i = 0; i < buffer.length; i++) {
                int bpos = i << 1;
                b[bpos] = (byte) ((buffer[i] & 65280) >> 8);
                b[bpos + 1] = (byte) (buffer[i] & 255);
            }
            return b;
        }

        public final Object convert(Object object) {
            if (object == null || ((String) object).isEmpty()) {
                return this.nullable ? null : this.defaultValue;
            } else {
                String value = object.toString();
                switch (value.length()) {
                    case 0:
                        return this.defaultValue;
                    default:
                        return toBytes(value);
                }
            }
        }
    }

    @Deprecated
    public static final class StringToDate extends ConnectorDataTypeConverter {
        public final Object convert(Object object) {
            if (object == null || ((String) object).isEmpty()) {
                return this.nullable ? null : this.defaultValue;
            } else {
                return Date.valueOf((String) object);
            }
        }
    }

    @Deprecated
    public static final class StringToDateWithFormat extends ConnectorDataTypeConverter {
        private String dateFormat;

        public StringToDateWithFormat(String dateFormat) {
            this.dateFormat = dateFormat;
        }

        public final Object convert(Object object) {
            if (object == null || ((String) object).isEmpty()) {
                return this.nullable ? null : this.defaultValue;
            } else {
                try {
                    return new Date(new SimpleDateFormat(this.dateFormat).parse((String) object).getTime());
                } catch (ParseException e) {
                    throw new RuntimeException(e.getMessage(), e);
                }
            }
        }
    }

    public static final class StringToDouble extends ConnectorDataTypeConverter {
        public final Object convert(Object object) {
            if (object == null || ((String) object).isEmpty()) {
                return this.nullable ? null : this.defaultValue;
            } else {
                return Double.valueOf((String) object);
            }
        }
    }

    public static final class StringToFloat extends ConnectorDataTypeConverter {
        public final Object convert(Object object) {
            if (object == null || ((String) object).isEmpty()) {
                return this.nullable ? null : this.defaultValue;
            } else {
                return Float.valueOf((String) object);
            }
        }
    }

    public static final class StringToInteger extends ConnectorDataTypeConverter {
        public final Object convert(Object object) {
            if (object == null || ((String) object).isEmpty()) {
                return this.nullable ? null : this.defaultValue;
            } else {
                return Integer.valueOf((String) object);
            }
        }
    }

    public static final class StringToInterval extends ConnectorDataTypeConverter {
        public final Object convert(Object object) {
            if (object != null && !((String) object).isEmpty()) {
                return object;
            }
            return this.nullable ? null : this.defaultValue;
        }
    }

    public static final class StringToLong extends ConnectorDataTypeConverter {
        public final Object convert(Object object) {
            if (object == null || ((String) object).isEmpty()) {
                return this.nullable ? null : this.defaultValue;
            } else {
                return Long.valueOf((String) object);
            }
        }
    }

    public static final class StringToPeriod extends ConnectorDataTypeConverter {
        public final Object convert(Object object) {
            if (object != null && !((String) object).isEmpty()) {
                return object;
            }
            return this.nullable ? null : this.defaultValue;
        }
    }

    public static final class StringToShort extends ConnectorDataTypeConverter {
        public final Object convert(Object object) {
            if (object == null || ((String) object).isEmpty()) {
                return this.nullable ? null : this.defaultValue;
            } else {
                return Short.valueOf((String) object);
            }
        }
    }

    public static final class StringToString extends ConnectorDataTypeConverter {
        int length;
        boolean truncate;

        public StringToString() {
            setTruncate(true);
            setLength(ConnectorDataTypeDefinition.MAX_STRING_LENGTH);
        }

        public StringToString(boolean truncate, int length) {
            setTruncate(truncate);
            setLength(length);
        }

        public void setTruncate(boolean truncate) {
            this.truncate = truncate;
        }

        public void setLength(int length) {
            if (length < 1) {
                this.length = ConnectorDataTypeDefinition.MAX_STRING_LENGTH;
            } else {
                this.length = length;
            }
        }

        public final Object convert(Object object) {
            if (object == null) {
                if (this.nullable) {
                    return null;
                }
                return this.defaultValue;
            } else if (this.truncate) {
                String objstr = object.toString();
                return objstr.length() > this.length ? objstr.substring(0, this.length) : objstr;
            } else if (object.toString().length() <= this.length) {
                return object;
            } else {
                throw new RuntimeException(ErrorMessage.CONVERTER_STRING_TRUNCATE_ERROR);
            }
        }
    }

    @Deprecated
    public static final class StringToTime extends ConnectorDataTypeConverter {
        public final Object convert(Object object) {
            if (object == null || ((String) object).isEmpty()) {
                return this.nullable ? null : this.defaultValue;
            } else {
                return Time.valueOf((String) object);
            }
        }
    }

    @Deprecated
    public static final class StringToTimeWithFormat extends ConnectorDataTypeConverter {
        String dateFormat;

        public StringToTimeWithFormat(String dateFormat) {
            this.dateFormat = dateFormat;
        }

        public final Object convert(Object object) {
            if (object == null || ((String) object).isEmpty()) {
                return this.nullable ? null : this.defaultValue;
            } else {
                try {
                    return new Time(new SimpleDateFormat(this.dateFormat).parse((String) object).getTime());
                } catch (ParseException e) {
                    throw new RuntimeException(e.getMessage(), e);
                }
            }
        }
    }

    @Deprecated
    public static final class StringToTimestamp extends ConnectorDataTypeConverter {
        public final Object convert(Object object) {
            if (object == null || ((String) object).isEmpty()) {
                return this.nullable ? null : this.defaultValue;
            } else {
                return Timestamp.valueOf((String) object);
            }
        }
    }

    @Deprecated
    public static final class StringToTimestampTZ_d extends ConnectorDataTypeConverter {
        Calendar targetCalendar = Calendar.getInstance(this.targetTimeZone);
        TimeZone targetTimeZone;

        public StringToTimestampTZ_d(String targetTimeZoneId) {
            this.targetTimeZone = TimeZone.getTimeZone(targetTimeZoneId);
        }

        public final Object convert(Object object) {
            long currentTime = System.currentTimeMillis();
            int hourDifference = (TimeZone.getDefault().getOffset(currentTime) - this.targetTimeZone.getOffset(currentTime)) / 3600000;
            if (object == null) {
                return this.nullable ? null : this.defaultValue;
            } else {
                this.targetCalendar.setTimeInMillis(Timestamp.valueOf((String) object).getTime());
                this.targetCalendar.add(10, hourDifference);
                return new Timestamp(this.targetCalendar.getTimeInMillis());
            }
        }
    }

    @Deprecated
    public static final class StringToTimestampWithFormat extends ConnectorDataTypeConverter {
        protected String dateFormat;

        public StringToTimestampWithFormat(String dateFormat) {
            this.dateFormat = dateFormat;
        }

        public final Object convert(Object object) {
            if (object == null || ((String) object).isEmpty()) {
                return this.nullable ? null : this.defaultValue;
            } else {
                try {
                    return new Timestamp(new SimpleDateFormat(this.dateFormat).parse((String) object).getTime());
                } catch (ParseException e) {
                    throw new RuntimeException(e.getMessage(), e);
                }
            }
        }
    }

    public static final class StringToTinyInt extends ConnectorDataTypeConverter {
        public final Object convert(Object object) {
            if (object == null || ((String) object).isEmpty()) {
                return this.nullable ? null : this.defaultValue;
            } else {
                return Byte.valueOf((String) object);
            }
        }
    }

    public static final class TimeTZToCalendar extends ConnectorDataTypeConverter {
        private TimeZone sourceTimezone;
        private Calendar targetCalendar;

        public TimeTZToCalendar(String sourceTimezoneId, String targetTimezoneId) {
            if (sourceTimezoneId == null || sourceTimezoneId.isEmpty()) {
                sourceTimezoneId = TimeZone.getDefault().getID();
            }
            if (targetTimezoneId == null || targetTimezoneId.isEmpty()) {
                targetTimezoneId = TimeZone.getDefault().getID();
            }
            this.sourceTimezone = TimeZone.getTimeZone(sourceTimezoneId);
            this.targetCalendar = Calendar.getInstance(TimeZone.getTimeZone(targetTimezoneId));
        }

        public final Object convert(Object object) {
            if (object == null) {
                return this.nullable ? null : this.defaultValue;
            } else {
                this.targetCalendar.setTimeInMillis(ConnectorDataTypeConverter.convertMillisecFromSourceToDefaultTZ(((Time) object).getTime(), this.sourceTimezone));
                return this.targetCalendar;
            }
        }
    }

    public static final class TimeTZToInteger extends ConnectorDataTypeConverter {
        private TimeZone sourceTimezone;

        public TimeTZToInteger(String sourceTimezoneId) {
            if (sourceTimezoneId == null || sourceTimezoneId.isEmpty()) {
                sourceTimezoneId = TimeZone.getDefault().getID();
            }
            this.sourceTimezone = TimeZone.getTimeZone(sourceTimezoneId);
        }

        public final Object convert(Object object) {
            if (object == null) {
                return this.nullable ? null : this.defaultValue;
            } else {
                return Integer.valueOf((int) (ConnectorDataTypeConverter.convertMillisecFromSourceToDefaultTZ(((Time) object).getTime(), this.sourceTimezone) / 1000));
            }
        }
    }

    public static final class TimeTZToLong extends ConnectorDataTypeConverter {
        private TimeZone sourceTimezone;

        public TimeTZToLong(String sourceTimezoneId) {
            if (sourceTimezoneId == null || sourceTimezoneId.isEmpty()) {
                sourceTimezoneId = TimeZone.getDefault().getID();
            }
            this.sourceTimezone = TimeZone.getTimeZone(sourceTimezoneId);
        }

        public final Object convert(Object object) {
            if (object == null) {
                return this.nullable ? null : this.defaultValue;
            } else {
                return Long.valueOf(ConnectorDataTypeConverter.convertMillisecFromSourceToDefaultTZ(((Time) object).getTime(), this.sourceTimezone));
            }
        }
    }

    public static final class TimeTZToStringFMTTZ extends ConnectorDataTypeConverter {
        private DateFormat df;
        private boolean isSetDefaultTimeFormat;
        private int offset;
        private Time time;

        public TimeTZToStringFMTTZ(String targetFormat, String sourceTimezoneId, String targetTimezoneId) {
            if (sourceTimezoneId == null || sourceTimezoneId.isEmpty()) {
                sourceTimezoneId = TimeZone.getDefault().getID();
            }
            if (targetTimezoneId == null || targetTimezoneId.isEmpty()) {
                targetTimezoneId = TimeZone.getDefault().getID();
            }
            if (targetFormat == null || targetFormat.isEmpty()) {
                targetFormat = ConnectorDataTypeConverter.DefaultTimeFormat;
                this.isSetDefaultTimeFormat = true;
            }
            TimeZone sourceTimezone = TimeZone.getTimeZone(sourceTimezoneId);
            TimeZone targetTimezone = TimeZone.getTimeZone(targetTimezoneId);
            this.offset = TimeZone.getDefault().getOffset(System.currentTimeMillis()) - sourceTimezone.getOffset(System.currentTimeMillis());
            this.df = ConnectorDataTypeConverter.createDateFormat(targetTimezone, targetFormat);
            this.time = new Time(0);
        }

        public final Object convert(Object object) {
            if (object == null) {
                if (this.nullable) {
                    return null;
                }
                return this.defaultValue;
            } else if (object instanceof Time) {
                this.time.setTime(((Time) object).getTime() + ((long) this.offset));
                return this.df.format(this.time);
            } else if (!(object instanceof Timestamp)) {
                return this.df.format(object);
            } else {
                Timestamp ts = (Timestamp) object;
                int nanos = ts.getNanos();
                Timestamp timestamp = new Timestamp(ts.getTime() + ((long) this.offset));
                timestamp.setNanos(nanos);
                if (this.isSetDefaultTimeFormat) {
                    return this.df.format(timestamp) + "." + removeTrailingZeroes(nanos);
                }
                return this.df.format(timestamp);
            }
        }

        private int removeTrailingZeroes(int n) {
            while (n != 0 && n % 10 == 0) {
                n /= 10;
            }
            return n;
        }
    }

    public static final class TimeTZToTimeTZ extends ConnectorDataTypeConverter {
        private int offset;

        public TimeTZToTimeTZ(String sourceTimezoneId, String targetTimezoneId) {
            if (sourceTimezoneId == null || sourceTimezoneId.isEmpty()) {
                sourceTimezoneId = TimeZone.getDefault().getID();
            }
            if (targetTimezoneId == null || targetTimezoneId.isEmpty()) {
                targetTimezoneId = TimeZone.getDefault().getID();
            }
            this.offset = TimeZone.getTimeZone(targetTimezoneId).getOffset(System.currentTimeMillis()) - TimeZone.getTimeZone(sourceTimezoneId).getOffset(System.currentTimeMillis());
        }

        public final Object convert(Object object) {
            if (object == null) {
                return this.nullable ? null : this.defaultValue;
            } else {
                return new Time(((Time) object).getTime() + ((long) this.offset));
            }
        }
    }

    @Deprecated
    public static final class TimeToString extends ConnectorDataTypeConverter {
        public final Object convert(Object object) {
            if (object == null) {
                return this.nullable ? null : this.defaultValue;
            } else {
                return object.toString();
            }
        }
    }

    @Deprecated
    public static final class TimeToStringWithFormat extends ConnectorDataTypeConverter {
        String dateFormat;

        public TimeToStringWithFormat(String dateFormat) {
            this.dateFormat = dateFormat;
        }

        public final Object convert(Object object) {
            if (object == null) {
                return this.nullable ? null : this.defaultValue;
            } else {
                return new SimpleDateFormat(this.dateFormat).format((Time) object);
            }
        }
    }

    @Deprecated
    public static final class TimeToTime extends ConnectorDataTypeConverter {
        public final Object convert(Object object) {
            if (object == null) {
                return this.nullable ? null : this.defaultValue;
            } else {
                return object;
            }
        }
    }

    @Deprecated
    public static final class TimeToTimeWithTimeZone extends ConnectorDataTypeConverter {
        Calendar targetCalendar = Calendar.getInstance(this.targetTimeZone);
        TimeZone targetTimeZone;

        public TimeToTimeWithTimeZone(String timeZoneId) {
            this.targetTimeZone = TimeZone.getTimeZone(timeZoneId);
        }

        public final Object convert(Object object) {
            long currentTime = System.currentTimeMillis();
            int hourDifference = (TimeZone.getDefault().getOffset(currentTime) - this.targetTimeZone.getOffset(currentTime)) / 3600000;
            if (object == null) {
                return this.nullable ? null : this.defaultValue;
            } else {
                this.targetCalendar.setTimeInMillis(((Time) object).getTime());
                this.targetCalendar.add(10, hourDifference);
                return new Time(this.targetCalendar.getTimeInMillis());
            }
        }
    }

    public static final class TimeToTimestamp extends ConnectorDataTypeConverter {
        public final Object convert(Object object) {
            if (object != null) {
                Time t = (Time) object;
                java.util.Date currentDate = new java.util.Date();
                Calendar cal = Calendar.getInstance();
                cal.setTime(currentDate);
                return Timestamp.valueOf(new SimpleDateFormat(ConnectorDataTypeConverter.DefaultDateFormat).format(cal.getTime()) + " " + t.toString());
            } else if (this.nullable) {
                return null;
            } else {
                return this.defaultValue;
            }
        }
    }

    public static final class TimestampTZToCalendar extends ConnectorDataTypeConverter {
        private TimeZone sourceTimezone;
        private Calendar targetCalendar;

        public TimestampTZToCalendar(String sourceTimezoneId, String targetTimezoneId) {
            if (sourceTimezoneId == null || sourceTimezoneId.isEmpty()) {
                sourceTimezoneId = TimeZone.getDefault().getID();
            }
            if (targetTimezoneId == null || targetTimezoneId.isEmpty()) {
                targetTimezoneId = TimeZone.getDefault().getID();
            }
            this.sourceTimezone = TimeZone.getTimeZone(sourceTimezoneId);
            this.targetCalendar = Calendar.getInstance(TimeZone.getTimeZone(targetTimezoneId));
        }

        public final Object convert(Object object) {
            if (object == null) {
                return this.nullable ? null : this.defaultValue;
            } else {
                this.targetCalendar.setTimeInMillis(ConnectorDataTypeConverter.convertMillisecFromSourceToDefaultTZ(((Timestamp) object).getTime(), this.sourceTimezone));
                return this.targetCalendar;
            }
        }
    }

    @Deprecated
    public static final class TimestampTZToDate extends ConnectorDataTypeConverter {
        Calendar targetCalendar = Calendar.getInstance(this.targetTimeZone);
        TimeZone targetTimeZone;

        public TimestampTZToDate(String timeZoneId) {
            this.targetTimeZone = TimeZone.getTimeZone(timeZoneId);
        }

        public final Object convert(Object object) {
            long currentTime = System.currentTimeMillis();
            int hourDifference = (TimeZone.getDefault().getOffset(currentTime) - this.targetTimeZone.getOffset(currentTime)) / 3600000;
            if (object == null) {
                return this.nullable ? null : this.defaultValue;
            } else {
                this.targetCalendar.setTimeInMillis(((Timestamp) object).getTime());
                this.targetCalendar.add(10, hourDifference);
                return new Date(this.targetCalendar.getTimeInMillis());
            }
        }
    }

    public static final class TimestampTZToDateTZ extends ConnectorDataTypeConverter {
        private int offset;

        public TimestampTZToDateTZ(String sourceTimezoneId, String targetTimezoneId) {
            if (sourceTimezoneId == null || sourceTimezoneId.isEmpty()) {
                sourceTimezoneId = TimeZone.getDefault().getID();
            }
            if (targetTimezoneId == null || targetTimezoneId.isEmpty()) {
                targetTimezoneId = TimeZone.getDefault().getID();
            }
            this.offset = TimeZone.getTimeZone(targetTimezoneId).getOffset(System.currentTimeMillis()) - TimeZone.getTimeZone(sourceTimezoneId).getOffset(System.currentTimeMillis());
        }

        public final Object convert(Object object) {
            if (object == null) {
                return this.nullable ? null : this.defaultValue;
            } else {
                return new Date(((Timestamp) object).getTime() + ((long) this.offset));
            }
        }
    }

    public static final class TimestampTZToInteger extends ConnectorDataTypeConverter {
        private TimeZone sourceTimezone;

        public TimestampTZToInteger(String sourceTimezoneId) {
            if (sourceTimezoneId == null || sourceTimezoneId.isEmpty()) {
                sourceTimezoneId = TimeZone.getDefault().getID();
            }
            this.sourceTimezone = TimeZone.getTimeZone(sourceTimezoneId);
        }

        public final Object convert(Object object) {
            if (object == null) {
                return this.nullable ? null : this.defaultValue;
            } else {
                return Integer.valueOf((int) (ConnectorDataTypeConverter.convertMillisecFromSourceToDefaultTZ(((Timestamp) object).getTime(), this.sourceTimezone) / 1000));
            }
        }
    }

    public static final class TimestampTZToLong extends ConnectorDataTypeConverter {
        private TimeZone sourceTimezone;

        public TimestampTZToLong(String sourceTimezoneId) {
            if (sourceTimezoneId == null || sourceTimezoneId.isEmpty()) {
                sourceTimezoneId = TimeZone.getDefault().getID();
            }
            this.sourceTimezone = TimeZone.getTimeZone(sourceTimezoneId);
        }

        public final Object convert(Object object) {
            if (object == null) {
                return this.nullable ? null : this.defaultValue;
            } else {
                return Long.valueOf(ConnectorDataTypeConverter.convertMillisecFromSourceToDefaultTZ(((Timestamp) object).getTime(), this.sourceTimezone));
            }
        }
    }

    @Deprecated
    public static final class TimestampTZToString extends ConnectorDataTypeConverter {
        Calendar targetCalendar = Calendar.getInstance(this.targetTimeZone);
        TimeZone targetTimeZone;

        public TimestampTZToString(String timeZoneId) {
            this.targetTimeZone = TimeZone.getTimeZone(timeZoneId);
        }

        public final Object convert(Object object) {
            long currentTime = System.currentTimeMillis();
            int hourDifference = (TimeZone.getDefault().getOffset(currentTime) - this.targetTimeZone.getOffset(currentTime)) / 3600000;
            if (object == null) {
                return this.nullable ? null : this.defaultValue;
            } else {
                this.targetCalendar.setTimeInMillis(((Timestamp) object).getTime());
                this.targetCalendar.add(10, hourDifference);
                return new Timestamp(this.targetCalendar.getTimeInMillis()).toString();
            }
        }
    }

    public static final class TimestampTZToStringFMTTZ extends ConnectorDataTypeConverter {
        private DateFormat df;
        private int firstIndexofS;
        private int iNanoLen;
        private int lastIndexofS;
        private int offset;
        private String origTargetFormat;
        private TimeZone targetTimezone;
        private Timestamp timestamp;
        private boolean tz;

        public TimestampTZToStringFMTTZ(String sourceTimezoneId, String targetTimezoneId, String targetFormat) {
            if (targetFormat == null || targetFormat.isEmpty()) {
                targetFormat = ConnectorDataTypeConverter.DefaultTimestampFormatStr;
            }
            this.origTargetFormat = targetFormat;
            if (targetFormat.indexOf(90) > -1) {
                this.tz = true;
            }
            if (targetFormat.indexOf(83) > -1) {
                this.firstIndexofS = targetFormat.indexOf(83);
                this.lastIndexofS = targetFormat.lastIndexOf(83) + 1;
                this.iNanoLen = this.lastIndexofS - this.firstIndexofS;
            }
            if (sourceTimezoneId == null || sourceTimezoneId.isEmpty()) {
                sourceTimezoneId = TimeZone.getDefault().getID();
            }
            if (targetTimezoneId == null || targetTimezoneId.isEmpty()) {
                targetTimezoneId = TimeZone.getDefault().getID();
            }
            TimeZone sourceTimezone = TimeZone.getTimeZone(sourceTimezoneId);
            this.targetTimezone = TimeZone.getTimeZone(targetTimezoneId);
            this.offset = TimeZone.getDefault().getOffset(System.currentTimeMillis()) - sourceTimezone.getOffset(System.currentTimeMillis());
            this.timestamp = new Timestamp(0);
            this.df = ConnectorDataTypeConverter.createDateFormat(this.targetTimezone, targetFormat);
        }

        public final Object convert(Object object) {
            if (object != null) {
                if (this.firstIndexofS != 0) {
                    try {
                        String interTimestamp;
                        String finalTimestamp;
                        this.timestamp = new Timestamp(((Timestamp) object).getTime() + ((long) this.offset));
                        int iNano = ((Timestamp) object).getNanos();
                        if (iNano != 0) {
                            this.timestamp.setNanos(iNano);
                        }
                        String finalNanosStr = "";
                        int origlastIndexofS = this.lastIndexofS;
                        int tempLastIndexofS = this.lastIndexofS;
                        int tempNanoLen = this.iNanoLen;
                        int i = 0;
                        if (origlastIndexofS != 0 && this.firstIndexofS != 0) {
                            do {
                                i++;
                                try {
                                    finalNanosStr = this.timestamp.toString().substring(this.firstIndexofS, tempLastIndexofS);
                                } catch (Exception e) {
                                    tempLastIndexofS--;
                                    tempNanoLen--;
                                }
                                if (this.firstIndexofS != tempLastIndexofS && !finalNanosStr.equals("")) {
                                    break;
                                }
                            } while (i <= 6);
                        }
                        if (origlastIndexofS > tempLastIndexofS) {
                            String strReplace = "";
                            do {
                                strReplace = "S" + strReplace;
                                origlastIndexofS--;
                            } while (origlastIndexofS > tempLastIndexofS);
                            interTimestamp = ConnectorDataTypeConverter.createDateFormat(this.targetTimezone, this.origTargetFormat.replaceFirst(strReplace, "")).format(this.timestamp);
                        } else {
                            interTimestamp = this.df.format(this.timestamp);
                        }
                        String interNanosStr = interTimestamp.substring(this.firstIndexofS, tempLastIndexofS);
                        if (tempNanoLen > 3) {
                            finalTimestamp = interTimestamp.replace(interNanosStr, finalNanosStr);
                        } else {
                            DateFormat dfNew;
                            String replacef = "";
                            String replaces = "";
                            if (this.iNanoLen == 6) {
                                replacef = "SSS";
                                if (this.origTargetFormat.indexOf(".S") > -1) {
                                    replaces = ".SSSSSS";
                                } else {
                                    replaces = "SSSSSS";
                                }
                            } else if (this.iNanoLen == 5) {
                                replacef = "SS";
                                if (this.origTargetFormat.indexOf(".S") > -1) {
                                    replaces = ".SSSSS";
                                } else {
                                    replaces = "SSSSS";
                                }
                            } else if (this.iNanoLen == 4) {
                                replacef = "S";
                                if (this.origTargetFormat.indexOf(".S") > -1) {
                                    replaces = ".SSSS";
                                } else {
                                    replaces = "SSSS";
                                }
                            } else if (this.iNanoLen == 3) {
                                if (this.origTargetFormat.indexOf(".S") > -1) {
                                    replaces = ".SSS";
                                } else {
                                    replaces = "SSS";
                                }
                            } else if (this.iNanoLen == 2) {
                                if (this.origTargetFormat.indexOf(".S") > -1) {
                                    replaces = ".SS";
                                } else {
                                    replaces = "SS";
                                }
                            } else if (this.iNanoLen == 1) {
                                if (this.origTargetFormat.indexOf(".S") > -1) {
                                    replaces = ".S";
                                } else {
                                    replaces = "S";
                                }
                            }
                            if (iNano != 0) {
                                dfNew = ConnectorDataTypeConverter.createDateFormat(this.targetTimezone, this.origTargetFormat.replaceFirst(replacef, ""));
                            } else {
                                dfNew = ConnectorDataTypeConverter.createDateFormat(this.targetTimezone, this.origTargetFormat.replaceFirst(replaces, ""));
                            }
                            finalTimestamp = dfNew.format(this.timestamp);
                        }
                        return finalTimestamp;
                    } catch (Exception e2) {
                    }
                }
                this.timestamp.setTime(((Timestamp) object).getTime() + ((long) this.offset));
                return this.df.format(this.timestamp);
            } else if (this.nullable) {
                return null;
            } else {
                return this.defaultValue;
            }
        }
    }

    @Deprecated
    public static final class TimestampTZToTime extends ConnectorDataTypeConverter {
        Calendar targetCalendar = Calendar.getInstance(this.targetTimeZone);
        TimeZone targetTimeZone;

        public TimestampTZToTime(String timeZoneId) {
            this.targetTimeZone = TimeZone.getTimeZone(timeZoneId);
        }

        public final Object convert(Object object) {
            long currentTime = System.currentTimeMillis();
            int hourDifference = (TimeZone.getDefault().getOffset(currentTime) - this.targetTimeZone.getOffset(currentTime)) / 3600000;
            if (object == null) {
                return this.nullable ? null : this.defaultValue;
            } else {
                this.targetCalendar.setTimeInMillis(((Timestamp) object).getTime());
                this.targetCalendar.add(10, hourDifference);
                return new Time(this.targetCalendar.getTimeInMillis());
            }
        }
    }

    public static final class TimestampTZToTimeTZ extends ConnectorDataTypeConverter {
        private int offset;

        public TimestampTZToTimeTZ(String sourceTimezoneId, String targetTimezoneId) {
            if (sourceTimezoneId == null || sourceTimezoneId.isEmpty()) {
                sourceTimezoneId = TimeZone.getDefault().getID();
            }
            if (targetTimezoneId == null || targetTimezoneId.isEmpty()) {
                targetTimezoneId = TimeZone.getDefault().getID();
            }
            this.offset = TimeZone.getTimeZone(targetTimezoneId).getOffset(System.currentTimeMillis()) - TimeZone.getTimeZone(sourceTimezoneId).getOffset(System.currentTimeMillis());
        }

        public final Object convert(Object object) {
            if (object == null) {
                return this.nullable ? null : this.defaultValue;
            } else {
                return new Time(((Timestamp) object).getTime() + ((long) this.offset));
            }
        }
    }

    @Deprecated
    public static final class TimestampTZToTimestamp extends ConnectorDataTypeConverter {
        Calendar targetCalendar = Calendar.getInstance(this.targetTimeZone);
        TimeZone targetTimeZone;

        public TimestampTZToTimestamp(String timeZoneId) {
            this.targetTimeZone = TimeZone.getTimeZone(timeZoneId);
        }

        public final Object convert(Object object) {
            long currentTime = System.currentTimeMillis();
            int hourDifference = (TimeZone.getDefault().getOffset(currentTime) - this.targetTimeZone.getOffset(currentTime)) / 3600000;
            if (object == null) {
                return this.nullable ? null : this.defaultValue;
            } else {
                this.targetCalendar.setTimeInMillis(((Timestamp) object).getTime());
                this.targetCalendar.add(10, hourDifference);
                return new Timestamp(this.targetCalendar.getTimeInMillis());
            }
        }
    }

    public static final class TimestampTZToTimestampTZ extends ConnectorDataTypeConverter {
        private int offset;

        public TimestampTZToTimestampTZ(String sourceTimezoneId, String targetTimezoneId) {
            if (sourceTimezoneId == null || sourceTimezoneId.isEmpty()) {
                sourceTimezoneId = TimeZone.getDefault().getID();
            }
            if (targetTimezoneId == null || targetTimezoneId.isEmpty()) {
                targetTimezoneId = TimeZone.getDefault().getID();
            }
            this.offset = TimeZone.getTimeZone(targetTimezoneId).getOffset(System.currentTimeMillis()) - TimeZone.getTimeZone(sourceTimezoneId).getOffset(System.currentTimeMillis());
        }

        public final Object convert(Object object) {
            if (object == null) {
                return this.nullable ? null : this.defaultValue;
            } else {
                Timestamp ts = new Timestamp(((Timestamp) object).getTime() + ((long) this.offset));
                ts.setNanos(((Timestamp) object).getNanos());
                return ts;
            }
        }
    }

    @Deprecated
    public static final class TimestampTZToTimestampTZ_d extends ConnectorDataTypeConverter {
        Calendar sourceCalendar = Calendar.getInstance(this.targetTimeZone);
        TimeZone sourceTimeZone;
        Calendar targetCalendar;
        TimeZone targetTimeZone;

        public TimestampTZToTimestampTZ_d(String sourceTimeZoneId, String targetTimeZoneId) {
            this.sourceTimeZone = TimeZone.getTimeZone(sourceTimeZoneId);
            this.targetTimeZone = TimeZone.getTimeZone(targetTimeZoneId);
            this.targetCalendar = Calendar.getInstance(this.targetTimeZone);
        }

        public final Object convert(Object object) {
            long currentTime = System.currentTimeMillis();
            int hourDifference = (this.sourceTimeZone.getOffset(currentTime) - this.targetTimeZone.getOffset(currentTime)) / 3600000;
            if (object == null) {
                return this.nullable ? null : this.defaultValue;
            } else {
                this.targetCalendar.setTimeInMillis(((Timestamp) object).getTime());
                this.targetCalendar.add(10, hourDifference);
                return new Timestamp(this.targetCalendar.getTimeInMillis());
            }
        }
    }

    @Deprecated
    public static final class TimestampToDate extends ConnectorDataTypeConverter {
        public final Object convert(Object object) {
            if (object == null) {
                return this.nullable ? null : this.defaultValue;
            } else {
                return new Date(((Timestamp) object).getTime());
            }
        }
    }

    @Deprecated
    public static final class TimestampToLong extends ConnectorDataTypeConverter {
        public final Object convert(Object object) {
            if (object == null) {
                return this.nullable ? null : this.defaultValue;
            } else {
                return Long.valueOf(((Timestamp) object).getTime());
            }
        }
    }

    public static final class TimestampToString extends ConnectorDataTypeConverter {
        public final Object convert(Object object) {
            if (object == null) {
                return this.nullable ? null : this.defaultValue;
            } else {
                return object.toString();
            }
        }
    }

    public static final class TimestampToStringWithFormat extends ConnectorDataTypeConverter {
        String dateFormat;

        public TimestampToStringWithFormat(String dateFormat) {
            this.dateFormat = dateFormat;
        }

        public final Object convert(Object object) {
            if (object == null) {
                return this.nullable ? null : this.defaultValue;
            } else {
                return new SimpleDateFormat(this.dateFormat).format((Timestamp) object);
            }
        }
    }

    @Deprecated
    public static final class TimestampToTime extends ConnectorDataTypeConverter {
        public final Object convert(Object object) {
            if (object == null) {
                return this.nullable ? null : this.defaultValue;
            } else {
                return new Time(((Timestamp) object).getTime());
            }
        }
    }

    @Deprecated
    public static final class TimestampToTimestamp extends ConnectorDataTypeConverter {
        public final Object convert(Object object) {
            if (object == null) {
                return this.nullable ? null : this.defaultValue;
            } else {
                return object;
            }
        }
    }

    @Deprecated
    public static final class TimestampToTimestampTZ extends ConnectorDataTypeConverter {
        Calendar targetCalendar = Calendar.getInstance(this.targetTimeZone);
        TimeZone targetTimeZone;

        public TimestampToTimestampTZ(String timeZoneId) {
            this.targetTimeZone = TimeZone.getTimeZone(timeZoneId);
        }

        public final Object convert(Object object) {
            long currentTime = System.currentTimeMillis();
            int hourDifference = (TimeZone.getDefault().getOffset(currentTime) - this.targetTimeZone.getOffset(currentTime)) / 3600000;
            if (object == null) {
                return this.nullable ? null : this.defaultValue;
            } else {
                this.targetCalendar.setTimeInMillis(((Timestamp) object).getTime());
                this.targetCalendar.add(10, hourDifference);
                return new Timestamp(this.targetCalendar.getTimeInMillis());
            }
        }
    }

    public static final class TimestampToTimestampWithTimeZone extends ConnectorDataTypeConverter {
        Calendar targetCalendar = Calendar.getInstance(this.targetTimeZone);
        TimeZone targetTimeZone;

        public TimestampToTimestampWithTimeZone(String timeZoneId) {
            this.targetTimeZone = TimeZone.getTimeZone(timeZoneId);
        }

        public final Object convert(Object object) {
            long currentTime = System.currentTimeMillis();
            int hourDifference = (TimeZone.getDefault().getOffset(currentTime) - this.targetTimeZone.getOffset(currentTime)) / 3600000;
            if (object == null) {
                return this.nullable ? null : this.defaultValue;
            } else {
                this.targetCalendar.setTimeInMillis(((Timestamp) object).getTime());
                this.targetCalendar.add(10, hourDifference);
                return new Timestamp(this.targetCalendar.getTimeInMillis());
            }
        }
    }

    public static final class TinyIntToBigDecimal extends ConnectorDataTypeConverter {
        protected int scale;

        public void setScale(int scale) {
            this.scale = scale;
        }

        public final Object convert(Object object) {
            if (object == null) {
                return this.nullable ? null : (BigDecimal) this.defaultValue;
            } else {
                return new BigDecimal(((Byte) object).byteValue());
            }
        }
    }

    public static final class TinyIntToBigDecimalWithScale extends ConnectorDataTypeConverter {
        protected int scale;

        public void setScale(int scale) {
            this.scale = scale;
        }

        public final Object convert(Object object) {
            if (object == null) {
                return this.nullable ? null : ((BigDecimal) this.defaultValue).setScale(this.scale, RoundingMode.HALF_UP);
            } else {
                return new BigDecimal(((Byte) object).byteValue()).setScale(this.scale, RoundingMode.HALF_UP);
            }
        }
    }

    public static final class TinyIntToBoolean extends ConnectorDataTypeConverter {
        public final Object convert(Object object) {
            if (object == null) {
                return this.nullable ? null : this.defaultValue;
            } else {
                return Boolean.valueOf(((Byte) object).byteValue() != (byte) 0);
            }
        }
    }

    public static final class TinyIntToDouble extends ConnectorDataTypeConverter {
        public final Object convert(Object object) {
            if (object == null) {
                return this.nullable ? null : this.defaultValue;
            } else {
                return Double.valueOf(((Byte) object).doubleValue());
            }
        }
    }

    public static final class TinyIntToFloat extends ConnectorDataTypeConverter {
        public final Object convert(Object object) {
            if (object == null) {
                return this.nullable ? null : this.defaultValue;
            } else {
                return Float.valueOf(((Byte) object).floatValue());
            }
        }
    }

    public static final class TinyIntToInteger extends ConnectorDataTypeConverter {
        public final Object convert(Object object) {
            if (object == null) {
                return this.nullable ? null : this.defaultValue;
            } else {
                return Integer.valueOf(((Byte) object).intValue());
            }
        }
    }

    public static final class TinyIntToLong extends ConnectorDataTypeConverter {
        public final Object convert(Object object) {
            if (object == null) {
                return this.nullable ? null : this.defaultValue;
            } else {
                return Long.valueOf(((Byte) object).longValue());
            }
        }
    }

    public static final class TinyIntToShort extends ConnectorDataTypeConverter {
        public final Object convert(Object object) {
            if (object == null) {
                return this.nullable ? null : this.defaultValue;
            } else {
                return Short.valueOf(((Byte) object).shortValue());
            }
        }
    }

    public static final class TinyIntToString extends ConnectorDataTypeConverter {
        public final Object convert(Object object) {
            if (object == null) {
                return this.nullable ? null : this.defaultValue;
            } else {
                return object.toString();
            }
        }
    }

    public static final class TinyIntToTinyInt extends ConnectorDataTypeConverter {
        public final Object convert(Object object) {
            if (object == null) {
                return this.nullable ? null : this.defaultValue;
            } else {
                return object;
            }
        }
    }

    public abstract Object convert(Object obj);

    public void setNullable(boolean nullable) {
        this.nullable = nullable;
    }

    public void setDefaultValue(Object defaultValue) {
        this.defaultValue = defaultValue;
    }

    protected static java.util.Date parseStringWithFormatArray(List<DateFormat> list, String s) {
        java.util.Date date = null;
        if (list != null && list.size() != 0) {
            int i = 0;
            while (i < list.size()) {
                try {
                    date = ((DateFormat) list.get(i)).parse(s);
                    break;
                } catch (ParseException e) {
                    i++;
                }
            }
        }
        return date;
    }

    protected static DateFormat createDateFormat(TimeZone tz, String dateFormat) {
        DateFormat df = new SimpleDateFormat(dateFormat);
        if (tz != null) {
            df.setTimeZone(tz);
        }
        return df;
    }

    public static long convertMillisecFromSourceToDefaultTZ(long millisec, TimeZone sourceTZ) {
        return ((long) (TimeZone.getDefault().getOffset(millisec) - sourceTZ.getOffset(millisec))) + millisec;
    }

    public static long convertMillisecFromDefaultToTargetTZ(long millisec, TimeZone targetTZ) {
        return ((long) (targetTZ.getOffset(millisec) - TimeZone.getDefault().getOffset(millisec))) + millisec;
    }
}