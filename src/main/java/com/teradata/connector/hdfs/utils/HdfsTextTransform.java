package com.teradata.connector.hdfs.utils;

import com.teradata.connector.common.converter.ConnectorDataTypeConverter;
import com.teradata.connector.common.converter.ConnectorDataTypeDefinition;
import com.teradata.connector.common.utils.ConnectorConfiguration;
import com.teradata.connector.common.utils.ConnectorConfiguration.direction;
import com.teradata.connector.common.utils.ConnectorSchemaUtils;

import java.math.BigDecimal;
import java.sql.Time;
import java.sql.Timestamp;
import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.*;

import org.apache.hadoop.conf.Configuration;


public abstract class HdfsTextTransform {
    protected String nullString;
    protected String nullNonString;
    protected boolean customNullSet;

    private static long parseDateTimeWithFMTTZ(final String value, final DateFormat df, final List<DateFormat> backupDateFormat, final TimeZone sourceTimezone) {
        Date d = null;
        Calendar parseCal = null;
        long milliSec = 0L;
        try {
            d = df.parse(value);
            milliSec = d.getTime();
            parseCal = df.getCalendar();
        } catch (ParseException e) {
            int i = 0;
            while (i < backupDateFormat.size()) {
                final DateFormat dateFormat = backupDateFormat.get(i);
                try {
                    d = dateFormat.parse(value);
                    milliSec = d.getTime();
                    parseCal = dateFormat.getCalendar();
                } catch (ParseException e2) {
                    ++i;
                    continue;
                }
                break;
            }
            if (d == null) {
                throw new RuntimeException(e);
            }
        }
        if (sourceTimezone == null) {
            return milliSec;
        }
        if (parseCal.isSet(15)) {
            milliSec = ConnectorDataTypeConverter.convertMillisecFromDefaultToTargetTZ(milliSec, sourceTimezone);
        }
        return milliSec;
    }

    private HdfsTextTransform() {
        this.nullString = null;
        this.nullNonString = this.nullString;
        this.customNullSet = false;
    }

    public abstract int getType();

    public abstract Object transform(final String p0);

    public abstract String toString(final Object p0);

    public void setNullString(final String nullString) {
        this.nullString = nullString;
        this.customNullSet = true;
    }

    public String getNullString() {
        return this.nullString;
    }

    public void setNullNonString(final String nullNonString) {
        this.nullNonString = nullNonString;
        this.customNullSet = true;
    }

    public String getNullNonString() {
        return this.nullNonString;
    }

    public void setCustomNullSet(final boolean customeNullSet) {
        this.customNullSet = customeNullSet;
    }

    public boolean getCustomNullSet() {
        return this.customNullSet;
    }

    public static HdfsTextTransform[] lookupTextTransformClass(final String recordSchema, final String nullString, final String nullNonString, final Configuration configuration, final ConnectorConfiguration.direction direction) {
        final List<String> recordDataTypes = ConnectorSchemaUtils.parseColumnTypes(ConnectorSchemaUtils.parseColumns(recordSchema));
        final HdfsTextTransform[] transformFunction = new HdfsTextTransform[recordDataTypes.size()];
        int i = 0;
        for (final String dataType : recordDataTypes) {
            switch (HdfsSchemaUtils.lookupHdfsAvroDatatype(dataType)) {
                case 4: {
                    transformFunction[i] = new IntTransform();
                    break;
                }
                case -5: {
                    transformFunction[i] = new BigIntTransform();
                    break;
                }
                case 5: {
                    transformFunction[i] = new SmallIntTransform();
                    break;
                }
                case -6: {
                    transformFunction[i] = new TinyIntTransform();
                    break;
                }
                case 2:
                case 3: {
                    transformFunction[i] = new DecimalTransform();
                    break;
                }
                case 91: {
                    String dateFormat;
                    if (direction == ConnectorConfiguration.direction.input) {
                        dateFormat = ConnectorConfiguration.getInputDateFormat(configuration);
                    } else {
                        dateFormat = ConnectorConfiguration.getOutputDateFormat(configuration);
                    }
                    transformFunction[i] = new DateTransformFMT(dateFormat).setupBackupDateFormat(configuration);
                    break;
                }
                case 92: {
                    String timeFormat;
                    String timezoneId;
                    if (direction == ConnectorConfiguration.direction.input) {
                        timeFormat = ConnectorConfiguration.getInputTimeFormat(configuration);
                        timezoneId = ConnectorConfiguration.getInputTimezoneId(configuration);
                    } else {
                        timeFormat = ConnectorConfiguration.getOutputTimeFormat(configuration);
                        timezoneId = ConnectorConfiguration.getOutputTimezoneId(configuration);
                    }
                    transformFunction[i] = new TimeTransformFMT(timezoneId, timeFormat, direction).setupBackupDateFormat(configuration);
                    break;
                }
                case 93: {
                    String timezoneId;
                    String tsFormat;
                    if (direction == ConnectorConfiguration.direction.input) {
                        tsFormat = ConnectorConfiguration.getInputTimestampFormat(configuration);
                        timezoneId = ConnectorConfiguration.getInputTimezoneId(configuration);
                    } else {
                        tsFormat = ConnectorConfiguration.getOutputTimestampFormat(configuration);
                        timezoneId = ConnectorConfiguration.getOutputTimezoneId(configuration);
                    }
                    transformFunction[i] = new TimeStampTransformFMT(timezoneId, tsFormat, direction).setupBackupDateFormat(configuration);
                    break;
                }
                case 6: {
                    transformFunction[i] = new FloatTransform();
                    break;
                }
                case 7:
                case 8: {
                    transformFunction[i] = new DoubleTransform();
                    break;
                }
                case 16: {
                    transformFunction[i] = new BooleanTransform();
                    break;
                }
                case -3:
                case -2: {
                    transformFunction[i] = new BinaryTransform();
                    break;
                }
                case 2004: {
                    transformFunction[i] = new BlobTransform();
                    break;
                }
                case 2005: {
                    transformFunction[i] = new ClobTransform();
                    break;
                }
                case -1:
                case 1:
                case 12: {
                    transformFunction[i] = new VarcharTransform();
                    break;
                }
                default: {
                    transformFunction[i] = new OtherTransform();
                    break;
                }
            }
            if (!nullString.isEmpty() || !nullNonString.isEmpty()) {
                transformFunction[i].setCustomNullSet(true);
                transformFunction[i].setNullString(nullString);
                transformFunction[i].setNullNonString(nullNonString);
            }
            ++i;
        }
        return transformFunction;
    }

    public static class IntTransform extends HdfsTextTransform {
        public IntTransform() {
            super();
        }

        @Override
        public final int getType() {
            return 4;
        }

        @Override
        public Object transform(final String value) {
            if (this.customNullSet) {
                return (value == null || value.length() == 0 || value.equals(this.nullNonString)) ? null : Integer.valueOf(value.trim());
            }
            return (value == null || value.length() == 0) ? null : Integer.valueOf(value.trim());
        }

        @Override
        public String toString(final Object value) {
            return value.toString();
        }
    }

    public static class BigIntTransform extends HdfsTextTransform {
        public BigIntTransform() {
            super();
        }

        @Override
        public final int getType() {
            return -5;
        }

        @Override
        public Object transform(final String value) {
            if (this.customNullSet) {
                return (value == null || value.length() == 0 || value.equals(this.nullNonString)) ? null : Long.valueOf(value);
            }
            return (value == null || value.length() == 0) ? null : Long.valueOf(value);
        }

        @Override
        public String toString(final Object value) {
            return value.toString();
        }
    }

    public static class SmallIntTransform extends HdfsTextTransform {
        public SmallIntTransform() {
            super();
        }

        @Override
        public int getType() {
            return 5;
        }

        @Override
        public Object transform(final String value) {
            if (this.customNullSet) {
                return (value == null || value.length() == 0 || value.equals(this.nullNonString)) ? null : Short.valueOf(value);
            }
            return (value == null || value.length() == 0) ? null : Short.valueOf(value);
        }

        @Override
        public String toString(final Object value) {
            return value.toString();
        }
    }

    public static class TinyIntTransform extends HdfsTextTransform {
        public TinyIntTransform() {
            super();
        }

        @Override
        public int getType() {
            return -6;
        }

        @Override
        public Object transform(final String value) {
            if (this.customNullSet) {
                return (value == null || value.length() == 0 || value.equals(this.nullNonString)) ? null : Byte.valueOf(value);
            }
            return (value == null || value.length() == 0) ? null : Byte.valueOf(value);
        }

        @Override
        public String toString(final Object value) {
            return value.toString();
        }
    }

    public static class DecimalTransform extends HdfsTextTransform {
        public DecimalTransform() {
            super();
        }

        @Override
        public int getType() {
            return 3;
        }

        @Override
        public Object transform(final String value) {
            if (this.customNullSet) {
                return (value == null || value.length() == 0 || value.equals(this.nullNonString)) ? null : new BigDecimal(value);
            }
            return (value == null || value.length() == 0) ? null : new BigDecimal(value);
        }

        @Override
        public String toString(final Object value) {
            return value.toString();
        }
    }

    public static class DateTransformFMT extends HdfsTextTransform {
        DateFormat df;
        private List<DateFormat> backupDateFormat;

        public HdfsTextTransform setupBackupDateFormat(final Configuration conf) {
            final List<String> values = ConnectorConfiguration.getAllConfigurationItems(conf, "tdch.input.date.format");
            this.backupDateFormat = new ArrayList<DateFormat>();
            for (int i = 0; i < values.size(); ++i) {
                final DateFormat df = new SimpleDateFormat(values.get(i));
                this.backupDateFormat.add(df);
            }
            return this;
        }

        public DateTransformFMT(final String format) {
            super();
            if (format == null || format.isEmpty()) {
                this.df = null;
            } else {
                this.df = new SimpleDateFormat(format);
            }
        }

        @Override
        public int getType() {
            return 91;
        }

        @Override
        public Object transform(final String value) {
            if (this.customNullSet) {
                if (value == null || value.length() == 0 || value.equals(this.nullNonString)) {
                    return null;
                }
            } else if (value == null || value.length() == 0) {
                return null;
            }
            if (this.df == null) {
                return java.sql.Date.valueOf(value);
            }
            Date d = null;
            try {
                d = this.df.parse(value);
            } catch (ParseException e) {
                int i = 0;
                while (i < this.backupDateFormat.size()) {
                    final DateFormat dateFormat = this.backupDateFormat.get(i);
                    try {
                        d = dateFormat.parse(value);
                    } catch (ParseException e2) {
                        ++i;
                        continue;
                    }
                    break;
                }
                if (d == null) {
                    throw new RuntimeException(e);
                }
            }
            return new java.sql.Date(d.getTime());
        }

        @Override
        public String toString(final Object value) {
            if (this.df == null) {
                return value.toString();
            }
            return this.df.format((Date) value);
        }
    }

    public static class TimeTransformFMT extends HdfsTextTransform {
        private DateFormat df;
        private TimeZone timezoneInfo;
        private List<DateFormat> backupDateFormat;

        public HdfsTextTransform setupBackupDateFormat(final Configuration conf) {
            final List<String> values = ConnectorConfiguration.getAllConfigurationItems(conf, "tdch.input.time.format");
            this.backupDateFormat = new ArrayList<DateFormat>();
            for (int i = 0; i < values.size(); ++i) {
                final DateFormat df = new SimpleDateFormat(values.get(i));
                this.backupDateFormat.add(df);
            }
            return this;
        }

        public TimeTransformFMT(final String timezoneId, final String format, final ConnectorConfiguration.direction d) {
            super();
            if (timezoneId.trim().isEmpty()) {
                this.timezoneInfo = TimeZone.getDefault();
            } else {
                this.timezoneInfo = TimeZone.getTimeZone(timezoneId);
            }
            if (format == null || format.isEmpty()) {
                this.df = null;
            } else {
                this.df = new SimpleDateFormat(format);
                if (d == ConnectorConfiguration.direction.output) {
                    this.df.setTimeZone(this.timezoneInfo);
                }
            }
        }

        @Override
        public int getType() {
            return 92;
        }

        @Override
        public Object transform(final String value) {
            if (this.customNullSet) {
                if (value == null || value.length() == 0 || value.equals(this.nullNonString)) {
                    return null;
                }
            } else if (value == null || value.length() == 0) {
                return null;
            }
            if (this.df == null) {
                return Time.valueOf(value);
            }
            final long milliSec = parseDateTimeWithFMTTZ(value, this.df, this.backupDateFormat, this.timezoneInfo);
            return new Time(milliSec);
        }

        @Override
        public String toString(final Object value) {
            if (this.df == null) {
                return value.toString();
            }
            final Time ts = (Time) value;
            long milliSec = ts.getTime();
            milliSec = ConnectorDataTypeConverter.convertMillisecFromSourceToDefaultTZ(milliSec, this.timezoneInfo);
            ts.setTime(milliSec);
            return this.df.format((Date) value);
        }
    }

    public static class TimeStampTransformFMT extends HdfsTextTransform {
        private DateFormat df;
        private TimeZone timeZoneInfo;
        private List<DateFormat> backupDateFormat;

        public HdfsTextTransform setupBackupDateFormat(final Configuration conf) {
            final List<String> values = ConnectorConfiguration.getAllConfigurationItems(conf, "tdch.input.timestamp.format");
            this.backupDateFormat = new ArrayList<DateFormat>();
            for (int i = 0; i < values.size(); ++i) {
                final DateFormat df = new SimpleDateFormat(values.get(i));
                this.backupDateFormat.add(df);
            }
            return this;
        }

        public TimeStampTransformFMT(final String timezoneId, final String format, final ConnectorConfiguration.direction d) {
            super();
            if (timezoneId.trim().isEmpty()) {
                this.timeZoneInfo = TimeZone.getDefault();
            } else {
                this.timeZoneInfo = TimeZone.getTimeZone(timezoneId);
            }
            if (format == null || format.isEmpty()) {
                this.df = null;
            } else {
                this.df = new SimpleDateFormat(format);
                if (d == ConnectorConfiguration.direction.output) {
                    this.df.setTimeZone(this.timeZoneInfo);
                }
            }
        }

        @Override
        public int getType() {
            return 93;
        }

        @Override
        public Object transform(final String value) {
            if (this.customNullSet) {
                if (value == null || value.length() == 0 || value.equals(this.nullNonString)) {
                    return null;
                }
            } else if (value == null || value.length() == 0) {
                return null;
            }
            long milliSec = 0L;
            if (this.df == null) {
                final Timestamp ts = Timestamp.valueOf(value);
                return ts;
            }
            milliSec = parseDateTimeWithFMTTZ(value, this.df, this.backupDateFormat, this.timeZoneInfo);
            return new Timestamp(milliSec);
        }

        @Override
        public String toString(final Object value) {
            if (this.df == null) {
                return value.toString();
            }
            final Timestamp ts = (Timestamp) value;
            long milliSec = ts.getTime();
            milliSec = ConnectorDataTypeConverter.convertMillisecFromSourceToDefaultTZ(milliSec, this.timeZoneInfo);
            ts.setTime(milliSec);
            return this.df.format(value);
        }
    }

    public static class VarcharTransform extends HdfsTextTransform {
        public VarcharTransform() {
            super();
        }

        @Override
        public int getType() {
            return 12;
        }

        @Override
        public Object transform(final String value) {
            if (this.customNullSet) {
                return (value == null || value.equals(this.nullString)) ? null : value;
            }
            return (value == null || value.length() == 0) ? null : value;
        }

        @Override
        public String toString(final Object value) {
            return (String) value;
        }
    }

    public static class DoubleTransform extends HdfsTextTransform {
        public DoubleTransform() {
            super();
        }

        @Override
        public int getType() {
            return 8;
        }

        @Override
        public Object transform(final String value) {
            if (this.customNullSet) {
                return (value == null || value.length() == 0 || value.equals(this.nullNonString)) ? null : Double.valueOf(value);
            }
            return (value == null || value.length() == 0) ? null : Double.valueOf(value);
        }

        @Override
        public String toString(final Object value) {
            return value.toString();
        }
    }

    public static class BinaryTransform extends HdfsTextTransform {
        public BinaryTransform() {
            super();
        }

        @Override
        public int getType() {
            return -2;
        }

        public byte[] toBytes(final String str) {
            final char[] buffer = str.toCharArray();
            final byte[] b = new byte[buffer.length << 1];
            for (int i = 0; i < buffer.length; ++i) {
                final int bpos = i << 1;
                b[bpos] = (byte) ((buffer[i] & '\uff00') >> 8);
                b[bpos + 1] = (byte) (buffer[i] & '\u00ff');
            }
            return b;
        }

        @Override
        public Object transform(final String value) {
            if (value == null || value.length() == 0) {
                return new byte[0];
            }
            if (this.customNullSet && value.equals(this.nullNonString)) {
                return new byte[0];
            }
            final int len = value.length();
            switch (len) {
                case 0: {
                    return new byte[0];
                }
                default: {
                    return this.toBytes(value);
                }
            }
        }

        @Override
        public String toString(final Object value) {
            return value.toString();
        }
    }

    public static class BlobTransform extends HdfsTextTransform {
        public BlobTransform() {
            super();
        }

        @Override
        public final int getType() {
            return 2004;
        }

        @Override
        public Object transform(final String value) {
            if (value == null || value.length() == 0) {
                return new byte[0];
            }
            if (this.customNullSet && value.equals(this.nullNonString)) {
                return new byte[0];
            }
            final int len = value.length();
            switch (len) {
                case 0: {
                    return new byte[0];
                }
                case 1: {
                    final byte[] bytesData = {(byte) Character.digit(value.charAt(0), 16)};
                    return bytesData;
                }
                default: {
                    final byte[] bytesData = new byte[len / 2];
                    for (int i = 0; i < len; i += 2) {
                        bytesData[i / 2] = (byte) ((Character.digit(value.charAt(i), 16) << 4) + Character.digit(value.charAt(i + 1), 16));
                    }
                    return bytesData;
                }
            }
        }

        @Override
        public String toString(final Object value) {
            return value.toString();
        }
    }

    public static class ClobTransform extends HdfsTextTransform {
        public ClobTransform() {
            super();
        }

        @Override
        public Object transform(final String value) {
            if (this.customNullSet) {
                return (value == null || value.equals(this.nullString)) ? null : value;
            }
            return (value == null || value.length() == 0) ? null : value;
        }

        @Override
        public final int getType() {
            return 2005;
        }

        @Override
        public String toString(final Object value) {
            return (String) value;
        }
    }

    public static class BooleanTransform extends HdfsTextTransform {
        public BooleanTransform() {
            super();
        }

        @Override
        public int getType() {
            return 16;
        }

        @Override
        public Object transform(final String value) {
            if (this.customNullSet) {
                return (value == null || value.length() == 0 || value.equals(this.nullNonString)) ? null : Boolean.valueOf(value);
            }
            return (value == null || value.length() == 0) ? null : Boolean.valueOf(value);
        }

        @Override
        public String toString(final Object value) {
            return value.toString();
        }
    }

    public static class FloatTransform extends HdfsTextTransform {
        public FloatTransform() {
            super();
        }

        @Override
        public int getType() {
            return 6;
        }

        @Override
        public Object transform(final String value) {
            return (value == null || value.length() == 0) ? null : Float.valueOf(value);
        }

        @Override
        public String toString(final Object value) {
            return value.toString();
        }
    }

    public static class OtherTransform extends HdfsTextTransform {
        public OtherTransform() {
            super();
        }

        @Override
        public int getType() {
            return 1882;
        }

        @Override
        public Object transform(final String value) {
            if (this.customNullSet) {
                return (value == null || value.length() == 0 || value.equals(this.nullString)) ? null : value;
            }
            return (value == null || value.length() == 0) ? null : value;
        }

        @Override
        public String toString(final Object value) {
            return value.toString();
        }
    }
}
