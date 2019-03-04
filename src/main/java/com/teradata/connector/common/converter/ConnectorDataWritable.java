package com.teradata.connector.common.converter;

import com.teradata.connector.common.exception.ConnectorException;
import com.teradata.connector.common.utils.ConnectorSchemaUtils;
import com.teradata.jdbc.ResultStruct;
import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.io.InputStream;
import java.math.BigDecimal;
import java.sql.Blob;
import java.sql.Clob;
import java.sql.Date;
import java.sql.SQLException;
import java.sql.Time;
import java.sql.Timestamp;
import java.util.Calendar;
import java.util.TimeZone;
import org.apache.hadoop.hive.common.type.HiveChar;
import org.apache.hadoop.hive.common.type.HiveVarchar;
import org.apache.hadoop.io.BooleanWritable;
import org.apache.hadoop.io.ByteWritable;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;

/**
 * @author Administrator
 */
public abstract class ConnectorDataWritable implements WritableComparable {
    public abstract void set(final Object p0) throws ConnectorException;

    public abstract Object get();

    public static void writeObjectAsText(final Object o, final DataOutput out) throws IOException {
        final Text n = new Text(o.toString());
        n.write(out);
    }

    public static String readObjectAsText(final DataInput in) throws IOException {
        final Text n = new Text();
        n.readFields(in);
        return n.toString();
    }

    public static Writable connectorWritableFactory(final Object o) throws ConnectorException {
        final int type = ConnectorSchemaUtils.getGenericObjectType(o);
        return connectorWritableFactory(type);
    }

    public static Writable connectorWritableFactory(final int type) throws ConnectorException {
        switch (type) {
            case 7:
            case 8: {
                return (Writable) new DoubleWritable();
            }
            case 16: {
                return (Writable) new BooleanWritable();
            }
            case -3:
            case -2: {
                return (Writable) new BytesWritable();
            }
            case -6: {
                return (Writable) new ByteWritable();
            }
            case 6: {
                return (Writable) new FloatWritable();
            }
            case 4: {
                return (Writable) new IntWritable();
            }
            case -5: {
                return (Writable) new LongWritable();
            }
            case 5: {
                return (Writable) new ShortWritable();
            }
            case 2:
            case 3: {
                return (Writable) new BigDecimalWritable();
            }
            case 2004: {
                return (Writable) new BlobWritable();
            }
            case 2005: {
                return (Writable) new ClobWritable();
            }
            case -1:
            case 1:
            case 12:
            case 1111: {
                return (Writable) new Text();
            }
            case 91: {
                return (Writable) new DateWritable();
            }
            case 92: {
                return (Writable) new TimeWritable();
            }
            case 93: {
                return (Writable) new TimestampWritable();
            }
            case 2002: {
                return (Writable) new PeriodWritable();
            }
            case 1884: {
                return (Writable) NullWritable.get();
            }
            case 1885:
            case 1886: {
                return (Writable) new CalendarWritable();
            }
            default: {
                throw new ConnectorException(15028, new Object[]{type});
            }
        }
    }

    public static Writable connectorWritableFactoryParquet(final int type) throws ConnectorException {
        switch (type) {
            case 7:
            case 8: {
                return (Writable) new DoubleWritable();
            }
            case 16: {
                return (Writable) new BooleanWritable();
            }
            case -3:
            case -2: {
                return (Writable) new BytesWritable();
            }
            case -6: {
                return (Writable) new org.apache.hadoop.hive.serde2.io.ByteWritable();
            }
            case 6: {
                return (Writable) new FloatWritable();
            }
            case 4: {
                return (Writable) new IntWritable();
            }
            case -5: {
                return (Writable) new LongWritable();
            }
            case 5: {
                return (Writable) new org.apache.hadoop.hive.serde2.io.ShortWritable();
            }
            case 2:
            case 3: {
                return (Writable) new BigDecimalWritable();
            }
            case 2004: {
                return (Writable) new BlobWritable();
            }
            case 2005: {
                return (Writable) new ClobWritable();
            }
            case -1:
            case 1:
            case 12:
            case 1111: {
                return (Writable) new Text();
            }
            case 91: {
                return (Writable) new DateWritable();
            }
            case 92: {
                return (Writable) new TimeWritable();
            }
            case 93: {
                return (Writable) new TimestampWritable();
            }
            case 2002: {
                return (Writable) new PeriodWritable();
            }
            case 1884: {
                return (Writable) NullWritable.get();
            }
            case 1885:
            case 1886: {
                return (Writable) new CalendarWritable();
            }
            default: {
                throw new ConnectorException(15028, new Object[]{type});
            }
        }
    }

    public static void setWritableValue(final Writable writable, final Object o, final int type) throws ConnectorException {
        if (o == null) {
            return;
        }
        switch (type) {
            case 7:
            case 8: {
                final DoubleWritable w1 = (DoubleWritable) writable;
                w1.set((double) o);
                return;
            }
            case 16: {
                final BooleanWritable w2 = (BooleanWritable) writable;
                w2.set((boolean) o);
                return;
            }
            case -3:
            case -2: {
                final BytesWritable w3 = (BytesWritable) writable;
                final byte[] bytes = (byte[]) o;
                w3.set(bytes, 0, bytes.length);
                return;
            }
            case -6: {
                final ByteWritable w4 = (ByteWritable) writable;
                w4.set((byte) o);
                return;
            }
            case 6: {
                final FloatWritable w5 = (FloatWritable) writable;
                w5.set((float) o);
                return;
            }
            case 4: {
                final IntWritable w6 = (IntWritable) writable;
                w6.set((int) o);
                return;
            }
            case -5: {
                final LongWritable w7 = (LongWritable) writable;
                w7.set((long) o);
                return;
            }
            case 2:
            case 3:
            case 5:
            case 91:
            case 92:
            case 93:
            case 1885:
            case 1886:
            case 2002:
            case 2004:
            case 2005: {
                final ConnectorDataWritable w8 = (ConnectorDataWritable) writable;
                w8.set(o);
                return;
            }
            case -1:
            case 1:
            case 12:
            case 1111: {
                final Text w9 = (Text) writable;
                w9.set((String) o);
                return;
            }
            case 1882:
            case 1883: {
                final int primType = ConnectorSchemaUtils.getGenericObjectType(o);
                setWritableValue(writable, o, primType);
                break;
            }
        }
        throw new ConnectorException(15028, new Object[]{type});
    }

    public static void setWritableValueParquet(final Writable writable, final Object o, final int type) throws ConnectorException {
        if (o == null) {
            return;
        }
        switch (type) {
            case 7:
            case 8: {
                final DoubleWritable w1 = (DoubleWritable) writable;
                w1.set((double) o);
                return;
            }
            case 16: {
                final BooleanWritable w2 = (BooleanWritable) writable;
                w2.set((boolean) o);
                return;
            }
            case -3:
            case -2: {
                final BytesWritable w3 = (BytesWritable) writable;
                final byte[] bytes = (byte[]) o;
                w3.set(bytes, 0, bytes.length);
                return;
            }
            case -6: {
                final org.apache.hadoop.hive.serde2.io.ByteWritable w4 = (org.apache.hadoop.hive.serde2.io.ByteWritable) writable;
                w4.set((byte) o);
                return;
            }
            case 6: {
                final FloatWritable w5 = (FloatWritable) writable;
                w5.set((float) o);
                return;
            }
            case 4: {
                final IntWritable w6 = (IntWritable) writable;
                w6.set((int) o);
                return;
            }
            case -5: {
                final LongWritable w7 = (LongWritable) writable;
                w7.set((long) o);
                return;
            }
            case 5: {
                final org.apache.hadoop.hive.serde2.io.ShortWritable ws4 = (org.apache.hadoop.hive.serde2.io.ShortWritable) writable;
                ws4.set((short) o);
                return;
            }
            case 2:
            case 3:
            case 91:
            case 92:
            case 93:
            case 1885:
            case 1886:
            case 2002:
            case 2004:
            case 2005: {
                final ConnectorDataWritable w8 = (ConnectorDataWritable) writable;
                w8.set(o);
                return;
            }
            case -1:
            case 1:
            case 12:
            case 1111: {
                final Text w9 = (Text) writable;
                if (o instanceof HiveVarchar) {
                    final HiveVarchar hv = new HiveVarchar((HiveVarchar) o, ((HiveVarchar) o).getCharacterLength());
                    w9.set(hv.getValue());
                    return;
                }
                if (o instanceof HiveChar) {
                    final HiveChar hv2 = new HiveChar((HiveChar) o, ((HiveChar) o).getCharacterLength());
                    w9.set(hv2.getValue());
                    return;
                }
                w9.set((String) o);
                return;
            }
            case 1882:
            case 1883: {
                final int primType = ConnectorSchemaUtils.getGenericObjectType(o);
                setWritableValue(writable, o, primType);
                break;
            }
        }
        throw new ConnectorException(15028, new Object[]{type});
    }

    public static Object getWritableValue(final Writable writable, final int type) throws ConnectorException {
        if (writable instanceof NullWritable) {
            return null;
        }
        switch (type) {
            case 7:
            case 8: {
                final DoubleWritable w1 = (DoubleWritable) writable;
                return w1.get();
            }
            case 16: {
                final BooleanWritable w2 = (BooleanWritable) writable;
                return w2.get();
            }
            case -3:
            case -2: {
                final BytesWritable w3 = (BytesWritable) writable;
                return w3.getBytes();
            }
            case -6: {
                final ByteWritable w4 = (ByteWritable) writable;
                return w4.get();
            }
            case 6: {
                final FloatWritable w5 = (FloatWritable) writable;
                return w5.get();
            }
            case 4: {
                final IntWritable w6 = (IntWritable) writable;
                return w6.get();
            }
            case -5: {
                final LongWritable w7 = (LongWritable) writable;
                return w7.get();
            }
            case 2:
            case 3:
            case 5:
            case 91:
            case 92:
            case 93:
            case 1885:
            case 1886:
            case 2002:
            case 2004:
            case 2005: {
                final ConnectorDataWritable w8 = (ConnectorDataWritable) writable;
                return w8.get();
            }
            case -1:
            case 1:
            case 12:
            case 1111: {
                final Text w9 = (Text) writable;
                return w9.toString();
            }
            case 1882:
            case 1883: {
                final int primType = ConnectorSchemaUtils.getWritableObjectType(writable);
                getWritableValue(writable, primType);
                break;
            }
        }
        throw new ConnectorException(15028, new Object[]{type});
    }

    public static final class ShortWritable extends ConnectorDataWritable {
        private short value;

        @Override
        public void write(final DataOutput out) throws IOException {
            out.writeShort(this.value);
        }

        @Override
        public void readFields(final DataInput in) throws IOException {
            this.value = in.readShort();
        }

        @Override
        public int compareTo(final Object o) {
            final short thisVal = this.value;
            final short thatVal = ((ShortWritable) o).value;
            return (thisVal < thatVal) ? -1 : ((thisVal == thatVal) ? 0 : 1);
        }

        @Override
        public Short get() {
            return this.value;
        }

        @Override
        public void set(final Object o) {
            this.value = (short) o;
        }
    }

    public static final class BigDecimalWritable extends ConnectorDataWritable {
        private BigDecimal value;

        @Override
        public void set(final Object val) {
            this.value = (BigDecimal) val;
        }

        @Override
        public BigDecimal get() {
            return this.value;
        }

        @Override
        public void write(final DataOutput out) throws IOException {
            ConnectorDataWritable.writeObjectAsText(this.value, out);
        }

        @Override
        public void readFields(final DataInput in) throws IOException {
            this.value = new BigDecimal(ConnectorDataWritable.readObjectAsText(in));
        }

        @Override
        public int compareTo(final Object o) {
            final BigDecimal thisVal = this.value;
            final BigDecimal thatVal = ((BigDecimalWritable) o).value;
            return thisVal.compareTo(thatVal);
        }
    }

    public static final class ClobWritable extends ConnectorDataWritable {
        private Clob value;
        private String sValue;

        private String toClobString() throws IOException {
            final StringBuffer builder = new StringBuffer();
            final int bufsize = 10000;
            final Clob clobObj = this.value;
            final byte[] bytesData = new byte[bufsize];
            InputStream stream = null;
            builder.setLength(0);
            try {
                stream = clobObj.getAsciiStream();
                int nbytes;
                while ((nbytes = stream.read(bytesData, 0, bufsize)) > 0) {
                    for (final byte b : bytesData) {
                        final int c = b & 0xFF;
                        builder.append((char) c);
                    }
                }
            } catch (IOException e) {
                throw e;
            } catch (SQLException e2) {
                throw new IOException(e2.getMessage(), e2);
            } finally {
                try {
                    if (stream != null) {
                        stream.close();
                    }
                } catch (IOException e3) {
                    throw e3;
                }
            }
            return builder.toString();
        }

        @Override
        public Object get() {
            if (this.value != null) {
                return this.value;
            }
            return this.sValue;
        }

        @Override
        public void write(final DataOutput out) throws IOException {
            ConnectorDataWritable.writeObjectAsText(this.toClobString(), out);
        }

        @Override
        public void readFields(final DataInput in) throws IOException {
            this.sValue = ConnectorDataWritable.readObjectAsText(in);
        }

        @Override
        public int compareTo(final Object o) {
            return 0;
        }

        @Override
        public void set(final Object o) throws ConnectorException {
            if (o instanceof Clob) {
                this.value = (Clob) o;
            } else {
                if (!(o instanceof String)) {
                    throw new ConnectorException(15028, new Object[]{o.getClass()});
                }
                this.sValue = (String) o;
            }
        }
    }

    public static final class BlobWritable extends ConnectorDataWritable {
        private Blob value;
        private byte[] bytes;

        private byte[] toBytes() {
            final Blob object = this.value;
            final int bufsize = 10000;
            int size = 100000;
            int offset = 0;
            int nbytes = 0;
            final Blob blobObj = object;
            final byte[] buf = new byte[bufsize];
            byte[] data = new byte[size];
            InputStream stream = null;
            try {
                stream = blobObj.getBinaryStream();
                while ((nbytes = stream.read(buf, 0, bufsize)) > 0) {
                    if (offset + nbytes > size) {
                        size *= 2;
                        final byte[] newdata = new byte[size];
                        System.arraycopy(data, 0, newdata, 0, offset);
                        data = newdata;
                    }
                    System.arraycopy(buf, 0, data, offset, nbytes);
                    offset += nbytes;
                }
            } catch (IOException ex) {
            } catch (SQLException ex2) {
            } finally {
                try {
                    if (stream != null) {
                        stream.close();
                    }
                } catch (IOException ex3) {
                }
            }
            if (offset == 0) {
                return new byte[0];
            }
            final byte[] lob = new byte[offset];
            System.arraycopy(data, 0, lob, 0, offset);
            return lob;
        }

        @Override
        public void write(final DataOutput out) throws IOException {
            final BytesWritable bw = new BytesWritable();
            final byte[] bytes = this.toBytes();
            bw.set(bytes, 0, bytes.length);
            bw.write(out);
        }

        @Override
        public void readFields(final DataInput in) throws IOException {
            final BytesWritable bw = new BytesWritable();
            bw.readFields(in);
            this.bytes = bw.getBytes();
        }

        @Override
        public int compareTo(final Object o) {
            return 0;
        }

        @Override
        public void set(final Object o) {
            if (o instanceof Blob) {
                this.value = (Blob) o;
            } else if (o instanceof byte[]) {
                this.bytes = (byte[]) o;
            }
        }

        @Override
        public Object get() {
            if (this.value != null) {
                return this.value;
            }
            return this.bytes;
        }
    }

    public static final class DateWritable extends ConnectorDataWritable {
        private Date value;

        @Override
        public void write(final DataOutput out) throws IOException {
            ConnectorDataWritable.writeObjectAsText(this.value, out);
        }

        @Override
        public void readFields(final DataInput in) throws IOException {
            final String s = ConnectorDataWritable.readObjectAsText(in);
            this.value = Date.valueOf(s);
        }

        @Override
        public int compareTo(final Object o) {
            final long thisVal = this.value.getTime();
            final long thatVal = ((DateWritable) o).value.getTime();
            return (thisVal < thatVal) ? -1 : ((thisVal == thatVal) ? 0 : 1);
        }

        @Override
        public void set(final Object o) throws ConnectorException {
            this.value = (Date) o;
        }

        @Override
        public Object get() {
            return this.value;
        }
    }

    public static final class TimeWritable extends ConnectorDataWritable {
        private Time value;

        @Override
        public void write(final DataOutput out) throws IOException {
            ConnectorDataWritable.writeObjectAsText(this.value, out);
        }

        @Override
        public void readFields(final DataInput in) throws IOException {
            final String s = ConnectorDataWritable.readObjectAsText(in);
            this.value = Time.valueOf(s);
        }

        @Override
        public int compareTo(final Object o) {
            final long thisVal = this.value.getTime();
            final long thatVal = ((TimeWritable) o).value.getTime();
            return (thisVal < thatVal) ? -1 : ((thisVal == thatVal) ? 0 : 1);
        }

        @Override
        public void set(final Object o) throws ConnectorException {
            this.value = (Time) o;
        }

        @Override
        public Object get() {
            return this.value;
        }
    }

    public static final class TimestampWritable extends ConnectorDataWritable {
        private Timestamp value;

        @Override
        public void write(final DataOutput out) throws IOException {
            ConnectorDataWritable.writeObjectAsText(this.value, out);
        }

        @Override
        public void readFields(final DataInput in) throws IOException {
            final String s = ConnectorDataWritable.readObjectAsText(in);
            this.value = Timestamp.valueOf(s);
        }

        @Override
        public int compareTo(final Object o) {
            final long thisVal = this.value.getTime();
            final long thatVal = ((TimestampWritable) o).value.getTime();
            return (thisVal < thatVal) ? -1 : ((thisVal == thatVal) ? 0 : 1);
        }

        @Override
        public void set(final Object o) throws ConnectorException {
            this.value = (Timestamp) o;
        }

        @Override
        public Object get() {
            return this.value;
        }
    }

    public static final class PeriodWritable extends ConnectorDataWritable {
        private Object value;
        private String sValue;

        private static String periodToString(final Object value) {
            String result = value.toString();
            int bracketPos = result.indexOf(91);
            if (bracketPos > 0) {
                result = result.substring(bracketPos + 1);
                bracketPos = result.lastIndexOf(93);
                if (bracketPos > 0) {
                    result = "(" + result.substring(0, bracketPos) + ")";
                }
            }
            return result;
        }

        @Override
        public void write(final DataOutput out) throws IOException {
            ConnectorDataWritable.writeObjectAsText(periodToString(this.value), out);
        }

        @Override
        public void readFields(final DataInput in) throws IOException {
            final String sVal = ConnectorDataWritable.readObjectAsText(in);
            this.sValue = sVal;
        }

        @Override
        public int compareTo(final Object o) {
            return 0;
        }

        @Override
        public void set(final Object o) throws ConnectorException {
            if (o instanceof ResultStruct) {
                this.value = o;
            } else if (o instanceof String) {
                this.sValue = (String) o;
            }
        }

        @Override
        public Object get() {
            if (this.value != null) {
                return this.value;
            }
            return this.sValue;
        }
    }

    public static final class CalendarWritable extends ConnectorDataWritable {
        Calendar cal;

        @Override
        public void write(final DataOutput out) throws IOException {
            final Long l = this.cal.getTimeInMillis();
            final TimeZone tz = this.cal.getTimeZone();
            out.writeLong(l);
            ConnectorDataWritable.writeObjectAsText(tz.getID(), out);
        }

        @Override
        public void readFields(final DataInput in) throws IOException {
            final long l = in.readLong();
            final String tzStr = ConnectorDataWritable.readObjectAsText(in);
            (this.cal = Calendar.getInstance(TimeZone.getTimeZone(tzStr))).setTimeInMillis(l);
        }

        @Override
        public int compareTo(final Object o) {
            final long thisVal = this.cal.getTimeInMillis();
            final long thatVal = ((CalendarWritable) o).cal.getTimeInMillis();
            return (thisVal < thatVal) ? -1 : ((thisVal == thatVal) ? 0 : 1);
        }

        @Override
        public void set(final Object o) throws ConnectorException {
            this.cal = (Calendar) o;
        }

        @Override
        public Object get() {
            return this.cal;
        }
    }
}
