package com.teradata.connector.hive;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import org.apache.parquet.Preconditions;
import org.apache.parquet.io.api.Binary;
import org.apache.parquet.io.api.RecordConsumer;

/**
 * @author Administrator
 */
public class ParquetNanoTime extends BasicDataTypes {
    private final int julianDay;
    private final long timeOfDayNanos;

    public static ParquetNanoTime fromBinary(final Binary bytes) {
        Preconditions.checkArgument(bytes.length() == 12, "Must be 12 bytes");
        final ByteBuffer buf = bytes.toByteBuffer();
        buf.order(ByteOrder.LITTLE_ENDIAN);
        final long timeOfDayNanos = buf.getLong();
        final int julianDay = buf.getInt();
        return new ParquetNanoTime(julianDay, timeOfDayNanos);
    }

    public static ParquetNanoTime fromInt96(final ParquetInt96Value int96) {
        final ByteBuffer buf = int96.getInt96().toByteBuffer();
        return new ParquetNanoTime(buf.getInt(), buf.getLong());
    }

    public ParquetNanoTime(final int julianDay, final long timeOfDayNanos) {
        this.julianDay = julianDay;
        this.timeOfDayNanos = timeOfDayNanos;
    }

    public int getJulianDay() {
        return this.julianDay;
    }

    public long getTimeOfDayNanos() {
        return this.timeOfDayNanos;
    }

    public Binary toBinary() {
        final ByteBuffer buf = ByteBuffer.allocate(12);
        buf.order(ByteOrder.LITTLE_ENDIAN);
        buf.putLong(this.timeOfDayNanos);
        buf.putInt(this.julianDay);
        buf.flip();
        return Binary.fromByteBuffer(buf);
    }

    public ParquetInt96Value toInt96() {
        return new ParquetInt96Value(this.toBinary());
    }

    @Override
    public void writeValue(final RecordConsumer recordConsumer) {
        recordConsumer.addBinary(this.toBinary());
    }

    @Override
    public String toString() {
        return "ParquetNanoTime{julianDay=" + this.julianDay + ", timeOfDayNanos=" + this.timeOfDayNanos + "}";
    }
}
