package com.teradata.connector.common.converter;

import java.nio.*;

import junit.framework.*;

import java.io.*;

import org.junit.*;

import java.math.*;
import javax.sql.rowset.serial.*;
import java.sql.*;

import com.teradata.connector.common.exception.*;
import org.apache.hadoop.io.*;
import org.junit.Assert;
import org.junit.Test;

import java.util.*;
import java.util.Date;

public class ConnectorDataWritableTest {
    @Test
    public void testShortWritable() throws Exception {
        final ByteArrayOutputStream byteArray = new ByteArrayOutputStream();
        final DataOutputStream output = new DataOutputStream(byteArray);
        final ConnectorDataWritable.ShortWritable sw = new ConnectorDataWritable.ShortWritable();
        sw.set(12);
        sw.write(output);
        output.close();
        final byte[] buf = byteArray.toByteArray();
        Assert.assertEquals(12, (int) ByteBuffer.wrap(buf).getShort());
        final ConnectorDataWritable.ShortWritable sw2 = new ConnectorDataWritable.ShortWritable();
        sw2.set(22);
        Assert.assertEquals(-1, sw.compareTo(sw2));
        final DataInputStream input = new DataInputStream(new ByteArrayInputStream(buf));
        sw.readFields(input);
        Assert.assertEquals((Object) new Short((short) 12), (Object) sw.get());
    }

    @Test
    public void testBigDecimalWritable() throws Exception {
        final ConnectorDataWritable.BigDecimalWritable big = new ConnectorDataWritable.BigDecimalWritable();
        big.set(new BigDecimal(9999));
        final ConnectorDataWritable.BigDecimalWritable big2 = new ConnectorDataWritable.BigDecimalWritable();
        big2.set(new BigDecimal(9999));
        Assert.assertEquals(0, big.compareTo(big2));
        big2.set(new BigDecimal(99999));
        Assert.assertEquals(-1, big.compareTo(big2));
    }

    @Test
    public void testClobWritable() throws Exception {
        final ConnectorDataWritable.ClobWritable cw = new ConnectorDataWritable.ClobWritable();
        cw.set("Test");
        Assert.assertEquals((Object) "Test", cw.get());
        final DataInputStream input = new DataInputStream(new ByteArrayInputStream("Test2".getBytes()));
        cw.set(new SerialClob("Test2".toCharArray()));
        Assert.assertTrue(cw.get() instanceof SerialClob);
    }

    @Test
    public void testBlobWritable() throws Exception {
        final ConnectorDataWritable.BlobWritable bw = new ConnectorDataWritable.BlobWritable();
        bw.set("Test".getBytes());
        Assert.assertEquals("Test", new String((byte[]) bw.get()));
        final ByteArrayInputStream byteArray = new ByteArrayInputStream("Test2".getBytes());
        final DataInputStream input = new DataInputStream(byteArray);
        bw.set(new SerialBlob("Test2".getBytes()));
        Assert.assertTrue(bw.get() instanceof SerialBlob);
    }

    @Test
    public void testDateWritable() throws Exception {
        final ConnectorDataWritable.DateWritable dw = new ConnectorDataWritable.DateWritable();
        dw.set(new java.sql.Date(new Date().getTime()));
        final ConnectorDataWritable.DateWritable dw2 = new ConnectorDataWritable.DateWritable();
        Thread.sleep(1L);
        dw2.set(new java.sql.Date(new Date().getTime()));
        Assert.assertEquals(-1, dw.compareTo(dw2));
    }

    @Test
    public void testTimeWritable() throws Exception {
        final ConnectorDataWritable.TimeWritable tw = new ConnectorDataWritable.TimeWritable();
        tw.set(new Time(new Date().getTime()));
        final ConnectorDataWritable.TimeWritable tw2 = new ConnectorDataWritable.TimeWritable();
        Thread.sleep(1L);
        tw2.set(new Time(new Date().getTime()));
        Assert.assertEquals(1, tw2.compareTo(tw));
    }

    @Test
    public void testTimestampWritable() throws Exception {
        final ConnectorDataWritable.TimestampWritable tw = new ConnectorDataWritable.TimestampWritable();
        tw.set(new Timestamp(66666L));
        final ConnectorDataWritable.TimestampWritable tw2 = new ConnectorDataWritable.TimestampWritable();
        tw2.set(new Timestamp(66666L));
        Assert.assertEquals(0, tw.compareTo(tw2));
    }

    @Test
    public void testPeriodWritable() throws Exception {
        final ConnectorDataWritable.PeriodWritable pw = new ConnectorDataWritable.PeriodWritable();
        pw.set("(2013-12-18, 2013-12-19)");
        Assert.assertEquals((Object) "(2013-12-18, 2013-12-19)", pw.get());
    }

    @Test
    public void testCalendarWritable() throws Exception {
        final ConnectorDataWritable.CalendarWritable cw = new ConnectorDataWritable.CalendarWritable();
        cw.set(new GregorianCalendar());
        final ConnectorDataWritable.CalendarWritable cw2 = new ConnectorDataWritable.CalendarWritable();
        Thread.sleep(1L);
        cw2.set(new GregorianCalendar());
        Assert.assertEquals(-1, cw.compareTo(cw2));
    }

    @Test
    public void testConnectorWritableFactory() throws Exception {
        Assert.assertTrue(ConnectorDataWritable.connectorWritableFactory(new Integer(0)) instanceof IntWritable);
        Assert.assertTrue(ConnectorDataWritable.connectorWritableFactory("") instanceof Text);
        Assert.assertTrue(ConnectorDataWritable.connectorWritableFactory(8) instanceof DoubleWritable);
        Assert.assertTrue(ConnectorDataWritable.connectorWritableFactory(16) instanceof BooleanWritable);
        Assert.assertTrue(ConnectorDataWritable.connectorWritableFactory(-3) instanceof BytesWritable);
        Assert.assertTrue(ConnectorDataWritable.connectorWritableFactory(-6) instanceof ByteWritable);
        Assert.assertTrue(ConnectorDataWritable.connectorWritableFactory(5) instanceof ConnectorDataWritable.ShortWritable);
        Assert.assertTrue(ConnectorDataWritable.connectorWritableFactory(3) instanceof ConnectorDataWritable.BigDecimalWritable);
        Assert.assertTrue(ConnectorDataWritable.connectorWritableFactory(1) instanceof Text);
        Assert.assertTrue(ConnectorDataWritable.connectorWritableFactory(-1) instanceof Text);
        Assert.assertTrue(ConnectorDataWritable.connectorWritableFactory(1111) instanceof Text);
        Assert.assertTrue(ConnectorDataWritable.connectorWritableFactory(1885) instanceof ConnectorDataWritable.CalendarWritable);
        try {
            ConnectorDataWritable.connectorWritableFactory(-900);
        } catch (ConnectorException ce) {
            Assert.assertTrue(ce.getMessage().contains("invalid object type \"-900\""));
        }
    }

    @Test
    public void testSetWritableValue() throws Exception {
        final BooleanWritable bw = new BooleanWritable();
        ConnectorDataWritable.setWritableValue((Writable) bw, true, 16);
        Assert.assertEquals(true, bw.get());
        final BytesWritable bwr = new BytesWritable();
        ConnectorDataWritable.setWritableValue((Writable) bwr, "Test".getBytes(), -3);
        Assert.assertEquals("Test", new String(bwr.getBytes()).trim());
        final ByteWritable byter = new ByteWritable();
        ConnectorDataWritable.setWritableValue((Writable) byter, 26, -6);
        Assert.assertEquals(26, (int) byter.get());
        final ConnectorDataWritable.ShortWritable sw = new ConnectorDataWritable.ShortWritable();
        ConnectorDataWritable.setWritableValue((Writable) sw, 26, 5);
        Assert.assertEquals((Object) new Short((short) 26), (Object) sw.get());
        final ConnectorDataWritable.BigDecimalWritable bdw = new ConnectorDataWritable.BigDecimalWritable();
        ConnectorDataWritable.setWritableValue((Writable) bdw, new BigDecimal(26), 3);
        Assert.assertEquals((Object) new BigDecimal(26), (Object) bdw.get());
        final Text text = new Text();
        ConnectorDataWritable.setWritableValue((Writable) text, "d", 1);
        Assert.assertEquals(100, text.charAt(0));
        ConnectorDataWritable.setWritableValue((Writable) text, "Test", -1);
        Assert.assertEquals("Test", text.toString());
        try {
            ConnectorDataWritable.setWritableValue((Writable) text, "d", 1883);
        } catch (ConnectorException ce) {
            Assert.assertTrue(ce.getMessage().contains("invalid object type \"1,883\""));
            Assert.assertEquals(100, text.charAt(0));
        }
    }

    @Test
    public void testGetWritableValue() throws Exception {
        final DoubleWritable dw = new DoubleWritable(23.0);
        Assert.assertEquals((Object) 23.0, ConnectorDataWritable.getWritableValue((Writable) dw, 8));
        final BooleanWritable bw = new BooleanWritable(true);
        Assert.assertEquals((Object) true, ConnectorDataWritable.getWritableValue((Writable) bw, 16));
        final BytesWritable bwr = new BytesWritable("bin".getBytes());
        byte[] barray = (byte[]) ConnectorDataWritable.getWritableValue((Writable) bwr, -3);
        Assert.assertEquals("bin", new String(barray));
        final ByteWritable byter = new ByteWritable((byte) 9);
        Assert.assertEquals((Object) 9, ConnectorDataWritable.getWritableValue((Writable) byter, -6));
        ConnectorDataWritable cdw = new ConnectorDataWritable.ShortWritable();
        cdw.set(10);
        Assert.assertEquals((Object) 10, ConnectorDataWritable.getWritableValue((Writable) cdw, 5));
        cdw = new ConnectorDataWritable.BigDecimalWritable();
        cdw.set(new BigDecimal(20));
        Assert.assertEquals((Object) new BigDecimal(20), ConnectorDataWritable.getWritableValue((Writable) cdw, 3));
        cdw = new ConnectorDataWritable.ClobWritable();
        cdw.set("large char");
        Assert.assertEquals((Object) "large char", ConnectorDataWritable.getWritableValue((Writable) cdw, 2005));
        cdw = new ConnectorDataWritable.BlobWritable();
        cdw.set("large char".getBytes());
        barray = (byte[]) ConnectorDataWritable.getWritableValue((Writable) cdw, 2004);
        Assert.assertEquals("large char", new String(barray));
        cdw = new ConnectorDataWritable.CalendarWritable();
        cdw.set(Calendar.getInstance());
        final Calendar cal = (Calendar) ConnectorDataWritable.getWritableValue((Writable) cdw, 1885);
        Assert.assertEquals(Calendar.getInstance().get(12), cal.get(12));
        final Text text = new Text("c");
        Assert.assertEquals((Object) "c", ConnectorDataWritable.getWritableValue((Writable) text, 1));
        Assert.assertEquals((Object) "c", ConnectorDataWritable.getWritableValue((Writable) text, -1));
        Assert.assertEquals((Object) "()", ConnectorDataWritable.getWritableValue((Writable) new Text("()"), 1111));
        try {
            ConnectorDataWritable.getWritableValue((Writable) text, 1883);
        } catch (ConnectorException ce) {
            Assert.assertTrue(ce.getMessage().contains("invalid object type \"1,883\""));
        }
    }
}
