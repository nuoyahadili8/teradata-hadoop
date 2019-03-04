package com.teradata.connector.common.converter;

import org.junit.*;

import java.util.*;
import java.sql.*;
import java.text.*;
import java.util.Date;

import org.apache.hadoop.conf.*;
import com.teradata.connector.teradata.utils.*;
import com.teradata.connector.common.utils.*;
import com.teradata.connector.hdfs.utils.*;

public class ConnectorDataTypeConverterFMTTZTest {
    @Test
    public void testIntegerToDateTZ() throws ParseException {
        final SimpleDateFormat df = new SimpleDateFormat("yyyy/MM/dd");
        int timestamp = 1420070400;
        String dateText = df.format(new ConnectorDataTypeConverter.IntegerToDateTZ("US/Pacific").convert(timestamp));
        Assert.assertTrue(dateText.equals("2015/01/01"));
        timestamp = 1430438400;
        dateText = df.format(new ConnectorDataTypeConverter.IntegerToDateTZ("US/Pacific").convert(timestamp));
        Assert.assertTrue(dateText.equals("2015/05/01"));
        timestamp = 1441065600;
        dateText = df.format(new ConnectorDataTypeConverter.IntegerToDateTZ("GMT-05:00").convert(timestamp));
        Assert.assertTrue(dateText.equals("2015/09/01"));
    }

    @Test
    public void testIntegerToTTZ() throws ParseException {
        final Date d = toDate("2013/1/5 10:00:00");
        final Calendar c_800 = Calendar.getInstance(TimeZone.getTimeZone("GMT+11:00"));
        c_800.setTime(d);
        final Time t = Time.valueOf("10:12:32");
        final ConnectorDataTypeConverter.TimeTZToInteger toInt = new ConnectorDataTypeConverter.TimeTZToInteger("GMT+11:00");
        final int i = (int) toInt.convert(t);
        final Time newTime = (Time) new ConnectorDataTypeConverter.IntegerToTimeTZ("GMT+11:00").convert(i);
        Assert.assertTrue("10:12:32".equals(newTime.toString()));
    }

    @Test
    public void testIntegerToTimestampTZ() throws ParseException {
        final ConnectorDataTypeConverter conv = new ConnectorDataTypeConverter.IntegerToTimestampTZ("GMT");
        Assert.assertTrue(conv.convert(null) == null);
        final int sec = 1436312389;
        final Object o = conv.convert(sec);
        Assert.assertTrue(o instanceof Timestamp);
        final Timestamp t = (Timestamp) o;
        final DateFormat df = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
        Assert.assertTrue(df.format(t).equals("2015-07-07 23:39:49"));
    }

    @Test
    public void testIntegerToCalendar() {
        final ConnectorDataTypeConverter conv = new ConnectorDataTypeConverter.IntegerToCalendar("GMT");
        Assert.assertTrue(conv.convert(null) == null);
        final int sec = 1436312389;
        final Object o = conv.convert(sec);
        Assert.assertTrue(o instanceof Calendar);
        final Calendar c1 = (Calendar) o;
        final Calendar c2 = Calendar.getInstance(TimeZone.getTimeZone("GMT"));
        c2.setTimeInMillis(sec * 1000L);
        Assert.assertTrue(c1.equals(c2));
    }

    @Test
    public void testDateTZToInteger() throws ParseException {
        final ConnectorDataTypeConverter conv = new ConnectorDataTypeConverter.DateTZToInteger("GMT+04:00");
        Assert.assertTrue(conv.convert(null) == null);
        final DateFormat df = new SimpleDateFormat("yyyy-MM-dd HH:mm:ssZ");
        final Date d = df.parse("2015-07-07 23:39:49+0000");
        final java.sql.Date sqld = new java.sql.Date(d.getTime());
        final Object o = conv.convert(sqld);
        Assert.assertTrue(o instanceof Integer);
        final Integer i = (Integer) o;
        final int sec = 1436297989;
        Assert.assertTrue(i == sec);
    }

    @Test
    public void testTimeTZToInteger() throws ParseException {
        final ConnectorDataTypeConverter conv = new ConnectorDataTypeConverter.TimeTZToInteger("GMT");
        Assert.assertTrue(conv.convert(null) == null);
        final Timestamp ts = Timestamp.valueOf("2015-07-07 23:39:49.000");
        final Time t = new Time(ts.getTime());
        final Object o = conv.convert(t);
        Assert.assertTrue(o instanceof Integer);
        final Integer i = (Integer) o;
        final int sec = 1436312389;
        Assert.assertTrue(i == sec);
    }

    @Test
    public void testTimeToTimestamp() throws ParseException {
        final ConnectorDataTypeConverter conv = new ConnectorDataTypeConverter.TimeToTimestamp();
        Assert.assertTrue(conv.convert(null) == null);
        final Time t = Time.valueOf("23:39:49");
        final Object o = conv.convert(t);
        Assert.assertTrue(o instanceof Timestamp);
        final Timestamp ts = (Timestamp) o;
        final Date currentDate = new Date();
        final Calendar cal = Calendar.getInstance();
        cal.setTime(currentDate);
        final SimpleDateFormat f = new SimpleDateFormat("yyyy-MM-dd");
        final Timestamp ts2 = Timestamp.valueOf(f.format(cal.getTime()) + " " + t.toString());
        Assert.assertTrue(ts.getTime() == ts2.getTime());
    }

    @Test
    public void testTimestapTZToInteger() throws ParseException {
        final ConnectorDataTypeConverter conv = new ConnectorDataTypeConverter.TimestampTZToInteger("GMT");
        Assert.assertTrue(conv.convert(null) == null);
        final Timestamp t = Timestamp.valueOf("2015-07-07 23:39:49.000");
        final Object o = conv.convert(t);
        Assert.assertTrue(o instanceof Integer);
        final Integer i = (Integer) o;
        final int sec = 1436312389;
        Assert.assertTrue(i == sec);
    }

    public void testDateToStringFMT() throws ParseException {
        final ConnectorDataTypeConverter.DateToStringFMT c = new ConnectorDataTypeConverter.DateToStringFMT("dd/MM/yyyy");
        final Date d = toDate2("2013/10/10");
        c.convert(d);
    }

    @Test
    public void testTimestampTZToStringFMT() throws ParseException {
        final Timestamp ts = Timestamp.valueOf("2014-10-02 10:12:13.123");
        ConnectorDataTypeConverter.TimestampTZToStringFMTTZ n = new ConnectorDataTypeConverter.TimestampTZToStringFMTTZ("GMT+02:00", "GMT+04:00", "dd/MM/yyyy hh:mm:ssZ");
        n.convert(ts);
        Assert.assertTrue("02/10/2014 12:12:13+0400".equals(n.convert(ts).toString()));
        n = new ConnectorDataTypeConverter.TimestampTZToStringFMTTZ("GMT+02:00", "GMT+04:00", "dd/MM/yyyy hh:mm:ss");
        n.convert(ts);
        Assert.assertTrue("02/10/2014 12:12:13".equals(n.convert(ts).toString()));
    }

    @Test
    public void testTimestampTZToStringFMTFullTest() throws ParseException {
        String strTS = "2014-10-02 10:12:13";
        String nano = "";
        Timestamp ts = Timestamp.valueOf("2014-10-02 10:12:13");
        ConnectorDataTypeConverter.TimestampTZToStringFMTTZ n = new ConnectorDataTypeConverter.TimestampTZToStringFMTTZ(null, null, null);
        for (int i = 1; i <= 6; ++i) {
            final String newts = n.convert(ts).toString();
            String outputnano = "";
            if (i == 2) {
                outputnano = "." + Integer.parseInt(nano) * 100;
            } else if (i == 3) {
                outputnano = "." + Integer.parseInt(nano) * 10;
            } else if (i > 3) {
                outputnano = "." + nano;
            }
            Assert.assertTrue(("2014-10-02 10:12:13" + outputnano).equals(newts));
            nano += i;
            ts = Timestamp.valueOf("2014-10-02 10:12:13." + nano);
        }
        strTS = "2014-10-02 10:12:13";
        nano = "";
        ts = Timestamp.valueOf("2014-10-02 10:12:13");
        n = new ConnectorDataTypeConverter.TimestampTZToStringFMTTZ("GMT+02:00", "GMT+04:00", "dd/MM/yyyy hh:mm:ss.SSSSSS");
        for (int i = 1; i <= 6; ++i) {
            final String newts = n.convert(ts).toString();
            String outputnano = "";
            if (i == 2) {
                outputnano = "." + Integer.parseInt(nano) * 100;
            } else if (i == 3) {
                outputnano = "." + Integer.parseInt(nano) * 10;
            } else if (i > 3) {
                outputnano = "." + nano;
            }
            Assert.assertTrue(("02/10/2014 12:12:13" + outputnano).equals(newts));
            nano += i;
            ts = Timestamp.valueOf("2014-10-02 10:12:13." + nano);
        }
        strTS = "2014-10-02 10:12:13";
        nano = "";
        ts = Timestamp.valueOf("2014-10-02 10:12:13");
        n = new ConnectorDataTypeConverter.TimestampTZToStringFMTTZ("GMT+02:00", "GMT+04:00", "dd/MM/yyyy hh:mm:ss.SSSSSSZ");
        for (int i = 1; i <= 6; ++i) {
            final String newts = n.convert(ts).toString();
            String outputnano = "";
            if (i == 2) {
                outputnano = "." + Integer.parseInt(nano) * 100;
            } else if (i == 3) {
                outputnano = "." + Integer.parseInt(nano) * 10;
            } else if (i > 3) {
                outputnano = "." + nano;
            }
            Assert.assertTrue(("02/10/2014 12:12:13" + outputnano + "+0400").equals(newts));
            nano += i;
            ts = Timestamp.valueOf("2014-10-02 10:12:13." + nano);
        }
        strTS = "2014-10-02 10:12:13";
        nano = "";
        ts = Timestamp.valueOf("2014-10-02 10:12:13");
        n = new ConnectorDataTypeConverter.TimestampTZToStringFMTTZ("GMT+02:00", "GMT+04:00", "dd/MM/yyyy hh:mm:ss.SSSSSZ");
        for (int i = 1; i <= 6; ++i) {
            final String newts = n.convert(ts).toString();
            String outputnano = "";
            if (i == 2) {
                outputnano = "." + Integer.parseInt(nano) * 100;
            } else if (i == 3) {
                outputnano = "." + Integer.parseInt(nano) * 10;
            } else if (i == 6) {
                outputnano = ".12345";
            } else if (i > 3) {
                outputnano = "." + nano;
            }
            Assert.assertTrue(("02/10/2014 12:12:13" + outputnano + "+0400").equals(newts));
            nano += i;
            ts = Timestamp.valueOf("2014-10-02 10:12:13." + nano);
        }
        strTS = "2014-10-02 10:12:13";
        nano = "";
        ts = Timestamp.valueOf("2014-10-02 10:12:13");
        n = new ConnectorDataTypeConverter.TimestampTZToStringFMTTZ("GMT+02:00", "GMT+04:00", "dd/MM/yyyy hh:mm:ss.SSSSZ");
        for (int i = 1; i <= 6; ++i) {
            final String newts = n.convert(ts).toString();
            String outputnano = "";
            if (i == 2) {
                outputnano = "." + Integer.parseInt(nano) * 100;
            } else if (i == 3) {
                outputnano = "." + Integer.parseInt(nano) * 10;
            } else if (i == 5) {
                outputnano = ".1234";
            } else if (i == 6) {
                outputnano = ".1234";
            } else if (i > 3) {
                outputnano = "." + nano;
            }
            Assert.assertTrue(("02/10/2014 12:12:13" + outputnano + "+0400").equals(newts));
            nano += i;
            ts = Timestamp.valueOf("2014-10-02 10:12:13." + nano);
        }
        strTS = "2014-10-02 10:12:13";
        nano = "";
        ts = Timestamp.valueOf("2014-10-02 10:12:13");
        n = new ConnectorDataTypeConverter.TimestampTZToStringFMTTZ("GMT+02:00", "GMT+04:00", "dd/MM/yyyy hh:mm:ss.SSSZ");
        for (int i = 1; i <= 6; ++i) {
            final String newts = n.convert(ts).toString();
            String outputnano = "";
            if (i == 2) {
                outputnano = "." + Integer.parseInt(nano) * 100;
            } else if (i == 3) {
                outputnano = "." + Integer.parseInt(nano) * 10;
            } else if (i > 3) {
                outputnano = ".123";
            }
            Assert.assertTrue(("02/10/2014 12:12:13" + outputnano + "+0400").equals(newts));
            nano += i;
            ts = Timestamp.valueOf("2014-10-02 10:12:13." + nano);
        }
        strTS = "2014-10-02 10:12:13";
        nano = "";
        ts = Timestamp.valueOf("2014-10-02 10:12:13");
        n = new ConnectorDataTypeConverter.TimestampTZToStringFMTTZ("GMT+02:00", "GMT+04:00", "dd.MM.yyyy hh:mm:ss.SSSSSSZ");
        for (int i = 1; i <= 6; ++i) {
            final String newts = n.convert(ts).toString();
            String outputnano = "";
            if (i == 2) {
                outputnano = "." + Integer.parseInt(nano) * 100;
            } else if (i == 3) {
                outputnano = "." + Integer.parseInt(nano) * 10;
            } else if (i > 3) {
                outputnano = "." + nano;
            }
            Assert.assertTrue(("02.10.2014 12:12:13" + outputnano + "+0400").equals(newts));
            nano += i;
            ts = Timestamp.valueOf("2014-10-02 10:12:13." + nano);
        }
        strTS = "2014-10-02 10:12:13";
        nano = "";
        ts = Timestamp.valueOf("2014-10-02 10:12:13");
        n = new ConnectorDataTypeConverter.TimestampTZToStringFMTTZ("GMT+02:00", "GMT+04:00", "dd.MM.yyyy.hh.mm.ss.SSSSSS.Z");
        for (int i = 1; i <= 6; ++i) {
            final String newts = n.convert(ts).toString();
            String outputnano = "";
            if (i == 2) {
                outputnano = "." + Integer.parseInt(nano) * 100;
            } else if (i == 3) {
                outputnano = "." + Integer.parseInt(nano) * 10;
            } else if (i > 3) {
                outputnano = "." + nano;
            }
            Assert.assertTrue(("02.10.2014.12.12.13" + outputnano + ".+0400").equals(newts));
            nano += i;
            ts = Timestamp.valueOf("2014-10-02 10:12:13." + nano);
        }
    }

    @Test
    public void testDateTZToLong() throws ParseException {
        Date date = toDate2("2015/1/1 05:00:00.000");
        long timestamp = (long) new ConnectorDataTypeConverter.DateTZToLong("US/Pacific").convert(date);
        Assert.assertTrue(timestamp == 1420070400000L);
        date = toDate2("2015/5/1");
        timestamp = (long) new ConnectorDataTypeConverter.DateTZToLong("US/Pacific").convert(date);
        Assert.assertTrue(timestamp == 1430434800000L);
        date = toDate2("2015/5/1");
        timestamp = (long) new ConnectorDataTypeConverter.DateTZToLong("GMT+05:00").convert(date);
        Assert.assertTrue(timestamp == 1430391600000L);
    }

    @Test
    public void testDateTZToDateTZ() throws ParseException {
        final Date d = toDate2("2015/5/1");
        Assert.assertTrue(toDate2("2015/5/2").equals(new ConnectorDataTypeConverter.DateTZToDateTZ("GMT-12:00", "GMT+12:00").convert(d)));
        final Date d2 = toDate2("2015/1/1");
        Assert.assertTrue(toDate2("2015/1/2").equals(new ConnectorDataTypeConverter.DateTZToDateTZ("GMT-12:00", "GMT+12:00").convert(d2)));
    }

    @Test
    public void testDateTZToTimestampTZ() throws ParseException {
        final Date d = toDate("2015/5/1 10:00:12");
        Assert.assertTrue(toDate("2015/5/1 14:00:12").equals(new ConnectorDataTypeConverter.DateTZToTimestampTZ("GMT+8:00", "GMT+12:00").convert(d)));
        final Date d2 = toDate("2015/1/1 10:00:12");
        Assert.assertTrue(toDate("2015/1/1 14:00:12").equals(new ConnectorDataTypeConverter.DateTZToTimestampTZ("GMT+8:00", "GMT+12:00").convert(d2)));
    }

    @Test
    public void testTimestampTZToDateAndTimeTZ() throws ParseException {
        final Timestamp t = Timestamp.valueOf("2014-10-10 10:30:00");
        final ConnectorDataTypeConverter.TimestampTZToDateTZ c = new ConnectorDataTypeConverter.TimestampTZToDateTZ("GMT+6", "GMT+7:00");
        c.convert(t);
        Assert.assertTrue(toDate("2014/10/10 11:30:00").equals(c.convert(t)));
        final ConnectorDataTypeConverter.TimestampTZToTimeTZ c2 = new ConnectorDataTypeConverter.TimestampTZToTimeTZ("GMT+6", "GMT+7:00");
        c2.convert(t);
        Assert.assertTrue("11:30:00".equals(c2.convert(t).toString()));
        final ConnectorDataTypeConverter.TimestampTZToTimestampTZ c3 = new ConnectorDataTypeConverter.TimestampTZToTimestampTZ("GMT+6", "GMT+7:00");
        c3.convert(t);
        Assert.assertTrue("2014-10-10 11:30:00.0".equals(c3.convert(t).toString()));
    }

    @Test
    public void testTimestampTZToLongTZ() throws ParseException {
        final Timestamp t = Timestamp.valueOf("2014-10-10 10:30:00");
        final ConnectorDataTypeConverter.TimestampTZToLong c = new ConnectorDataTypeConverter.TimestampTZToLong("GMT-7:00");
        c.convert(t);
        new ConnectorDataTypeConverter.LongToTimestampTZ("GMT-07:00").convert(c.convert(t));
        Assert.assertTrue(1412962200000L == (long) c.convert(t));
        Assert.assertTrue("2014-10-10 10:30:00.0".equals(new ConnectorDataTypeConverter.LongToTimestampTZ("GMT-07:00").convert(c.convert(t)).toString()));
    }

    @Test
    public void testTimestampTZToLong_2() throws ParseException {
        final ConnectorDataTypeConverter.TimestampTZToLong toLong = new ConnectorDataTypeConverter.TimestampTZToLong("PST");
        final long l = (long) toLong.convert(Timestamp.valueOf("2014-08-25 00:18:12"));
        final ConnectorDataTypeConverter.LongToTimestampTZ n = new ConnectorDataTypeConverter.LongToTimestampTZ("PST");
        final long timeWithDST = 1408951092000L;
        final Timestamp ts = (Timestamp) n.convert(l);
        Assert.assertTrue("2014-08-25 00:18:12.0".equals(ts.toString()));
        Assert.assertTrue(timeWithDST == l);
    }

    @Test
    public void testTimeTZToStringFMTTZ() throws ParseException {
        final Time t = Time.valueOf("10:30:00");
        ConnectorDataTypeConverter.TimeTZToStringFMTTZ c = new ConnectorDataTypeConverter.TimeTZToStringFMTTZ("", "GMT+7:00", "GMT+2");
        c.convert(t);
        Assert.assertTrue("05:30:00".equals(c.convert(t).toString()));
        c = new ConnectorDataTypeConverter.TimeTZToStringFMTTZ("HH//mm:ss", "GMT+7:00", "GMT+2");
        c.convert(t);
        Assert.assertTrue("05//30:00".equals(c.convert(t).toString()));
    }

    @Test
    public void testTimeTZToTimeTZ() throws ParseException {
        final Time t = Time.valueOf("10:30:00");
        ConnectorDataTypeConverter.TimeTZToTimeTZ c = new ConnectorDataTypeConverter.TimeTZToTimeTZ("GMT+3:00", "GMT+1:00");
        c.convert(t);
        Assert.assertTrue("08:30:00".equals(c.convert(t).toString()));
        c = new ConnectorDataTypeConverter.TimeTZToTimeTZ("GMT+8:00", "GMT+1:00");
        c.convert(t);
        Assert.assertTrue("03:30:00".equals(c.convert(t).toString()));
    }

    @Test
    public void testDateTZToStringWithFormat() throws ParseException {
        final Date d = toDate("2013/1/5 10:00:00");
        ConnectorDataTypeConverter.DateTZToStringFMT strTS = new ConnectorDataTypeConverter.DateTZToStringFMT("GMT+08:00", "dd/MM/yyyy HH//mm:ss");
        final java.sql.Date sqlD = new java.sql.Date(d.getTime());
        strTS.convert(sqlD);
        Assert.assertTrue("05/01/2013 10//00:00".equals(strTS.convert(sqlD).toString()));
        strTS = new ConnectorDataTypeConverter.DateTZToStringFMT("GMT+08:00", "");
        strTS.convert(sqlD);
    }

    @Test
    public void testStringFMTTZToTimestampTZ() throws ParseException {
        final String ts = "2013-01-05 10:00:00.123";
        ConnectorDataTypeConverter.StringFMTTZToTimestampTZ strTS = new ConnectorDataTypeConverter.StringFMTTZToTimestampTZ("", "GMT+09:00", "GMT+11:00");
        strTS.convert(ts);
        Assert.assertTrue("2013-01-05 12:00:00.123".equals(strTS.convert(ts).toString()));
        strTS = new ConnectorDataTypeConverter.StringFMTTZToTimestampTZ("yyyy-mm-dd hh:mm:ss", "GMT+09:00", "GMT+11:00");
        strTS.convert(ts);
        Assert.assertTrue("2013-01-05 12:00:00.0".equals(strTS.convert(ts).toString()));
        strTS = new ConnectorDataTypeConverter.StringFMTTZToTimestampTZ("yyyy-mm-dd hh:mm:ss.SSSZ", "GMT+09:00", "GMT+11:00");
        strTS.convert("2013-01-05 10:00:00.123+0700");
        Assert.assertTrue("2013-01-05 14:00:00.123".equals(strTS.convert("2013-01-05 10:00:00.123+0700").toString()));
    }

    @Test
    public void testStringFMTTZToTimestampTZ_12456789precision() throws ParseException {
        final String srcfmt0 = "yyyy-MM-dd hh:mm:ss.SSSSSS";
        final String sourcets0 = "2015-11-17 02:00:00.111111";
        final String targetts0 = "2015-11-17 05:00:00.111111";
        ConnectorDataTypeConverter converter = new ConnectorDataTypeConverter.StringFMTTZToTimestampTZ(srcfmt0, "PST", "EST");
        Assert.assertTrue(targetts0.equals(converter.convert(sourcets0).toString()));
        final String srcfmt2 = "yyyy-MM-dd hh:mm:ss.SSSSSS a";
        final String sourcets2 = "2015-11-17 11:59:00.999999 PM";
        final String targetts2 = "2015-11-18 02:59:00.999999";
        converter = new ConnectorDataTypeConverter.StringFMTTZToTimestampTZ(srcfmt2, "PST", "EST");
        Assert.assertTrue(targetts2.equals(converter.convert(sourcets2).toString()));
        final String srcfmt3 = "yyyy-MM-dd hh:mm:ss.SSSSSSZ";
        final String sourcets3 = "2015-11-17 02:59:00.123456-05:00";
        final String targetts3 = "2015-11-16 23:59:00.123456";
        converter = new ConnectorDataTypeConverter.StringFMTTZToTimestampTZ(srcfmt3, "", "PST");
        Assert.assertTrue(targetts3.equals(converter.convert(sourcets3).toString()));
        final String srcfmt4 = "yyyy-MM-dd hh:mm:ss.SSSSSS";
        final String sourcets4 = "2015-08-17 02:00:00.222222";
        final String targetts4 = "2015-08-17 05:00:00.222222";
        converter = new ConnectorDataTypeConverter.StringFMTTZToTimestampTZ(srcfmt4, "America/Los_Angeles", "America/New_York");
        Assert.assertTrue(targetts4.equals(converter.convert(sourcets4).toString()));
        final String srcfmt5 = "yyyy-MM-dd hh:mm:ss.SSSSSSZ";
        final String sourcets5 = "2015-08-17 02:00:00.333333-07:00";
        final String targetts5 = "2015-08-17 05:00:00.333333";
        converter = new ConnectorDataTypeConverter.StringFMTTZToTimestampTZ(srcfmt5, "", "America/New_York");
        Assert.assertTrue(targetts5.equals(converter.convert(sourcets5).toString()));
        final String srcfmt6 = "yyyy-MM-dd hh:mm:ss.SSSSSSa";
        final String sourcets6 = "2015-08-17 08:00:00.444444PM";
        final String targetts6 = "2015-08-18 03:00:00.444444";
        converter = new ConnectorDataTypeConverter.StringFMTTZToTimestampTZ(srcfmt6, "America/Los_Angeles", "UTC");
        Assert.assertTrue(targetts6.equals(converter.convert(sourcets6).toString()));
        final String srcfmt7 = "yyyy-MM-dd hh:mm:ss.SSSSSSZa";
        final String sourcets7 = "2015-11-17 08:00:00.444444-08:00PM";
        final String targetts7 = "2015-11-18 04:00:00.444444";
        converter = new ConnectorDataTypeConverter.StringFMTTZToTimestampTZ(srcfmt7, "", "UTC");
        Assert.assertTrue(targetts7.equals(converter.convert(sourcets7).toString()));
        final String srcfmt8 = "";
        final String sourcets8 = "2013-01-05 10:00:00.123";
        final String targetts8 = "2013-01-05 12:00:00.123";
        converter = new ConnectorDataTypeConverter.StringFMTTZToTimestampTZ(srcfmt8, "GMT+09:00", "GMT+11:00");
        Assert.assertTrue(targetts8.equals(converter.convert(sourcets8).toString()));
        final String srcfmt9 = "yyyy-MM-dd HH:mm:ss.SSSSSSZ";
        final String sourcets9 = "2013-01-05 10:00:00.123456+0700";
        final String targetts9 = "2013-01-05 14:00:00.123456";
        converter = new ConnectorDataTypeConverter.StringFMTTZToTimestampTZ(srcfmt9, "GMT+09:00", "GMT+11:00");
        Assert.assertTrue(targetts9.equals(converter.convert(sourcets9).toString()));
        final String srcfmt10 = "yyyy-MM-dd HH:mm:ss.S";
        final String sourcets10 = "2015-11-17 02:00:00.1";
        final String targetts10 = "2015-11-17 05:00:00.1";
        converter = new ConnectorDataTypeConverter.StringFMTTZToTimestampTZ(srcfmt10, "PST", "EST");
        Assert.assertTrue(targetts10.equals(converter.convert(sourcets10).toString()));
        final String srcfmt11 = "yyyy-MM-dd HH:mm:ss.SS";
        final String sourcets11 = "2015-11-17 02:00:00.22";
        final String targetts11 = "2015-11-17 05:00:00.22";
        converter = new ConnectorDataTypeConverter.StringFMTTZToTimestampTZ(srcfmt11, "PST", "EST");
        Assert.assertTrue(targetts11.equals(converter.convert(sourcets11).toString()));
        final String srcfmt12 = "yyyy-MM-dd HH:mm:ss.SSS";
        final String sourcets12 = "2015-11-17 02:00:00.333";
        final String targetts12 = "2015-11-17 05:00:00.333";
        converter = new ConnectorDataTypeConverter.StringFMTTZToTimestampTZ(srcfmt12, "PST", "EST");
        Assert.assertTrue(targetts12.equals(converter.convert(sourcets12).toString()));
        final String srcfmt13 = "yyyy-MM-dd HH:mm:ss.SSSS";
        final String sourcets13 = "2015-11-17 02:00:00.4444";
        final String targetts13 = "2015-11-17 05:00:00.4444";
        converter = new ConnectorDataTypeConverter.StringFMTTZToTimestampTZ(srcfmt13, "PST", "EST");
        Assert.assertTrue(targetts13.equals(converter.convert(sourcets13).toString()));
        final String srcfmt14 = "yyyy-MM-dd HH:mm:ss.SSSSS";
        final String sourcets14 = "2015-11-17 02:00:00.55555";
        final String targetts14 = "2015-11-17 05:00:00.55555";
        converter = new ConnectorDataTypeConverter.StringFMTTZToTimestampTZ(srcfmt14, "PST", "EST");
        Assert.assertTrue(targetts14.equals(converter.convert(sourcets14).toString()));
        final String srcfmt15 = "yyyy-MM-dd HH:mm:ss.SSSSSSS";
        final String sourcets15 = "2015-11-17 02:00:00.7777777";
        final String targetts15 = "2015-11-17 05:00:00.7777777";
        converter = new ConnectorDataTypeConverter.StringFMTTZToTimestampTZ(srcfmt15, "PST", "EST");
        Assert.assertTrue(targetts15.equals(converter.convert(sourcets15).toString()));
        final String srcfmt16 = "yyyy-MM-dd HH:mm:ss.SSSSSSSS";
        final String sourcets16 = "2015-11-17 02:00:00.88888888";
        final String targetts16 = "2015-11-17 05:00:00.88888888";
        converter = new ConnectorDataTypeConverter.StringFMTTZToTimestampTZ(srcfmt16, "PST", "EST");
        Assert.assertTrue(targetts16.equals(converter.convert(sourcets16).toString()));
        final String srcfmt17 = "yyyy-MM-dd HH:mm:ss.SSSSSSSSS";
        final String sourcets17 = "2015-11-17 02:00:00.999999999";
        final String targetts17 = "2015-11-17 05:00:00.999999999";
        converter = new ConnectorDataTypeConverter.StringFMTTZToTimestampTZ(srcfmt17, "PST", "EST");
        Assert.assertTrue(targetts17.equals(converter.convert(sourcets17).toString()));
        boolean exceptionThrown = false;
        try {
            final String srcfmt18 = "yyyyMMdd hh:mm:ss";
            final String sourcets18 = "2013-10-10 12:12:12.123+0700";
            converter = new ConnectorDataTypeConverter.StringFMTTZToTimestampTZ(srcfmt18, "PST", "EST");
            converter.convert(sourcets18);
        } catch (Exception e) {
            exceptionThrown = true;
        }
        Assert.assertTrue(exceptionThrown);
        exceptionThrown = false;
        try {
            final String srcfmt18 = "yyyyMMdd HH:mm:ss";
            final String sourcets18 = "2013-10-10 12:12:12.123456-0800";
            final String targetts18 = "2013-10-10 15:12:12.123456";
            final Configuration conf = new Configuration();
            conf.set("tdch.input.timestamp.format.1", "yyyy-MM-dd HH:mm:ss.SSSSSS");
            conf.set("tdch.input.timestamp.format.2", "yyyy-MM-dd HH:mm:ss.SSSSSSZ");
            converter = new ConnectorDataTypeConverter.StringFMTTZToTimestampTZ(srcfmt18, "", "EST").setupBackupDateFormat(conf);
            Assert.assertTrue(converter.convert(sourcets18).toString().equals(targetts18));
        } catch (Exception e) {
            System.out.println("Caught exception " + e.getMessage());
            exceptionThrown = true;
        }
        Assert.assertTrue(!exceptionThrown);
    }

    @Test
    public void testStringFMTToTimeTZ() {
        ConnectorDataTypeConverter.StringFMTTZToTimeTZ c = new ConnectorDataTypeConverter.StringFMTTZToTimeTZ("", "GMT-02:00", "GMT+6:00");
        Assert.assertTrue(c.convert("9:00:00").toString().equals("17:00:00"));
        c = new ConnectorDataTypeConverter.StringFMTTZToTimeTZ("HH:mm:ssZ", "GMT-02:00", "GMT+6:00");
        Assert.assertTrue(c.convert("9:00:00+0500").toString().equals("10:00:00"));
        c = new ConnectorDataTypeConverter.StringFMTTZToTimeTZ("HH:mm:ss.SSSZ", "GMT-03:00", "GMT+2:00");
        c.convert("10:00:02.000+0400");
    }

    @Test
    public void testStringFMTToDateTZ() throws ParseException {
        final ConnectorDataTypeConverter.StringFMTTZToDateTZ sfmt = new ConnectorDataTypeConverter.StringFMTTZToDateTZ("dd/MM/yyyy HH:mm:ss", "GMT-2", "GMT+6:00");
        sfmt.convert("5/1/2012 22:00:00");
        Assert.assertTrue("2012-01-06".equals(sfmt.convert("5/1/2012 22:00:00").toString()));
    }

    @Test
    public void testLongToTTZ() throws ParseException {
        final Date d = toDate("2013/1/5 10:00:00");
        final Calendar c_800 = Calendar.getInstance(TimeZone.getTimeZone("GMT+11:00"));
        c_800.setTime(d);
        final Time t = Time.valueOf("10:12:32");
        final ConnectorDataTypeConverter.TimeTZToLong toLong = new ConnectorDataTypeConverter.TimeTZToLong("GMT+11:00");
        final long l = (long) toLong.convert(t);
        final Time newTime = (Time) new ConnectorDataTypeConverter.LongToTimeTZ("GMT+11:00").convert(l);
        Assert.assertTrue("10:12:32".equals(newTime.toString()));
    }

    @Test
    public void testLongToDateTZ() throws ParseException {
        final SimpleDateFormat df = new SimpleDateFormat("yyyy/MM/dd");
        long timestamp = 1420070400000L;
        String dateText = df.format(new ConnectorDataTypeConverter.LongToDateTZ("US/Pacific").convert(timestamp));
        Assert.assertTrue(dateText.equals("2015/01/01"));
        timestamp = 1430438400000L;
        dateText = df.format(new ConnectorDataTypeConverter.LongToDateTZ("US/Pacific").convert(timestamp));
        Assert.assertTrue(dateText.equals("2015/05/01"));
        timestamp = 1441065600000L;
        dateText = df.format(new ConnectorDataTypeConverter.LongToDateTZ("GMT-05:00").convert(timestamp));
        Assert.assertTrue(dateText.equals("2015/09/01"));
    }

    @Test
    public void testTimestampTZToLong() throws ParseException {
        final ConnectorDataTypeConverter.TimestampTZToLong toLong = new ConnectorDataTypeConverter.TimestampTZToLong("GMT+4");
        final long l = (long) toLong.convert(Timestamp.valueOf("2013-10-08 05:03:02"));
        final ConnectorDataTypeConverter.LongToTimestampTZ n = new ConnectorDataTypeConverter.LongToTimestampTZ("GMT+4");
        final Timestamp ts = (Timestamp) n.convert(l);
        Assert.assertTrue("2013-10-08 05:03:02.0".equals(ts.toString()));
    }

    @Test
    public void testMultiDateFormat() {
        final Configuration configuration = new Configuration();
        configuration.set("tdch.input.date.format.1", "yyyy-MM-dd");
        configuration.set("tdch.input.date.format.2", "yyyyMMdd");
        configuration.set("tdch.input.date.format.3", "dd/MM/yyyy");
        configuration.set("tdch.input.date.format.4", "dd'**'MM'**'yyyy");
        final ConnectorDataTypeConverter.StringFMTTZToDateTZ c = new ConnectorDataTypeConverter.StringFMTTZToDateTZ("yy/MM/dd", "", "");
        c.setupBackupDateFormat(configuration);
        Assert.assertTrue(c.convert("12/10/10").toString().equals("2012-10-10"));
        Assert.assertTrue(c.convert("20121010").toString().equals("2012-10-10"));
        Assert.assertTrue(c.convert("10**12**2019").toString().equals("2019-12-10"));
    }

    @Test
    public void testMultiTimeFormat() {
        final Configuration configuration = new Configuration();
        configuration.set("tdch.input.time.format.1", "HH//mm:ss");
        configuration.set("tdch.input.time.format.2", "HH:mm//ss");
        configuration.set("tdch.input.time.format.3", "HH:mm////ss");
        configuration.set("tdch.input.time.format.4", "HH:mm//////ss");
        final ConnectorDataTypeConverter.StringFMTTZToTimeTZ c = new ConnectorDataTypeConverter.StringFMTTZToTimeTZ("hh:mm:ss", "GMT+03:00", "GMT+04:00");
        c.setupBackupDateFormat(configuration);
        Assert.assertTrue(c.convert("12//10:10").toString().equals("13:10:10"));
        Assert.assertTrue(c.convert("12:10//10").toString().equals("13:10:10"));
        Assert.assertTrue(c.convert("12:10////10").toString().equals("13:10:10"));
        Assert.assertTrue(c.convert("12:10//////10").toString().equals("13:10:10"));
    }

    @Test
    public void testMultiTimestampFormat() {
        final Configuration configuration = new Configuration();
        configuration.set("tdch.input.timestamp.format.1", "yyyy-MM-dd hh:mm:ss.SSSZ");
        configuration.set("tdch.input.timestamp.format.2", "hh:mm:ss.SSSZ");
        configuration.set("tdch.input.timestamp.format.3", "yyyy-MM-dd hh:mm:ss.SSS");
        configuration.set("tdch.input.timestamp.format.4", "yyyy-MM-dd hh:mm:ss");
        final ConnectorDataTypeConverter.StringFMTTZToTimestampTZ c = new ConnectorDataTypeConverter.StringFMTTZToTimestampTZ("yyyyMMdd hh:mm:ss", "GMT+3", "GMT+6");
        c.setupBackupDateFormat(configuration);
        Assert.assertTrue(c.convert("20121010 07:12:12").toString().equals("2012-10-10 10:12:12.0"));
        Assert.assertTrue(c.convert("2013-10-10 07:12:12.123+0700").toString().equals("2013-10-10 06:12:12.123"));
        Assert.assertTrue(c.convert("07:10:11.123+0800").toString().equals("1970-01-01 05:10:11.123"));
        Assert.assertTrue(c.convert("2013-10-10 07:12:12.123").toString().equals("2013-10-10 10:12:12.123"));
        Assert.assertTrue(c.convert("2013-10-10 07:12:12").toString().equals("2013-10-10 10:12:12.0"));
    }

    public static String toString(final Calendar c_800) {
        return c_800.get(1) + "/" + (c_800.get(2) + 1) + "/" + c_800.get(5) + " " + c_800.get(10) + ":" + c_800.get(12) + ":" + c_800.get(13) + " " + ((c_800.get(9) == 1) ? "PM" : "AM") + "  " + c_800.getTimeZone().getID();
    }

    public static String toString2(final Calendar c_800) {
        return c_800.get(1) + "/" + (c_800.get(2) + 1) + "/" + c_800.get(5) + " " + c_800.get(10) + ":" + c_800.get(12) + ":" + c_800.get(13) + "." + c_800.get(14) + " " + ((c_800.get(9) == 1) ? "PM" : "AM") + "  " + c_800.getTimeZone().getID();
    }

    public static java.sql.Date toDate(final String s) throws ParseException {
        final DateFormat df = new SimpleDateFormat("yyyy/MM/dd HH:mm:ss");
        return new java.sql.Date(df.parse(s).getTime());
    }

    public static java.sql.Date toDate2(final String s) throws ParseException {
        final DateFormat df = new SimpleDateFormat("yyyy/MM/dd");
        return new java.sql.Date(df.parse(s).getTime());
    }

    public static java.sql.Date toDate3(final String s) throws ParseException {
        final DateFormat df = new SimpleDateFormat("HH:mm:ss");
        return new java.sql.Date(df.parse(s).getTime());
    }

    public static String calendarToString(final Calendar cal) {
        return toString(cal);
    }

    @Test
    public void testTimeTZToCalendar() throws ParseException {
        final ConnectorDataTypeConverter.TimeTZToCalendar c = new ConnectorDataTypeConverter.TimeTZToCalendar("GMT+2", "GMT+7");
        Calendar cal = (Calendar) c.convert(Time.valueOf("04:39:32"));
        calendarToString(cal);
        Assert.assertTrue("1970/1/1 9:39:32 AM  GMT+07:00".equals(calendarToString(cal)));
        final DateFormat df = new SimpleDateFormat("HH:mm:ss.SSSZ");
        final Date d = df.parse("04:39:32.000+0400");
        final Calendar internalCal = df.getCalendar();
        if (internalCal.isSet(15)) {
            final long l = ConnectorDataTypeConverter.convertMillisecFromDefaultToTargetTZ(d.getTime(), TimeZone.getTimeZone("GMT+2"));
            cal = (Calendar) c.convert(new Time(l));
            calendarToString(cal);
            Assert.assertTrue("1970/1/1 7:39:32 AM  GMT+07:00".equals(calendarToString(cal)));
        } else {
            Assert.assertTrue(false);
        }
    }

    @Test
    public void testTimestampTZToCalendar() throws ParseException {
        final ConnectorDataTypeConverter.TimestampTZToCalendar n = new ConnectorDataTypeConverter.TimestampTZToCalendar("GMT+2", "GMT+7");
        Calendar cal = (Calendar) n.convert(Timestamp.valueOf("2012-10-10 04:23:24.123"));
        calendarToString(cal);
        Assert.assertTrue("2012/10/10 9:23:24 AM  GMT+07:00".equals(calendarToString(cal)));
        final DateFormat df = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSSZ");
        final Date d = df.parse("2013-10-12 03:21:39.129+0400");
        final Calendar internalCal = df.getCalendar();
        if (internalCal.isSet(15)) {
            final long l = ConnectorDataTypeConverter.convertMillisecFromDefaultToTargetTZ(d.getTime(), TimeZone.getTimeZone("GMT+2"));
            final Timestamp ts = new Timestamp(l);
            cal = (Calendar) n.convert(ts);
            calendarToString(cal);
            Assert.assertTrue("2013/10/12 6:21:39 AM  GMT+07:00".equals(calendarToString(cal)));
        } else {
            Assert.assertTrue(false);
        }
    }

    @Test
    public void testDateToCalendar() throws ParseException {
        final ConnectorDataTypeConverter.DateToCalendar d = new ConnectorDataTypeConverter.DateToCalendar("GMT+9");
        Assert.assertTrue("2012/10/10 1:0:0 AM  GMT+09:00".equals(calendarToString((Calendar) d.convert(java.sql.Date.valueOf("2012-10-10")))));
    }

    @Test
    public void testStringFMTTZToCalendar() throws ParseException {
        ConnectorDataTypeConverter.StringFMTTZToCalendarTimestamp c = new ConnectorDataTypeConverter.StringFMTTZToCalendarTimestamp("yyyy-MM-dd HH:mm:ss.SSSZ", "GMT+2", "GMT+6");
        final Configuration configuration = new Configuration();
        configuration.set("tdch.input.timestamp.format.1", "yyyy-MM-dd HH:mm:ss.SSS");
        c.setupBackupDateFormat(configuration, "tdch.input.timestamp.format");
        Calendar cal1 = (Calendar) c.convert("2012-10-10 09:12:23.123");
        Assert.assertTrue("2012/10/10 1:12:23 PM  GMT+06:00".equals(calendarToString(cal1)));
        calendarToString(cal1);
        final Calendar cal2 = (Calendar) c.convert("2012-10-10 08:12:32.132+0500");
        Assert.assertTrue("2012/10/10 9:12:32 AM  GMT+06:00".equals(calendarToString(cal2)));
        calendarToString(cal2);
        c = new ConnectorDataTypeConverter.StringFMTTZToCalendarTimestamp("", "GMT+2", "GMT+6");
        cal1 = (Calendar) c.convert("2012-10-10 09:12:23.123");
        calendarToString(cal1);
        Assert.assertTrue("2012/10/10 1:12:23 PM  GMT+06:00".equals(calendarToString(cal1)));
    }

    @Test
    public void testCalendarToCalendar() throws ParseException {
        final ConnectorDataTypeConverter.CalendarToCalendar c = new ConnectorDataTypeConverter.CalendarToCalendar("GMT+10");
        final Calendar cal = Calendar.getInstance(TimeZone.getTimeZone("GMT+7"));
        cal.setTimeInMillis(getTimeAsLong("2013-12-13 10:10:12.132+0700"));
        Assert.assertTrue(TeradataSchemaUtils.getTimestampFromCalendar((Calendar) c.convert(cal)).toString().equals("2013-12-13 13:10:12.132"));
        TeradataSchemaUtils.getTimestampFromCalendar((Calendar) c.convert(cal));
    }

    @Test
    public void testCalendarToTimeTZ() throws ParseException {
        final ConnectorDataTypeConverter.CalendarToTimeTZ conv = new ConnectorDataTypeConverter.CalendarToTimeTZ("GMT+10");
        final Calendar cal = Calendar.getInstance(TimeZone.getTimeZone("GMT+7"));
        cal.setTimeInMillis(getTimeAsLong("2013-12-13 10:10:12.132+0700"));
        final Time ts = (Time) conv.convert(cal);
        Assert.assertTrue("13:10:12".equals(ts.toString()));
    }

    @Test
    public void testCalendarToTimestampTZ() throws ParseException {
        final ConnectorDataTypeConverter.CalendarToTimestampTZ conv = new ConnectorDataTypeConverter.CalendarToTimestampTZ("GMT+10");
        final Calendar cal = Calendar.getInstance(TimeZone.getTimeZone("GMT+7"));
        cal.setTimeInMillis(getTimeAsLong("2013-12-13 10:10:12.132+0700"));
        final Timestamp ts = (Timestamp) conv.convert(cal);
        Assert.assertTrue("2013-12-13 13:10:12.132".equals(ts.toString()));
    }

    @Test
    public void testCalendarToDateTZ() throws ParseException {
        final ConnectorDataTypeConverter.CalendarToDateTZ conv1 = new ConnectorDataTypeConverter.CalendarToDateTZ("GMT-15");
        final Calendar cal = Calendar.getInstance(TimeZone.getTimeZone("GMT+7"));
        cal.setTimeInMillis(getTimeAsLong("2013-12-13 10:10:12.132+0700"));
        final java.sql.Date d = (java.sql.Date) conv1.convert(cal);
        Assert.assertTrue("2013-12-12".equals(d.toString()));
    }

    @Test
    public void testCalendarToLongTZALongTZToCalendar() throws ParseException {
        final ConnectorDataTypeConverter.CalendarToLong conv1 = new ConnectorDataTypeConverter.CalendarToLong();
        final Calendar cal = Calendar.getInstance(TimeZone.getTimeZone("GMT+7"));
        cal.setTimeInMillis(getTimeAsLong("2013-12-13 10:10:12.132+0700"));
        final long l1 = (long) conv1.convert(cal);
        final ConnectorDataTypeConverter.LongToCalendar conv2 = new ConnectorDataTypeConverter.LongToCalendar("GMT+7");
        final String st = new ConnectorDataTypeConverter.CalendarToStringFMTTZ("GMT+07:00", "yyyy-MM-dd HH:mm:ss.SSSZ").convert(conv2.convert(l1)).toString();
        Assert.assertTrue("2013-12-13 10:10:12.132+0700".equals(st));
    }

    @Test
    public void testCalendarToInteger() throws ParseException {
        final ConnectorDataTypeConverter.CalendarToInteger conv = new ConnectorDataTypeConverter.CalendarToInteger();
        final Calendar cal = Calendar.getInstance(TimeZone.getTimeZone("GMT-07"));
        cal.setTimeInMillis(getTimeAsLong("2013-12-13 10:10:12.000-0700"));
        final int epochsec = 1386954612;
        final Object o = conv.convert(cal);
        Assert.assertTrue(o instanceof Integer);
        final Integer i = (Integer) o;
        Assert.assertTrue(i == epochsec);
    }

    @Test
    public void testCalendarToInteger_2() throws ParseException {
        final ConnectorDataTypeConverter conv = new ConnectorDataTypeConverter.CalendarToInteger();
        Assert.assertTrue(conv.convert(null) == null);
        final Calendar c = Calendar.getInstance(TimeZone.getTimeZone("GMT"));
        final DateFormat ft = new SimpleDateFormat("yyyy-MM-dd HH:mm:ssz");
        final Date d = ft.parse("2015-07-07 23:39:49+0000");
        c.setTime(d);
        final Object o = conv.convert(c);
        Assert.assertTrue(o instanceof Integer);
        final Integer i = (Integer) o;
        final int sec = 1436312389;
        Assert.assertTrue(i == sec);
    }

    @Test
    public void testCalendarToStringFMTTZ() throws ParseException {
        final ConnectorDataTypeConverter.CalendarToStringFMTTZ fmt = new ConnectorDataTypeConverter.CalendarToStringFMTTZ("GMT+06:00", "yyyy-MM-dd HH:mm:ss.SSSZ");
        final Calendar cal = Calendar.getInstance(TimeZone.getTimeZone("GMT+7"));
        cal.setTimeInMillis(getTimeAsLong("2013-12-13 10:10:12.132+0700"));
        final String s = fmt.convert(cal).toString();
        Assert.assertTrue("2013-12-13 09:10:12.132+0600".equals(s));
    }

    @Test
    public void testTimeStampTransformFMT() throws ParseException {
        final String timestampText = "2013-10-11 11:11:10.123";
        HdfsTextTransform.TimeStampTransformFMT t = new HdfsTextTransform.TimeStampTransformFMT("GMT+3", "", ConnectorConfiguration.direction.output);
        String s = t.toString(Timestamp.valueOf(timestampText));
        Assert.assertTrue(s.equals("2013-10-11 11:11:10.123"));
        t = new HdfsTextTransform.TimeStampTransformFMT("GMT+3", "yyyy-MM-dd HH:mm:ss.SSS", ConnectorConfiguration.direction.output);
        s = t.toString(Timestamp.valueOf(timestampText));
        Assert.assertTrue(s.equals("2013-10-11 11:11:10.123"));
        t = new HdfsTextTransform.TimeStampTransformFMT("GMT+3", "yyyy-MM-dd HH:mm:ss.SSSZ", ConnectorConfiguration.direction.output);
        s = t.toString(Timestamp.valueOf(timestampText));
        Assert.assertTrue(s.equals("2013-10-11 11:11:10.123+0300"));
    }

    @Test
    public void testTimeTransformFMT() throws ParseException {
        final String timeText = "11:11:10";
        HdfsTextTransform.TimeTransformFMT t = new HdfsTextTransform.TimeTransformFMT("GMT+3", "", ConnectorConfiguration.direction.output);
        String s = t.toString(Time.valueOf(timeText));
        Assert.assertTrue("11:11:10".equals(s));
        t = new HdfsTextTransform.TimeTransformFMT("GMT+3", "HH:mm:ss.SSS", ConnectorConfiguration.direction.output);
        s = t.toString(Time.valueOf(timeText));
        Assert.assertTrue("11:11:10.000".equals(s));
        t = new HdfsTextTransform.TimeTransformFMT("GMT+3", "HH:mm:ss.SSSZ", ConnectorConfiguration.direction.output);
        s = t.toString(Time.valueOf(timeText));
        Assert.assertTrue("11:11:10.000+0300".equals(s));
    }

    @Test
    public void testTimestampTZToTimestampTZ() throws ParseException {
        final Timestamp t = Timestamp.valueOf("2016-04-23 14:11:33");
        final ConnectorDataTypeConverter.TimestampTZToTimestampTZ c3 = new ConnectorDataTypeConverter.TimestampTZToTimestampTZ("GMT+6", "GMT+7:00");
        t.setNanos(987891000);
        Assert.assertTrue("2016-04-23 15:11:33.987891".equals(c3.convert(t).toString()));
        t.setNanos(987890000);
        Assert.assertTrue("2016-04-23 15:11:33.98789".equals(c3.convert(t).toString()));
        t.setNanos(987800000);
        Assert.assertTrue("2016-04-23 15:11:33.9878".equals(c3.convert(t).toString()));
        t.setNanos(987000000);
        Assert.assertTrue("2016-04-23 15:11:33.987".equals(c3.convert(t).toString()));
        t.setNanos(980000000);
        Assert.assertTrue("2016-04-23 15:11:33.98".equals(c3.convert(t).toString()));
        t.setNanos(900000000);
        Assert.assertTrue("2016-04-23 15:11:33.9".equals(c3.convert(t).toString()));
    }

    @Test
    public void testStringTZToTimestamp() throws ParseException {
        final Configuration configuration = new Configuration();
        configuration.set("tdch.input.timestamp.format.1", "yyyy-MM-dd HH:mm:ss.SZ");
        configuration.set("tdch.input.timestamp.format.2", "yyyy-MM-dd HH:mm:ss.SSZ");
        configuration.set("tdch.input.timestamp.format.3", "yyyy-MM-dd HH:mm:ss.SSSZ");
        configuration.set("tdch.input.timestamp.format.4", "yyyy-MM-dd HH:mm:ss.SSSSZ");
        configuration.set("tdch.input.timestamp.format.5", "yyyy-MM-dd HH:mm:ss.SSSSSZ");
        configuration.set("tdch.input.timestamp.format.6", "yyyy-MM-dd HH:mm:ss.SSSSSSZ");
        configuration.set("tdch.input.timestamp.format.7", "yyyy-MM-dd HH:mm:ss.SSSSSS");
        configuration.set("tdch.input.timestamp.format.8", "yyyy-MM-dd HH:mm:ss.SSSSS");
        configuration.set("tdch.input.timestamp.format.9", "yyyy-MM-dd HH:mm:ss.SSSS");
        configuration.set("tdch.input.timestamp.format.10", "yyyy-MM-dd HH:mm:ss.SSS");
        configuration.set("tdch.input.timestamp.format.11", "yyyy-MM-dd HH:mm:ss.SS");
        configuration.set("tdch.input.timestamp.format.12", "yyyy-MM-dd HH:mm:ss.S");
        configuration.set("tdch.input.timestamp.format.13", "SSSSSS yyyy-MM-dd HH:mm:ss");
        configuration.set("tdch.input.timestamp.format.14", "SSSSS yyyy-MM-dd HH:mm:ss");
        configuration.set("tdch.input.timestamp.format.15", "SSSS yyyy-MM-dd HH:mm:ss");
        configuration.set("tdch.input.timestamp.format.16", "SSS yyyy-MM-dd HH:mm:ss");
        configuration.set("tdch.input.timestamp.format.17", "SS yyyy-MM-dd HH:mm:ss");
        configuration.set("tdch.input.timestamp.format.18", "S yyyy-MM-dd HH:mm:ss");
        configuration.set("tdch.input.timestamp.format.19", "SSSSSS.yyyy-MM-dd HH:mm:ss");
        configuration.set("tdch.input.timestamp.format.20", "yyyy-MM-dd HH:mm:ss");
        configuration.set("tdch.input.timestamp.format.21", "yyyyMMdd hh:mm:ss");
        configuration.set("tdch.input.timestamp.format.22", "hh:mm:ss.SSSZ");
        final ConnectorDataTypeConverter.StringFMTTZToTimestampTZ c3 = new ConnectorDataTypeConverter.StringFMTTZToTimestampTZ("yyyy-MM-dd HH:mm:ss.SSSSSS", "GMT+3", "GMT+6");
        c3.setupBackupDateFormat(configuration);
        Assert.assertTrue(c3.convert("20121010 07:12:12").toString().equals("2012-10-10 10:12:12.0"));
        Assert.assertTrue(c3.convert("2013-10-10 07:12:12.1+0700").toString().equals("2013-10-10 06:12:12.1"));
        Assert.assertTrue(c3.convert("2013-10-10 07:12:12.12+0700").toString().equals("2013-10-10 06:12:12.12"));
        Assert.assertTrue(c3.convert("2013-10-10 07:12:12.123+0700").toString().equals("2013-10-10 06:12:12.123"));
        Assert.assertTrue(c3.convert("2013-1-10 07:12:12.123+0700").toString().equals("2013-01-10 06:12:12.123"));
        Assert.assertTrue(c3.convert("2013-01-10 07:12:12.1234+0700").toString().equals("2013-01-10 06:12:12.1234"));
        Assert.assertTrue(c3.convert("2013-01-10 07:12:12.12345+0700").toString().equals("2013-01-10 06:12:12.12345"));
        Assert.assertTrue(c3.convert("2013-01-10 07:12:12.123456+0700").toString().equals("2013-01-10 06:12:12.123456"));
        Assert.assertTrue(c3.convert("2013-1-10 07:12:12.1234+0700").toString().equals("2013-01-10 06:12:12.1234"));
        Assert.assertTrue(c3.convert("2013-1-10 07:12:12.12345+0700").toString().equals("2013-01-10 06:12:12.12345"));
        Assert.assertTrue(c3.convert("2013-1-10 07:12:12.123456+0700").toString().equals("2013-01-10 06:12:12.123456"));
        Assert.assertTrue(c3.convert("07:10:11.123+0800").toString().equals("1970-01-01 05:10:11.123"));
        Assert.assertTrue(c3.convert("2013-10-10 07:12:12.123").toString().equals("2013-10-10 10:12:12.123"));
        Assert.assertTrue(c3.convert("2013-10-10 07:12:12").toString().equals("2013-10-10 10:12:12.0"));
        Assert.assertTrue("2016-04-23 17:11:33.987891".equals(c3.convert("2016-04-23 14:11:33.987891").toString()));
        Assert.assertTrue("2016-04-23 17:11:33.987891".equals(c3.convert("2016-4-23 14:11:33.987891").toString()));
        Assert.assertTrue("2016-04-02 17:11:33.98789".equals(c3.convert("2016-4-2 14:11:33.98789").toString()));
        Assert.assertTrue("2016-04-23 17:11:33.98789".equals(c3.convert("2016-04-23 14:11:33.98789").toString()));
        Assert.assertTrue("2016-04-23 17:11:33.9878".equals(c3.convert("2016-4-23 14:11:33.9878").toString()));
        Assert.assertTrue("2016-04-23 17:11:33.9878".equals(c3.convert("2016-04-23 14:11:33.9878").toString()));
        Assert.assertTrue("2016-04-03 17:11:33.987".equals(c3.convert("2016-4-3 14:11:33.987").toString()));
        Assert.assertTrue("2016-04-23 17:11:33.987".equals(c3.convert("2016-4-23 14:11:33.987").toString()));
        Assert.assertTrue("2016-04-23 17:11:33.98".equals(c3.convert("2016-04-23 14:11:33.98").toString()));
        Assert.assertTrue("2016-04-23 17:11:33.98".equals(c3.convert("2016-4-23 14:11:33.98").toString()));
        Assert.assertTrue("2016-04-23 17:11:33.9".equals(c3.convert("2016-4-23 14:11:33.9").toString()));
        Assert.assertTrue("2016-04-23 17:11:33.9".equals(c3.convert("2016-04-23 14:11:33.9").toString()));
        Assert.assertTrue("2016-04-23 17:11:33.0".equals(c3.convert("2016-4-23 14:11:33.0").toString()));
        Assert.assertTrue("2016-04-23 17:11:33.0".equals(c3.convert("2016-04-23 14:11:33.0").toString()));
        Assert.assertTrue("2016-04-23 17:11:33.0".equals(c3.convert("2016-4-23 14:11:33").toString()));
        Assert.assertTrue("2016-04-23 17:11:33.0".equals(c3.convert("2016-04-23 14:11:33").toString()));
        Assert.assertTrue("2016-04-23 17:11:33.987891".equals(c3.convert("987891 2016-4-23 14:11:33").toString()));
        Assert.assertTrue("2016-04-23 17:11:33.987891".equals(c3.convert("987891 2016-04-23 14:11:33").toString()));
        Assert.assertTrue("2016-04-23 17:11:33.98789".equals(c3.convert("98789 2016-04-23 14:11:33").toString()));
        Assert.assertTrue("2016-04-23 17:11:33.98789".equals(c3.convert("98789 2016-4-23 14:11:33").toString()));
        Assert.assertTrue("2016-04-23 17:11:33.9878".equals(c3.convert("9878 2016-04-23 14:11:33").toString()));
        Assert.assertTrue("2016-04-23 17:11:33.9878".equals(c3.convert("9878 2016-4-23 14:11:33").toString()));
        Assert.assertTrue("2016-04-23 17:11:33.987".equals(c3.convert("987 2016-4-23 14:11:33").toString()));
        Assert.assertTrue("2016-04-23 17:11:33.987".equals(c3.convert("987 2016-04-23 14:11:33").toString()));
        Assert.assertTrue("2016-04-23 17:11:33.98".equals(c3.convert("98 2016-4-23 14:11:33").toString()));
        Assert.assertTrue("2016-04-23 17:11:33.98".equals(c3.convert("98 2016-04-23 14:11:33").toString()));
        Assert.assertTrue("2016-04-23 17:11:33.9".equals(c3.convert("9 2016-4-23 14:11:33").toString()));
        Assert.assertTrue("2016-04-23 17:11:33.9".equals(c3.convert("9 2016-04-23 14:11:33").toString()));
        Assert.assertTrue("2016-04-23 17:11:33.987891".equals(c3.convert("987891.2016-4-23 14:11:33").toString()));
        Assert.assertTrue("2016-04-23 17:11:33.987891".equals(c3.convert("987891.2016-04-23 14:11:33").toString()));
    }

    public static long getTimeAsLong(final String s) throws ParseException {
        final DateFormat df = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSSZ");
        return df.parse(s).getTime();
    }
}
