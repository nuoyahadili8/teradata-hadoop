package com.teradata.connector.hdfs.utils;

import java.math.BigDecimal;
import java.sql.Date;

import junit.framework.Assert;
import org.junit.Test;


public class HdfsTextTransformTest {
    @Test
    public void testHdfsTextTransform() {
        Assert.assertEquals((Object) 15, new HdfsTextTransform.IntTransform().transform("15"));
        Assert.assertEquals((Object) 999915L, new HdfsTextTransform.BigIntTransform().transform("999915"));
        Assert.assertEquals((Object) 15, new HdfsTextTransform.SmallIntTransform().transform("15"));
        Assert.assertEquals((Object) 15, new HdfsTextTransform.TinyIntTransform().transform("15"));
        Assert.assertEquals((Object) new BigDecimal(".000015"), new HdfsTextTransform.DecimalTransform().transform(".000015"));
        final Date fmtDate = (Date) new HdfsTextTransform.DateTransformFMT("dd-M-yyyy hh:mm:ss").transform("31-08-1982 10:20:56");
        Assert.assertEquals("1982-08-31", fmtDate.toString());
        Assert.assertEquals((Object) 1.0, new HdfsTextTransform.DoubleTransform().transform("1"));
        byte[] bdata = (byte[]) new HdfsTextTransform.BinaryTransform().transform("");
        Assert.assertEquals(0, bdata.length);
        bdata = (byte[]) new HdfsTextTransform.BinaryTransform().transform("f");
        Assert.assertEquals(102, (int) bdata[1]);
        bdata = (byte[]) new HdfsTextTransform.BinaryTransform().transform("7f");
        Assert.assertEquals(55, (int) bdata[1]);
        Assert.assertEquals(102, (int) bdata[3]);
        bdata = (byte[]) new HdfsTextTransform.BinaryTransform().transform("e07fabc0");
        Assert.assertEquals(101, (int) bdata[1]);
        Assert.assertEquals(48, (int) bdata[3]);
        Assert.assertEquals(55, (int) bdata[5]);
        Assert.assertEquals(102, (int) bdata[7]);
        Assert.assertEquals(97, (int) bdata[9]);
        Assert.assertEquals(98, (int) bdata[11]);
        Assert.assertEquals(99, (int) bdata[13]);
        Assert.assertEquals(48, (int) bdata[15]);
        bdata = (byte[]) new HdfsTextTransform.BlobTransform().transform("7f");
        Assert.assertEquals(127, (int) bdata[0]);
        Assert.assertEquals((Object) true, new HdfsTextTransform.BooleanTransform().transform("true"));
    }
}
