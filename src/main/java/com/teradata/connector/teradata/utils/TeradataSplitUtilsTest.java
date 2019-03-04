package com.teradata.connector.teradata.utils;

import java.lang.reflect.Method;
import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.List;
import org.apache.hadoop.conf.Configuration;
import org.junit.Assert;
import org.junit.Test;


public class TeradataSplitUtilsTest
{
    @Test
    public void testStringToBigDecimal() throws Exception {
        final Method method = TeradataSplitUtils.class.getDeclaredMethod("stringToBigDecimal", String.class);
        method.setAccessible(true);
        Assert.assertEquals((Object)new BigDecimal(9.918366558849812E-4), method.invoke(null, "AB"));
        final Method method2 = TeradataSplitUtils.class.getDeclaredMethod("bigDecimalToString", BigDecimal.class);
        method2.setAccessible(true);
        Assert.assertEquals((Object)"AB", method2.invoke(null, new BigDecimal(9.918366558849812E-4)));
        final TeradataSplitUtils t = new TeradataSplitUtils();
        List<String> splitStrings = new ArrayList<String>();
        List<String> splitStringsOP = new ArrayList<String>();
        splitStringsOP.add("10330045A1");
        splitStringsOP.add("10330145A1");
        splitStringsOP.add("10330245A1");
        splitStringsOP.add("10330345A1");
        splitStrings = TeradataSplitUtils.getStringSplitPoints(4, "10330045A1", "10330535A67898");
        Assert.assertEquals(4, splitStrings.size());
        Assert.assertEquals((Object)splitStringsOP, (Object)splitStrings);
        splitStrings = new ArrayList<String>();
        splitStrings = TeradataSplitUtils.getStringSplitPoints(70, "000301542582", "998392158163");
        Assert.assertEquals(70, splitStrings.size());
        splitStrings = new ArrayList<String>();
        splitStringsOP = new ArrayList<String>();
        splitStringsOP.add("000301542582");
        splitStringsOP.add("249824196477");
        splitStringsOP.add("499346850373");
        splitStringsOP.add("748869504268");
        splitStrings = TeradataSplitUtils.getStringSplitPoints(4, "000301542582", "998392158163");
        Assert.assertEquals(4, splitStrings.size());
        Assert.assertEquals((Object)splitStringsOP, (Object)splitStrings);
        splitStrings = new ArrayList<String>();
        splitStrings = TeradataSplitUtils.getStringSplitPoints(70, "10330045A1", "10330535A6712345678987123712345678987123");
        Assert.assertEquals(70, splitStrings.size());
        splitStrings = new ArrayList<String>();
        splitStrings = TeradataSplitUtils.getStringSplitPoints(60, "10330045A1", "10330535A6712345678987123712345678987123");
        Assert.assertEquals(60, splitStrings.size());
        splitStrings = new ArrayList<String>();
        splitStrings = TeradataSplitUtils.getStringSplitPoints(61, "10330045A1", "10330535A6712345678987123712345678987123");
        Assert.assertEquals(61, splitStrings.size());
        splitStrings = new ArrayList<String>();
        splitStrings = TeradataSplitUtils.getStringSplitPoints(31, "10330045A1", "10330535A6712345678987123712345678987123");
        Assert.assertEquals(31, splitStrings.size());
        splitStrings = new ArrayList<String>();
        splitStrings = TeradataSplitUtils.getStringSplitPoints(8, "10330045A1", "10330535A671234567898712371234567898712");
        Assert.assertEquals(8, splitStrings.size());
        splitStrings = new ArrayList<String>();
        splitStrings = TeradataSplitUtils.getStringSplitPoints(10, "10330045A1", "10330535A67123456789871237123456789871234");
        Assert.assertEquals(10, splitStrings.size());
        splitStrings = new ArrayList<String>();
        splitStrings = TeradataSplitUtils.getStringSplitPoints(10, "10330045A1", "10330535A6712345678987123712345678987123");
        Assert.assertEquals(10, splitStrings.size());
        splitStrings = new ArrayList<String>();
        splitStrings = TeradataSplitUtils.getStringSplitPoints(4, "10330045A1", "10331045A1");
        Assert.assertEquals(4, splitStrings.size());
        splitStrings = new ArrayList<String>();
        splitStrings = TeradataSplitUtils.getStringSplitPoints(4, "10330045A1", "10334045A1");
        Assert.assertEquals(4, splitStrings.size());
        splitStrings = new ArrayList<String>();
        splitStrings = TeradataSplitUtils.getStringSplitPoints(17, "10300045A1", "10390045A1");
        Assert.assertEquals(17, splitStrings.size());
        splitStrings = new ArrayList<String>();
        splitStrings = TeradataSplitUtils.getStringSplitPoints(9, "10300045A1", "10330045A1");
        Assert.assertEquals(9, splitStrings.size());
        splitStrings = new ArrayList<String>();
        splitStringsOP = new ArrayList<String>();
        splitStringsOP.add("10300045A1");
        splitStringsOP.add("1030?045A1");
        splitStringsOP.add("10310045A1");
        splitStringsOP.add("1031?045A1");
        splitStringsOP.add("10320045A1");
        splitStringsOP.add("1032c045A1");
        splitStringsOP.add("1032?045A1");
        splitStringsOP.add("1032\u00c9045A1");
        splitStrings = TeradataSplitUtils.getStringSplitPoints(8, "10300045A1", "10330045A1");
        Assert.assertEquals(8, splitStrings.size());
        Assert.assertEquals((Object)splitStringsOP, (Object)splitStrings);
        splitStrings = new ArrayList<String>();
        splitStrings = TeradataSplitUtils.getStringSplitPoints(3, "10000045A1", "10800045A1");
        Assert.assertEquals(3, splitStrings.size());
        splitStrings = new ArrayList<String>();
        splitStrings = TeradataSplitUtils.getStringSplitPoints(8, "10000045A1", "10900045A1");
        Assert.assertEquals(8, splitStrings.size());
        splitStrings = new ArrayList<String>();
        splitStringsOP = new ArrayList<String>();
        splitStringsOP.add("10000045A1");
        splitStringsOP.add("10200045A1");
        splitStringsOP.add("10400045A1");
        splitStringsOP.add("10600045A1");
        splitStrings = TeradataSplitUtils.getStringSplitPoints(4, "10000045A1", "10800045A1");
        Assert.assertEquals(4, splitStrings.size());
        Assert.assertEquals((Object)splitStringsOP, (Object)splitStrings);
    }
    
    @Test
    public void testGetSplitsByBooleanType() {
        Assert.assertEquals(2, TeradataSplitUtils.getSplitsByBooleanType(new Configuration(), "", false, true).size());
    }
    
    @Test
    public void testGetSupportedCharCode() {
        Assert.assertEquals(65470, TeradataSplitUtils.getSupportedCharCode(65535));
        Assert.assertEquals(65281, TeradataSplitUtils.getSupportedCharCode(65280));
    }
}
