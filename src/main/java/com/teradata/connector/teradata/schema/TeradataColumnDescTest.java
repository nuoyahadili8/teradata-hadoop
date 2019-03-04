package com.teradata.connector.teradata.schema;

import org.junit.Assert;
import org.junit.Test;

public class TeradataColumnDescTest {
    @Test
    public void testGetScale() {
        final TeradataColumnDesc desc = new TeradataColumnDesc();
        desc.setType(93);
        desc.setScale(7);
        Assert.assertEquals(6, desc.getScale());
        desc.setType(2002);
        desc.setTypeName("TIME_PERIOD");
        Assert.assertEquals(6, desc.getScale());
    }

    @Test
    public void testGetLobLengthInKMG() {
        final TeradataColumnDesc desc = new TeradataColumnDesc();
        desc.setLength(2048L);
        Assert.assertEquals("2K", desc.getLobLengthInKMG());
        desc.setLength(4194304L);
        Assert.assertEquals("4M", desc.getLobLengthInKMG());
        desc.setLength(8589934592L);
        Assert.assertEquals("8G", desc.getLobLengthInKMG());
    }

    @Test
    public void testGetTypeString4Using() {
        final TeradataColumnDesc desc = new TeradataColumnDesc();
        desc.setType(1);
        desc.setLength(20L);
        Assert.assertEquals("VARCHAR(60)", desc.getTypeString4Using("UTF8", 0, 0));
        Assert.assertEquals("VARCHAR(40)", desc.getTypeString4Using("UTF16", 0, 0));
        desc.setLength(4194304L);
        desc.setType(2005);
        Assert.assertEquals("CLOB(4M)", desc.getTypeString4Using("UTF16", 0, 0));
        desc.setType(92);
        Assert.assertEquals("CHAR(16)", desc.getTypeString4Using("UTF16", 0, 0));
        Assert.assertEquals("CHAR(14)", desc.getTypeString4Using("UTF8", 5, 5));
        Assert.assertEquals("CHAR(30)", desc.getTypeString4Using("UTF16", 6, 6));
        desc.setType(93);
        Assert.assertEquals("CHAR(38)", desc.getTypeString4Using("UTF16", 0, 0));
        Assert.assertEquals("CHAR(24)", desc.getTypeString4Using("UTF8", 0, 4));
        Assert.assertEquals("CHAR(52)", desc.getTypeString4Using("UTF16", 0, 6));
        desc.setType(0);
        Assert.assertEquals("", desc.getTypeString4Using("UTF8", 0, 0));
    }

    @Test
    public void testGetTypeStringWithoutNullability() {
        final TeradataColumnDesc desc = new TeradataColumnDesc();
        desc.setType(1);
        desc.setCharType(2);
        desc.setCaseSensitive(true);
        Assert.assertTrue(desc.getTypeStringWithoutNullability().contains("UNICODE"));
        desc.setType(12);
        Assert.assertTrue(desc.getTypeStringWithoutNullability().contains("CASESPECIFIC"));
        desc.setType(-1);
        Assert.assertTrue(desc.getTypeStringWithoutNullability().contains("UNICODE CASESPECIFIC"));
        desc.setType(6);
        Assert.assertTrue(desc.getTypeStringWithoutNullability().contains("FLOAT"));
        desc.setType(7);
        Assert.assertTrue(desc.getTypeStringWithoutNullability().contains("REAL"));
        desc.setType(2);
        desc.setTypeName("NUMERIC");
        Assert.assertTrue(desc.getTypeStringWithoutNullability().contains("NUMERIC(0, 0)"));
        desc.setType(2005);
        Assert.assertTrue(desc.getTypeStringWithoutNullability().contains("UNICODE"));
        desc.setType(2002);
        desc.setTypeName("TIME");
        Assert.assertTrue(desc.getTypeStringWithoutNullability().equals("TIME"));
        desc.setTypeName("TIMESTAMP_WITH_PERIOD");
        desc.setScale(7);
        Assert.assertTrue(desc.getTypeStringWithoutNullability().equals("PERIOD(TIMESTAMP)"));
        desc.setTypeName("TIME_WITH_PERIOD");
        Assert.assertTrue(desc.getTypeStringWithoutNullability().equals("PERIOD(TIME)"));
        desc.setTypeName("PERIOD");
        Assert.assertTrue(desc.getTypeStringWithoutNullability().equals("PERIOD"));
        desc.setType(-7);
        desc.setTypeName("BIT");
        Assert.assertTrue(desc.getTypeStringWithoutNullability().equals("BIT"));
        desc.setType(2009);
        desc.setTypeName("XML");
        Assert.assertTrue(desc.getTypeStringWithoutNullability().equals("XML"));
    }
}
