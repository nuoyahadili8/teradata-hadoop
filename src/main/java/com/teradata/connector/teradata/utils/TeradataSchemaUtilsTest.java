package com.teradata.connector.teradata.utils;

import com.teradata.connector.teradata.schema.*;
import org.junit.Assert;
import org.junit.Test;

import java.util.*;
import java.sql.*;

public class TeradataSchemaUtilsTest {
    @Test
    public void testGetFieldNamesFromSchema() throws Exception {
        final String schema = "f1 INTEGER,f2 BLOB(2048),f3 CLOB(2048) CHARACTER SET LATIN,f4 SYSUDTLIB.TYPE_ARY";
        Assert.assertEquals("\"f1\",\"f2\",\"f3\",\"f4\"", TeradataSchemaUtils.getFieldNamesFromSchema(schema));
    }

    @Test
    public void testColumnSchemaToTableDesc() throws Exception {
        final String schema = "f1 INTEGER,f2 BLOB(2048),f3 CLOB(2048) CHARACTER SET LATIN,f4 SYSUDTLIB.TYPE_ARY";
        final TeradataTableDesc tableDesc = TeradataSchemaUtils.columnSchemaToTableDesc(schema);
        Assert.assertEquals("f1", tableDesc.getColumn(0).getName());
        Assert.assertEquals("BLOB(2048)", tableDesc.getColumn(1).getTypeName());
        Assert.assertEquals(1883, tableDesc.getColumn(3).getType());
    }

    @Test
    public void testGetTimestampFromCalendar() throws Exception {
        final Calendar cal = Calendar.getInstance();
        final long millis = cal.getTimeInMillis();
        final Timestamp ts = TeradataSchemaUtils.getTimestampFromCalendar(cal);
        Assert.assertEquals(millis, ts.getTime());
    }
}
