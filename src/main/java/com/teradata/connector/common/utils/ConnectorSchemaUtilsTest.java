package com.teradata.connector.common.utils;

import java.util.*;

import com.teradata.connector.common.*;

import java.io.*;

import org.apache.hadoop.hive.serde2.io.ByteWritable;
import org.apache.hadoop.hive.serde2.io.DoubleWritable;
import org.apache.hadoop.hive.serde2.io.ShortWritable;
import org.apache.hadoop.io.*;
import org.apache.hadoop.hive.serde2.io.*;
import com.teradata.jdbc.*;
import com.teradata.connector.common.converter.*;
import org.junit.Assert;
import org.junit.Test;

public class ConnectorSchemaUtilsTest {
    @Test
    public void testParseColumnNames() {
        final List<String> columnNames = new ArrayList<String>();
        columnNames.add("\\  \\  ");
        columnNames.add("col\\'1  ");
        Assert.assertEquals("", (String) ConnectorSchemaUtils.parseColumnNames(columnNames).get(0));
        Assert.assertEquals("col'1", (String) ConnectorSchemaUtils.parseColumnNames(columnNames).get(1));
    }

    @Test
    public void testParseColumnTypes() {
        final List<String> columnTypes = new ArrayList<String>();
        columnTypes.add("\\   ");
        Assert.assertEquals("", (String) ConnectorSchemaUtils.parseColumnTypes(columnTypes).get(0));
    }

    @Test
    public void testConcatFieldNamesArray() {
        final String[] fieldNamesArray = new String[0];
        Assert.assertEquals("", ConnectorSchemaUtils.concatFieldNamesArray(fieldNamesArray));
    }

    @Test
    public void testQuoteFieldName() {
        Assert.assertEquals("\"\"", ConnectorSchemaUtils.quoteFieldName("", ""));
    }

    @Test
    public void testQuoteFieldNames() {
        Assert.assertEquals("\"col1\",\"col2\"", ConnectorSchemaUtils.quoteFieldNames("col1,col2"));
    }

    @Test
    public void testQuoteFieldNamesArray() {
        Assert.assertEquals(0, ConnectorSchemaUtils.quoteFieldNamesArray(null).length);
        Assert.assertEquals("[\"col1, col2\"]", Arrays.toString(ConnectorSchemaUtils.quoteFieldNamesArray(new String[]{"col1, col2"})));
    }

    @Test
    public void testQuoteFieldValue() {
        Assert.assertEquals("''", ConnectorSchemaUtils.quoteFieldValue("", ""));
        Assert.assertEquals("'test\"'", ConnectorSchemaUtils.quoteFieldValue("test\"", "\\'"));
    }

    @Test
    public void testUnquoteFieldValue() {
        Assert.assertEquals("test", ConnectorSchemaUtils.unquoteFieldValue("'test'"));
        Assert.assertEquals("", ConnectorSchemaUtils.unquoteFieldValue(null));
    }

    @Test
    public void testUnquoteFieldName() {
        Assert.assertEquals("", ConnectorSchemaUtils.unquoteFieldName(null));
        Assert.assertEquals("test", ConnectorSchemaUtils.unquoteFieldName("\"test\""));
    }

    @Test
    public void testGetHivePathString() {
        Assert.assertEquals("4.3", ConnectorSchemaUtils.getHivePathString(new Double(4.3)));
        Assert.assertEquals("true", ConnectorSchemaUtils.getHivePathString(new Boolean(true)));
        Assert.assertEquals("4", ConnectorSchemaUtils.getHivePathString(4));
        Assert.assertEquals("16", ConnectorSchemaUtils.getHivePathString(16));
        Assert.assertEquals("44", ConnectorSchemaUtils.getHivePathString(new Long(44L)));
    }

    @Test
    public void testLookupUDF() throws Exception {
        final ConnectorRecordSchema schema = new ConnectorRecordSchema(3);
        for (int i = 0; i < 3; ++i) {
            schema.setFieldType(i, 4);
        }
        schema.setDataTypeConverter(0, "TestUserConverter0Args");
        schema.setDataTypeConverter(1, "com.teradata.connector.common.utils.ConnectorSchemaUtilsTest$TestUserConverter1Arg");
        schema.setDataTypeConverter(2, "com.teradata.connector.common.utils.ConnectorSchemaUtilsTest$TestUserConverter2Args");
        schema.setParameters(0, new String[]{"1", "2", "3"});
        Assert.assertEquals((Object) null, (Object) ConnectorSchemaUtils.lookupUDF(schema, 0));
        schema.setParameters(2, new String[]{"1"});
        Assert.assertEquals((Object) null, (Object) ConnectorSchemaUtils.lookupUDF(schema, 2));
        schema.setParameters(1, new String[0]);
        Assert.assertEquals((Object) null, (Object) ConnectorSchemaUtils.lookupUDF(schema, 1));
        schema.setParameters(0, new String[0]);
        Assert.assertTrue(ConnectorSchemaUtils.lookupUDF(schema, 0).toString().contains("TestUserConverter0Args"));
        schema.setParameters(1, new String[]{"1"});
        Assert.assertTrue(ConnectorSchemaUtils.lookupUDF(schema, 1).toString().contains("$TestUserConverter1Arg"));
        schema.setParameters(2, new String[]{"1", "2"});
        Assert.assertTrue(ConnectorSchemaUtils.lookupUDF(schema, 2).toString().contains("$TestUserConverter2Args"));
    }

    @Test
    public void testRecordSchema() throws Exception {
        final ConnectorRecordSchema test = new ConnectorRecordSchema(3);
        final ConnectorRecordSchema newTest = new ConnectorRecordSchema();
        final ByteArrayOutputStream output = new ByteArrayOutputStream();
        final DataOutputStream out = new DataOutputStream(output);
        test.setFieldType(0, 5);
        test.setFieldType(1, 1883);
        test.setFieldType(2, 1883);
        test.setDataTypeConverter(1, "StringToDateFormat");
        test.setDataTypeConverter(2, "StringToDateFormat");
        test.setParameters(1, new String[]{"a", "b", "c"});
        test.setParameters(2, new String[0]);
        test.write(out);
        final byte[] arrayList = output.toByteArray();
        final ByteArrayInputStream input = new ByteArrayInputStream(arrayList);
        final DataInputStream in = new DataInputStream(input);
        newTest.readFields(in);
        Assert.assertEquals(1883, newTest.getFieldType(1));
        Assert.assertTrue(newTest.getDataTypeConverter(1).contains("StringToDateFormat"));
        final String[] parameters = newTest.getParameters(1);
        Assert.assertTrue(parameters.length == 3);
        Assert.assertTrue(parameters[0].equals("a"));
        Assert.assertTrue(parameters[1].equals("b"));
        Assert.assertTrue(parameters[2].equals("c"));
        Assert.assertTrue(newTest.getParameters(2).length == 0);
    }

    @Test
    public void testRecordSchema0Args() throws Exception {
        final ConnectorRecordSchema test = new ConnectorRecordSchema(1);
        final ConnectorRecordSchema newTest = new ConnectorRecordSchema();
        final ByteArrayOutputStream output = new ByteArrayOutputStream();
        final DataOutputStream out = new DataOutputStream(output);
        test.setFieldType(0, 1883);
        test.setDataTypeConverter(0, "StringToDateFormat");
        test.setParameters(0, new String[0]);
        test.write(out);
        final byte[] arrayList = output.toByteArray();
        final ByteArrayInputStream input = new ByteArrayInputStream(arrayList);
        final DataInputStream in = new DataInputStream(input);
        newTest.readFields(in);
        Assert.assertEquals(1883, newTest.getFieldType(0));
        Assert.assertTrue(newTest.getDataTypeConverter(0).contains("StringToDateFormat"));
        Assert.assertTrue(newTest.getParameters(0).length == 0);
    }

    @Test
    public void testLookupDataTypeAndValidate() throws Exception {
        Assert.assertEquals(3, ConnectorSchemaUtils.lookupDataTypeAndValidate("DECIMAL"));
        Assert.assertEquals(16, ConnectorSchemaUtils.lookupDataTypeAndValidate("BOOLEAN"));
        Assert.assertEquals(1885, ConnectorSchemaUtils.lookupDataTypeAndValidate("time with time zone"));
        Assert.assertEquals(1111, ConnectorSchemaUtils.lookupDataTypeAndValidate("INTERVAL"));
        Assert.assertEquals(1, ConnectorSchemaUtils.lookupDataTypeAndValidate("CHAR"));
        Assert.assertEquals(12, ConnectorSchemaUtils.lookupDataTypeAndValidate("MAP"));
        Assert.assertEquals(12, ConnectorSchemaUtils.lookupDataTypeAndValidate("ARRAY"));
        Assert.assertEquals(12, ConnectorSchemaUtils.lookupDataTypeAndValidate("STRUCT"));
        Assert.assertEquals(-6, ConnectorSchemaUtils.lookupDataTypeAndValidate("TINYINT"));
        Assert.assertEquals(-1, ConnectorSchemaUtils.lookupDataTypeAndValidate("LONGVARCHAR"));
        Assert.assertEquals(2005, ConnectorSchemaUtils.lookupDataTypeAndValidate("CLOB"));
        Assert.assertEquals(2004, ConnectorSchemaUtils.lookupDataTypeAndValidate("BLOB"));
        Assert.assertEquals(8, ConnectorSchemaUtils.lookupDataTypeAndValidate("REAL"));
        Assert.assertEquals(12, ConnectorSchemaUtils.lookupDataTypeAndValidate("ENUM"));
        Assert.assertEquals(12, ConnectorSchemaUtils.lookupDataTypeAndValidate("NULL"));
        Assert.assertEquals(1882, ConnectorSchemaUtils.lookupDataTypeAndValidate("UNION"));
        Assert.assertEquals(12, ConnectorSchemaUtils.lookupDataTypeAndValidate("FIXED"));
        Assert.assertEquals(1882, ConnectorSchemaUtils.lookupDataTypeAndValidate("OTHER"));
    }

    @Test
    public void testGetUdfParameters() {
        Assert.assertEquals("", ConnectorSchemaUtils.getUdfParameters("")[0]);
        Assert.assertEquals("two", ConnectorSchemaUtils.getUdfParameters("(one,two)")[1]);
    }

    @Test
    public void testGetWritableObjectType() throws Exception {
        Assert.assertEquals(4, ConnectorSchemaUtils.getWritableObjectType(new IntWritable()));
        Assert.assertEquals(-5, ConnectorSchemaUtils.getWritableObjectType(new LongWritable()));
        Assert.assertEquals(5, ConnectorSchemaUtils.getWritableObjectType(new ShortWritable()));
        Assert.assertEquals(-6, ConnectorSchemaUtils.getWritableObjectType(new ByteWritable()));
        Assert.assertEquals(6, ConnectorSchemaUtils.getWritableObjectType(new FloatWritable()));
        Assert.assertEquals(7, ConnectorSchemaUtils.getWritableObjectType(new DoubleWritable()));
        Assert.assertEquals(16, ConnectorSchemaUtils.getWritableObjectType(new BooleanWritable()));
        Assert.assertEquals(2, ConnectorSchemaUtils.getWritableObjectType(new ConnectorDataWritable.BigDecimalWritable()));
        Assert.assertEquals(2005, ConnectorSchemaUtils.getWritableObjectType(new ConnectorDataWritable.ClobWritable()));
        Assert.assertEquals(2004, ConnectorSchemaUtils.getWritableObjectType(new ConnectorDataWritable.BlobWritable()));
        Assert.assertEquals(12, ConnectorSchemaUtils.getWritableObjectType(new Text()));
        Assert.assertEquals(-2, ConnectorSchemaUtils.getWritableObjectType(new BytesWritable()));
        Assert.assertEquals(91, ConnectorSchemaUtils.getWritableObjectType(new ConnectorDataWritable.DateWritable()));
        Assert.assertEquals(92, ConnectorSchemaUtils.getWritableObjectType(new ConnectorDataWritable.TimeWritable()));
        Assert.assertEquals(93, ConnectorSchemaUtils.getWritableObjectType(new TimestampWritable()));
        Assert.assertEquals(2002, ConnectorSchemaUtils.getWritableObjectType(new ConnectorDataWritable.PeriodWritable()));
        Assert.assertEquals(1886, ConnectorSchemaUtils.getWritableObjectType(new ConnectorDataWritable.CalendarWritable()));
    }

    @Test
    public void testGetGenericObjectType() {
        Assert.assertEquals(5, ConnectorSchemaUtils.getGenericObjectType(4));
        Assert.assertEquals(-6, ConnectorSchemaUtils.getGenericObjectType(16));
        Assert.assertEquals(16, ConnectorSchemaUtils.getGenericObjectType(new Boolean(true)));
        Assert.assertEquals(-2, ConnectorSchemaUtils.getGenericObjectType(new byte[0]));
        final ResultStruct rs = new ResultStruct();
        rs.setSQLTypeName("PERIOD");
        Assert.assertEquals(2002, ConnectorSchemaUtils.getGenericObjectType(rs));
    }

    public static class TestUserConverter1Arg extends ConnectorDataTypeConverter {
        public TestUserConverter1Arg(final String arg1) {
        }

        @Override
        public final Object convert(final Object object) {
            return object;
        }
    }

    public static class TestUserConverter2Args extends ConnectorDataTypeConverter {
        public TestUserConverter2Args(final String arg1, final String arg2) {
        }

        @Override
        public final Object convert(final Object object) {
            return object;
        }
    }
}
