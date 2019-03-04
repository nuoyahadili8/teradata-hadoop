package com.teradata.connector.common.converter;

import org.junit.*;

import java.math.*;

import com.teradata.connector.teradata.db.*;
import org.junit.experimental.categories.*;
import com.teradata.connector.test.group.*;

import java.sql.*;
import java.util.*;
import java.util.Date;

/**
 * @author Administrator
 */
public class ConnectorDataTypeConverterTest {

    final String dbs;
    final String databasename;
    final String user;
    final String password;

    public ConnectorDataTypeConverterTest() {
        this.dbs = System.getProperty("dbs");
        this.databasename = System.getProperty("databasename");
        this.user = System.getProperty("user");
        this.password = System.getProperty("password");
    }

    @Test
    public void testIntegerToInteger() {
        final ConnectorDataTypeConverter converter = new ConnectorDataTypeConverter.IntegerToInteger();
        final int input = 1;
        Assert.assertTrue((int) converter.convert(input) == 1);
    }

    @Test
    public void testIntegerToLong() {
        final ConnectorDataTypeConverter converter = new ConnectorDataTypeConverter.IntegerToLong();
        final int input = 1;
        final long output = 1L;
        Assert.assertTrue((long) converter.convert(input) == output);
    }

    @Test
    public void testIntegerToShort() {
        final ConnectorDataTypeConverter converter = new ConnectorDataTypeConverter.IntegerToShort();
        int input = 1;
        short output = 1;
        Assert.assertTrue((short) converter.convert(input) == output);
        input = 1234566;
        try {
            output = (short) converter.convert(input);
        } catch (NumberFormatException E) {
            output = 12345;
            Assert.assertFalse(input == output);
        }
    }

    @Test
    public void testIntegerToTinyInt() {
        final ConnectorDataTypeConverter converter = new ConnectorDataTypeConverter.IntegerToTinyInt();
        final int input = 1;
        final byte output = 1;
        Assert.assertTrue((byte) converter.convert(input) == output);
    }

    @Test
    public void testIntegerToFloat() {
        final ConnectorDataTypeConverter converter = new ConnectorDataTypeConverter.IntegerToFloat();
        final int input = 1;
        final float output = Float.parseFloat("1.0");
        Assert.assertTrue((float) converter.convert(input) == output);
    }

    @Test
    public void testIntegerToDouble() {
        final ConnectorDataTypeConverter converter = new ConnectorDataTypeConverter.IntegerToDouble();
        final int input = 1;
        final double output = 1.0;
        Assert.assertTrue((double) converter.convert(input) == output);
    }

    @Test
    public void testIntegerToBigDecimal() {
        final ConnectorDataTypeConverter converter = new ConnectorDataTypeConverter.IntegerToBigDecimalWithScale();
        ((ConnectorDataTypeConverter.IntegerToBigDecimalWithScale) converter).setScale(5);
        final int input = 1;
        final BigDecimal output = (BigDecimal) converter.convert(input);
        Assert.assertTrue(output.scale() == 5);
        Assert.assertTrue(output.equals(new BigDecimal(1).setScale(5)));
    }

    @Test
    public void testIntegerToBoolean() {
        final int trueValue = -1;
        final int falseValue = 0;
        final ConnectorDataTypeConverter converter = new ConnectorDataTypeConverter.IntegerToBoolean();
        final boolean trueBool = (boolean) converter.convert(trueValue);
        final boolean falseBool = (boolean) converter.convert(falseValue);
        Assert.assertTrue(trueBool);
        Assert.assertTrue(!falseBool);
    }

    @Test
    public void testIntegerToString() {
        final ConnectorDataTypeConverter converter = new ConnectorDataTypeConverter.IntegerToString();
        final Integer input = 1;
        final String output = "1";
        Assert.assertTrue(converter.convert(input).toString().equals(output));
    }

    @Test
    public void testLongToInteger() {
        final ConnectorDataTypeConverter converter = new ConnectorDataTypeConverter.LongToInteger();
        long input = 1L;
        int output = 1;
        Assert.assertTrue((int) converter.convert(input) == output);
        input = 123456789112L;
        try {
            output = (int) converter.convert(input);
        } catch (NumberFormatException E) {
            output = 123456789;
            Assert.assertFalse(input == output);
        }
    }

    @Test
    public void testLongToLong() {
        final ConnectorDataTypeConverter converter = new ConnectorDataTypeConverter.LongToLong();
        final long input = 1L;
        Assert.assertTrue((long) converter.convert(input) == 1L);
    }

    @Test
    public void testLongToShort() {
        final ConnectorDataTypeConverter converter = new ConnectorDataTypeConverter.LongToShort();
        long input = 1L;
        short output = 1;
        Assert.assertTrue((short) converter.convert(input) == output);
        input = 12345667L;
        try {
            output = (short) converter.convert(input);
        } catch (NumberFormatException E) {
            output = 12345;
            Assert.assertFalse(input == output);
        }
    }

    @Test
    public void testLongToTinyInt() {
        final ConnectorDataTypeConverter converter = new ConnectorDataTypeConverter.LongToTinyInt();
        final long input = 1L;
        final byte output = 1;
        Assert.assertTrue((byte) converter.convert(input) == output);
    }

    @Test
    public void testLongToFloat() {
        final ConnectorDataTypeConverter converter = new ConnectorDataTypeConverter.LongToFloat();
        final long input = 1L;
        final float output = Float.parseFloat("1.0");
        Assert.assertTrue((float) converter.convert(input) == output);
    }

    @Test
    public void testLongToDouble() {
        final ConnectorDataTypeConverter converter = new ConnectorDataTypeConverter.LongToDouble();
        final long input = 1L;
        final double output = 1.0;
        Assert.assertTrue((double) converter.convert(input) == output);
    }

    @Test
    public void testLongToBigDecimal() {
        final ConnectorDataTypeConverter converter = new ConnectorDataTypeConverter.LongToBigDecimalWithScale();
        ((ConnectorDataTypeConverter.LongToBigDecimalWithScale) converter).setScale(5);
        final long input = 1L;
        final BigDecimal output = (BigDecimal) converter.convert(input);
        Assert.assertTrue(output.scale() == 5);
        Assert.assertTrue(output.equals(new BigDecimal(1).setScale(5)));
    }

    @Test
    public void testLongToDate() {
        final ConnectorDataTypeConverter converter = new ConnectorDataTypeConverter.LongToDateTZ("GMT");
        final long input = 1L;
        final Date output = (Date) converter.convert(input);
        Assert.assertTrue(output.toString().equals(new Date(1L).toString()));
    }

    @Test
    public void testLongToTimestamp() {
    }

    @Test
    public void testLongToBoolean() {
        final ConnectorDataTypeConverter converter = new ConnectorDataTypeConverter.LongToBoolean();
        final long input_one = 1L;
        final long input_zero = 0L;
        final boolean output_true = true;
        final boolean output_false = false;
        Assert.assertTrue((boolean) converter.convert(input_one) == output_true);
        Assert.assertTrue((boolean) converter.convert(input_zero) == output_false);
    }

    @Test
    public void testLongToString() {
        final ConnectorDataTypeConverter converter = new ConnectorDataTypeConverter.LongToString();
        final long input = 1L;
        final String output = "1";
        Assert.assertTrue(converter.convert(input).toString().equals(output));
    }

    @Test
    public void testShortToInteger() {
        final ConnectorDataTypeConverter converter = new ConnectorDataTypeConverter.ShortToInteger();
        final short input = 1;
        final int output = 1;
        Assert.assertTrue((int) converter.convert(input) == output);
    }

    @Test
    public void testShortToLong() {
        final ConnectorDataTypeConverter converter = new ConnectorDataTypeConverter.ShortToLong();
        final short input = 1;
        final long output = 1L;
        Assert.assertTrue((long) converter.convert(input) == output);
    }

    @Test
    public void testShortToShort() {
        final ConnectorDataTypeConverter converter = new ConnectorDataTypeConverter.ShortToShort();
        final short input = 1;
        Assert.assertTrue((short) converter.convert(input) == 1);
    }

    @Test
    public void testShortToTinyInt() {
        final ConnectorDataTypeConverter converter = new ConnectorDataTypeConverter.ShortToTinyInt();
        final short input = 1;
        final Byte output = 1;
        Assert.assertTrue(converter.convert(input) == output);
    }

    @Test
    public void testShortToFloat() {
        final ConnectorDataTypeConverter converter = new ConnectorDataTypeConverter.ShortToFloat();
        final short input = 1;
        final float output = Float.parseFloat("1.0");
        Assert.assertTrue((float) converter.convert(input) == output);
    }

    @Test
    public void testShortToDouble() {
        final ConnectorDataTypeConverter converter = new ConnectorDataTypeConverter.ShortToDouble();
        final short input = 1;
        final double output = 1.0;
        Assert.assertTrue((double) converter.convert(input) == output);
    }

    @Test
    public void testShortToBigDecimal() {
        final ConnectorDataTypeConverter converter = new ConnectorDataTypeConverter.ShortToBigDecimalWithScale();
        ((ConnectorDataTypeConverter.ShortToBigDecimalWithScale) converter).setScale(5);
        final short input = 1;
        final BigDecimal output = (BigDecimal) converter.convert(input);
        Assert.assertTrue(output.scale() == 5);
        Assert.assertTrue(output.equals(new BigDecimal(1).setScale(5)));
    }

    @Test
    public void testShortToBoolean() {
        final short trueValue = 1;
        final short falseValue = 0;
        final ConnectorDataTypeConverter converter = new ConnectorDataTypeConverter.ShortToBoolean();
        final boolean trueBool = (boolean) converter.convert(trueValue);
        final boolean falseBool = (boolean) converter.convert(falseValue);
        Assert.assertTrue(trueBool);
        Assert.assertTrue(!falseBool);
    }

    @Test
    public void testShortToString() {
        final ConnectorDataTypeConverter converter = new ConnectorDataTypeConverter.ShortToString();
        final short input = 1;
        final String output = "1";
        Assert.assertTrue(converter.convert(input).toString().equals(output));
    }

    @Test
    public void testTinyIntToInteger() {
        final ConnectorDataTypeConverter converter = new ConnectorDataTypeConverter.TinyIntToInteger();
        final byte input = 1;
        final int output = 1;
        Assert.assertTrue((int) converter.convert(input) == output);
    }

    @Test
    public void testTinyIntToLong() {
        final ConnectorDataTypeConverter converter = new ConnectorDataTypeConverter.TinyIntToLong();
        final byte input = 1;
        final long output = 1L;
        Assert.assertTrue((long) converter.convert(input) == output);
    }

    @Test
    public void testTinyIntToShort() {
        final ConnectorDataTypeConverter converter = new ConnectorDataTypeConverter.TinyIntToShort();
        final byte input = 1;
        final short output = 1;
        Assert.assertTrue((short) converter.convert(input) == output);
    }

    @Test
    public void testTinyIntToTinyInt() {
        final ConnectorDataTypeConverter converter = new ConnectorDataTypeConverter.TinyIntToTinyInt();
        final byte input = 1;
        Assert.assertTrue((byte) converter.convert(input) == 1);
    }

    @Test
    public void testTinyIntToFloat() {
        final ConnectorDataTypeConverter converter = new ConnectorDataTypeConverter.TinyIntToFloat();
        final byte input = 1;
        final float output = Float.parseFloat("1.0");
        Assert.assertTrue((float) converter.convert(input) == output);
    }

    @Test
    public void testTinyIntToDouble() {
        final ConnectorDataTypeConverter converter = new ConnectorDataTypeConverter.TinyIntToDouble();
        final byte input = 1;
        final double output = 1.0;
        Assert.assertTrue((double) converter.convert(input) == output);
    }

    @Test
    public void testTinyIntToBigDecimal() {
        final ConnectorDataTypeConverter converter = new ConnectorDataTypeConverter.TinyIntToBigDecimalWithScale();
        ((ConnectorDataTypeConverter.TinyIntToBigDecimalWithScale) converter).setScale(5);
        final byte input = 1;
        final BigDecimal output = (BigDecimal) converter.convert(input);
        Assert.assertTrue(output.scale() == 5);
        Assert.assertTrue(output.equals(new BigDecimal(1).setScale(5)));
    }

    @Test
    public void testTinyIntToBoolean() {
        final byte trueValue = 1;
        final byte falseValue = 0;
        final ConnectorDataTypeConverter converter = new ConnectorDataTypeConverter.TinyIntToBoolean();
        final boolean trueBool = (boolean) converter.convert(trueValue);
        final boolean falseBool = (boolean) converter.convert(falseValue);
        Assert.assertTrue(trueBool);
        Assert.assertTrue(!falseBool);
    }

    @Test
    public void testTinyIntToString() {
        final ConnectorDataTypeConverter converter = new ConnectorDataTypeConverter.TinyIntToString();
        final byte input = 1;
        final String output = "1";
        Assert.assertTrue(converter.convert(input).toString().equals(output));
    }

    @Test
    public void testFloatToInteger() {
        final ConnectorDataTypeConverter converter = new ConnectorDataTypeConverter.FloatToInteger();
        final float input = Float.parseFloat("1.0");
        final int output = 1;
        Assert.assertTrue((int) converter.convert(input) == output);
    }

    @Test
    public void testFloatToLong() {
        final ConnectorDataTypeConverter converter = new ConnectorDataTypeConverter.FloatToLong();
        final float input = Float.parseFloat("1.0");
        final long output = 1L;
        Assert.assertTrue((long) converter.convert(input) == output);
    }

    @Test
    public void testFloatToShort() {
        final ConnectorDataTypeConverter converter = new ConnectorDataTypeConverter.FloatToShort();
        final float input = Float.parseFloat("1.0");
        final short output = 1;
        Assert.assertTrue((short) converter.convert(input) == output);
    }

    @Test
    public void testFloatToTinyInt() {
        final ConnectorDataTypeConverter converter = new ConnectorDataTypeConverter.FloatToTinyInt();
        final float input = Float.parseFloat("1.0");
        final byte output = 1;
        Assert.assertTrue((byte) converter.convert(input) == output);
    }

    @Test
    public void testFloatToFloat() {
        final ConnectorDataTypeConverter converter = new ConnectorDataTypeConverter.FloatToFloat();
        final float input = Float.parseFloat("1.0");
        Assert.assertTrue((float) converter.convert(input) == Float.parseFloat("1.0"));
    }

    @Test
    public void testFloatToDouble() {
        final ConnectorDataTypeConverter converter = new ConnectorDataTypeConverter.FloatToDouble();
        final float input = Float.parseFloat("1.0");
        final double output = 1.0;
        Assert.assertTrue((double) converter.convert(input) == output);
    }

    @Test
    public void testFloatToBigDecimal() {
        final ConnectorDataTypeConverter converter = new ConnectorDataTypeConverter.FloatToBigDecimalWithScale();
        ((ConnectorDataTypeConverter.FloatToBigDecimalWithScale) converter).setScale(5);
        final float input = Float.parseFloat("1.0");
        final BigDecimal output = (BigDecimal) converter.convert(input);
        Assert.assertTrue(output.scale() == 5);
        Assert.assertTrue(output.equals(new BigDecimal(1).setScale(5)));
    }

    @Test
    public void testFloatToBoolean() {
        final float trueValue = Float.parseFloat("1.0");
        final float falseValue = Float.parseFloat("0.0");
        final ConnectorDataTypeConverter converter = new ConnectorDataTypeConverter.FloatToBoolean();
        final boolean trueBool = (boolean) converter.convert(trueValue);
        final boolean falseBool = (boolean) converter.convert(falseValue);
        Assert.assertTrue(trueBool);
        Assert.assertTrue(!falseBool);
    }

    @Test
    public void testFloatToString() {
        final ConnectorDataTypeConverter converter = new ConnectorDataTypeConverter.FloatToString();
        final float input = Float.parseFloat("1.0");
        final String output = "1.0";
        Assert.assertTrue(converter.convert(input).toString().equals(output));
    }

    @Test
    public void testDoubleToInteger() {
        final ConnectorDataTypeConverter converter = new ConnectorDataTypeConverter.DoubleToInteger();
        final double input = 1.0;
        final int output = 1;
        Assert.assertTrue((int) converter.convert(input) == output);
    }

    @Test
    public void testDoubleToLong() {
        final ConnectorDataTypeConverter converter = new ConnectorDataTypeConverter.DoubleToLong();
        final double input = 1.0;
        final long output = 1L;
        Assert.assertTrue((long) converter.convert(input) == output);
    }

    @Test
    public void testDoubleToShort() {
        final ConnectorDataTypeConverter converter = new ConnectorDataTypeConverter.DoubleToShort();
        final double input = 1.0;
        final short output = 1;
        Assert.assertTrue((short) converter.convert(input) == output);
    }

    @Test
    public void testDoubleToTinyInt() {
        final ConnectorDataTypeConverter converter = new ConnectorDataTypeConverter.DoubleToTinyInt();
        final double input = 1.0;
        final byte output = 1;
        Assert.assertTrue((byte) converter.convert(input) == output);
    }

    @Test
    public void testDoubleToFloat() {
        final ConnectorDataTypeConverter converter = new ConnectorDataTypeConverter.DoubleToFloat();
        final double input = 1.0;
        final float output = Float.parseFloat("1.0");
        Assert.assertTrue((float) converter.convert(input) == output);
    }

    @Test
    public void testDoubleToDouble() {
        final ConnectorDataTypeConverter converter = new ConnectorDataTypeConverter.DoubleToDouble();
        final double input = 1.0;
        Assert.assertTrue((double) converter.convert(input) == 1.0);
    }

    @Test
    public void testDoubleToBigDecimal() {
        final ConnectorDataTypeConverter converter = new ConnectorDataTypeConverter.DoubleToBigDecimalWithScale();
        ((ConnectorDataTypeConverter.DoubleToBigDecimalWithScale) converter).setScale(5);
        final double input = 1.0;
        final BigDecimal output = (BigDecimal) converter.convert(input);
        Assert.assertTrue(output.scale() == 5);
        Assert.assertTrue(output.equals(new BigDecimal(1).setScale(5)));
    }

    @Test
    public void testDoubleToBoolean() {
        final double trueValue = 1.0;
        final double falseValue = 0.0;
        final ConnectorDataTypeConverter converter = new ConnectorDataTypeConverter.DoubleToBoolean();
        final boolean trueBool = (boolean) converter.convert(trueValue);
        final boolean falseBool = (boolean) converter.convert(falseValue);
        Assert.assertTrue(trueBool);
        Assert.assertTrue(!falseBool);
    }

    @Test
    public void testDoubleToString() {
        final ConnectorDataTypeConverter converter = new ConnectorDataTypeConverter.DoubleToString();
        final double input = 1.0;
        final String output = "1.0";
        Assert.assertTrue(converter.convert(input).toString().equals(output));
    }

    @Test
    public void testBigDecimalToInteger() {
        final ConnectorDataTypeConverter converter = new ConnectorDataTypeConverter.BigDecimalToInteger();
        final BigDecimal input = new BigDecimal(1);
        input.setScale(5);
        final int output = 1;
        Assert.assertTrue((int) converter.convert(input) == output);
    }

    @Test
    public void testBigDecimalToLong() {
        final ConnectorDataTypeConverter converter = new ConnectorDataTypeConverter.BigDecimalToLong();
        final BigDecimal input = new BigDecimal(1);
        input.setScale(5);
        final long output = 1L;
        Assert.assertTrue((long) converter.convert(input) == output);
    }

    @Test
    public void testBigDecimalToShort() {
        final ConnectorDataTypeConverter converter = new ConnectorDataTypeConverter.BigDecimalToShort();
        final BigDecimal input = new BigDecimal(1);
        input.setScale(5);
        final short output = 1;
        Assert.assertTrue((short) converter.convert(input) == output);
    }

    @Test
    public void testBigDecimalToTinyInt() {
        final ConnectorDataTypeConverter converter = new ConnectorDataTypeConverter.BigDecimalToTinyInt();
        final BigDecimal input = new BigDecimal(1);
        input.setScale(5);
        final byte output = 1;
        Assert.assertTrue((byte) converter.convert(input) == output);
    }

    @Test
    public void testBigDecimalToFloat() {
        final ConnectorDataTypeConverter converter = new ConnectorDataTypeConverter.BigDecimalToFloat();
        final BigDecimal input = new BigDecimal(1);
        input.setScale(5);
        final float output = Float.parseFloat("1.0");
        Assert.assertTrue((float) converter.convert(input) == output);
    }

    @Test
    public void testBigDecimalToDouble() {
        final ConnectorDataTypeConverter converter = new ConnectorDataTypeConverter.BigDecimalToDouble();
        final BigDecimal input = new BigDecimal(1);
        input.setScale(5);
        final double output = 1.0;
        Assert.assertTrue((double) converter.convert(input) == output);
    }

    @Test
    public void testBigDecimalToBigDecimal() {
        final ConnectorDataTypeConverter converter = new ConnectorDataTypeConverter.BigDecimalToBigDecimalWithScale();
        final BigDecimal input = new BigDecimal(1);
        input.setScale(5);
        Assert.assertTrue(((BigDecimal) converter.convert(input)).setScale(5).equals(new BigDecimal(1).setScale(5)));
    }

    @Test
    public void testBigDecimalToBoolean() {
        final ConnectorDataTypeConverter converter = new ConnectorDataTypeConverter.BigDecimalToBoolean();
        final boolean trueBool = (boolean) converter.convert(new BigDecimal(1).setScale(5));
        final boolean falseBool = (boolean) converter.convert(new BigDecimal(0).setScale(5));
        Assert.assertTrue(trueBool);
        Assert.assertTrue(!falseBool);
    }

    @Test
    public void testBigDecimalToString() {
        final ConnectorDataTypeConverter converter = new ConnectorDataTypeConverter.BigDecimalToString();
        final BigDecimal input = new BigDecimal(1);
        input.setScale(5);
        final String output = "1";
        Assert.assertTrue(((String) converter.convert(input)).equals(output));
    }

    @Test
    public void testBooleanToInteger() {
        final ConnectorDataTypeConverter converter = new ConnectorDataTypeConverter.BooleanToInteger();
        final int defaultvalue = 1;
        final int falsedefaultvalue = 0;
        ((ConnectorDataTypeConverter.BooleanToInteger) converter).setDefaultValue(defaultvalue);
        ((ConnectorDataTypeConverter.BooleanToInteger) converter).setFalseDefaultValue(falsedefaultvalue);
        final boolean truevalue = true;
        final boolean falsevalue = false;
        Assert.assertTrue((int) converter.convert(truevalue) == 1);
        Assert.assertTrue((int) converter.convert(falsevalue) == 0);
    }

    @Test
    public void testBooleanToLong() {
        final ConnectorDataTypeConverter converter = new ConnectorDataTypeConverter.BooleanToLong();
        final long defaultvalue = 1L;
        final long falsedefaultvalue = 0L;
        ((ConnectorDataTypeConverter.BooleanToLong) converter).setDefaultValue(defaultvalue);
        ((ConnectorDataTypeConverter.BooleanToLong) converter).setFalseDefaultValue(falsedefaultvalue);
        final boolean truevalue = true;
        final boolean falsevalue = false;
        Assert.assertTrue((long) converter.convert(truevalue) == defaultvalue);
        Assert.assertTrue((long) converter.convert(falsevalue) == falsedefaultvalue);
    }

    @Test
    public void testBooleanToShort() {
        final ConnectorDataTypeConverter converter = new ConnectorDataTypeConverter.BooleanToShort();
        final short defaultvalue = 1;
        final short falsedefaultvalue = 0;
        ((ConnectorDataTypeConverter.BooleanToShort) converter).setDefaultValue(defaultvalue);
        ((ConnectorDataTypeConverter.BooleanToShort) converter).setFalseDefaultValue(falsedefaultvalue);
        final boolean truevalue = true;
        final boolean falsevalue = false;
        Assert.assertTrue((short) converter.convert(truevalue) == defaultvalue);
        Assert.assertTrue((short) converter.convert(falsevalue) == falsedefaultvalue);
    }

    @Test
    public void testBooleanToTinyInt() {
        final ConnectorDataTypeConverter converter = new ConnectorDataTypeConverter.BooleanToTinyInt();
        final byte defaultvalue = 1;
        final byte falsedefaultvalue = 0;
        ((ConnectorDataTypeConverter.BooleanToTinyInt) converter).setDefaultValue(defaultvalue);
        ((ConnectorDataTypeConverter.BooleanToTinyInt) converter).setFalseDefaultValue(falsedefaultvalue);
        final boolean truevalue = true;
        final boolean falsevalue = false;
        Assert.assertTrue((byte) converter.convert(truevalue) == defaultvalue);
        Assert.assertTrue((byte) converter.convert(falsevalue) == falsedefaultvalue);
    }

    @Test
    public void testBooleanToFloat() {
        final ConnectorDataTypeConverter converter = new ConnectorDataTypeConverter.BooleanToFloat();
        final float defaultvalue = Float.parseFloat("1.0");
        final float falsedefaultvalue = Float.parseFloat("0.0");
        ((ConnectorDataTypeConverter.BooleanToFloat) converter).setDefaultValue(defaultvalue);
        ((ConnectorDataTypeConverter.BooleanToFloat) converter).setFalseDefaultValue(falsedefaultvalue);
        final boolean truevalue = true;
        final boolean falsevalue = false;
        Assert.assertTrue((float) converter.convert(truevalue) == defaultvalue);
        Assert.assertTrue((float) converter.convert(falsevalue) == falsedefaultvalue);
    }

    @Test
    public void testBooleanToDouble() {
        final ConnectorDataTypeConverter converter = new ConnectorDataTypeConverter.BooleanToDouble();
        final double defaultvalue = 1.0;
        final double falsedefaultvalue = 0.0;
        ((ConnectorDataTypeConverter.BooleanToDouble) converter).setDefaultValue(defaultvalue);
        ((ConnectorDataTypeConverter.BooleanToDouble) converter).setFalseDefaultValue(falsedefaultvalue);
        final boolean truevalue = true;
        final boolean falsevalue = false;
        Assert.assertTrue((double) converter.convert(truevalue) == defaultvalue);
        Assert.assertTrue((double) converter.convert(falsevalue) == falsedefaultvalue);
    }

    @Test
    public void testBooleanToBigDecimal() {
        final ConnectorDataTypeConverter converter = new ConnectorDataTypeConverter.BooleanToBigDecimalWithScale();
        final BigDecimal defaultvalue = new BigDecimal(1);
        defaultvalue.setScale(5);
        final BigDecimal falsedefaultvalue = new BigDecimal(0);
        falsedefaultvalue.setScale(5);
        ((ConnectorDataTypeConverter.BooleanToBigDecimalWithScale) converter).setDefaultValue(defaultvalue);
        ((ConnectorDataTypeConverter.BooleanToBigDecimalWithScale) converter).setFalseDefaultValue(falsedefaultvalue);
        ((ConnectorDataTypeConverter.BooleanToBigDecimalWithScale) converter).setScale(5);
        final boolean truevalue = true;
        final boolean falsevalue = false;
        Assert.assertTrue(((BigDecimal) converter.convert(truevalue)).equals(new BigDecimal(1).setScale(5)));
        Assert.assertTrue(((BigDecimal) converter.convert(falsevalue)).equals(new BigDecimal(0).setScale(5)));
    }

    @Test
    public void testBooleanToBoolean() {
        final ConnectorDataTypeConverter converter = new ConnectorDataTypeConverter.BooleanToBoolean();
        final boolean truevalue = true;
        final boolean falsevalue = false;
        Assert.assertTrue((boolean) converter.convert(truevalue));
        Assert.assertTrue(!(boolean) converter.convert(falsevalue));
    }

    @Test
    public void testBooleanToString() {
        final ConnectorDataTypeConverter converter = new ConnectorDataTypeConverter.BooleanToString();
        final boolean truevalue = true;
        final boolean falsevalue = false;
        Assert.assertTrue(converter.convert(truevalue).toString().equals("true"));
        Assert.assertTrue(converter.convert(falsevalue).toString().equals("false"));
    }

    @Test
    public void testStringToInteger() {
        final ConnectorDataTypeConverter converter = new ConnectorDataTypeConverter.StringToInteger();
        final String input = "1";
        final int output = 1;
        Assert.assertTrue((int) converter.convert(input) == output);
    }

    @Test
    public void testStringToLong() {
        final ConnectorDataTypeConverter converter = new ConnectorDataTypeConverter.StringToLong();
        final String input = "1";
        final long output = 1L;
        Assert.assertTrue((long) converter.convert(input) == output);
    }

    @Test
    public void testStringToShort() {
        final ConnectorDataTypeConverter converter = new ConnectorDataTypeConverter.StringToShort();
        final String input = "1";
        final short output = 1;
        Assert.assertTrue((short) converter.convert(input) == output);
    }

    @Test
    public void testStringToTinyInt() {
        final ConnectorDataTypeConverter converter = new ConnectorDataTypeConverter.StringToTinyInt();
        final String input = "1";
        final byte output = 1;
        Assert.assertTrue((byte) converter.convert(input) == output);
    }

    @Test
    public void testStringToFloat() {
        final ConnectorDataTypeConverter converter = new ConnectorDataTypeConverter.StringToFloat();
        final String input = "1.0";
        final float output = Float.parseFloat("1.0");
        Assert.assertTrue((float) converter.convert(input) == output);
    }

    @Test
    public void testStringToDouble() {
        final ConnectorDataTypeConverter converter = new ConnectorDataTypeConverter.StringToDouble();
        final String input = "1.0";
        final double output = 1.0;
        Assert.assertTrue((double) converter.convert(input) == output);
    }

    @Test
    public void testStringToBigDecimal() {
        final ConnectorDataTypeConverter converter = new ConnectorDataTypeConverter.StringToBigDecimalWithScale();
        ((ConnectorDataTypeConverter.StringToBigDecimalWithScale) converter).setScale(5);
        final String input = "1";
        final BigDecimal output = (BigDecimal) converter.convert(input);
        Assert.assertTrue(((BigDecimal) converter.convert(input)).scale() == 5);
        Assert.assertTrue(output.equals(new BigDecimal(1).setScale(5)));
    }

    @Test
    public void testStringToBigDecimalUnbounded() {
        final ConnectorDataTypeConverter converter = new ConnectorDataTypeConverter.StringToBigDecimalUnbounded();
        ((ConnectorDataTypeConverter.StringToBigDecimalUnbounded) converter).setScale(5);
        String input = "1";
        BigDecimal output = (BigDecimal) converter.convert(input);
        Assert.assertTrue(((BigDecimal) converter.convert(input)).scale() == 5);
        Assert.assertTrue(output.equals(new BigDecimal(1).setScale(5)));
        ((ConnectorDataTypeConverter.StringToBigDecimalUnbounded) converter).setScale(0);
        ((ConnectorDataTypeConverter.StringToBigDecimalUnbounded) converter).setPrecision(40);
        ((ConnectorDataTypeConverter.StringToBigDecimalUnbounded) converter).setLength(47);
        input = "15553.6789236772";
        output = (BigDecimal) converter.convert(input);
        Assert.assertTrue(((BigDecimal) converter.convert(input)).scale() == 10);
        Assert.assertTrue(output.equals(new BigDecimal("15553.6789236772").setScale(10)));
    }

    @Test
    public void testStringToBoolean() {
        final ConnectorDataTypeConverter converter = new ConnectorDataTypeConverter.StringToBoolean();
        final String inputTrue = "true";
        final String inputFalse = "false";
        Assert.assertTrue((boolean) converter.convert(inputTrue));
        Assert.assertTrue(!(boolean) converter.convert(inputFalse));
    }

    @Test
    public void testStringToString() {
        final String input = "12345";
        final ConnectorDataTypeConverter.StringToString converter = new ConnectorDataTypeConverter.StringToString();
        converter.setLength(4);
        Assert.assertTrue(converter.convert(input).toString().equals("1234"));
        boolean exceptionThrown = false;
        try {
            converter.setTruncate(false);
            converter.convert(input).toString();
        } catch (RuntimeException e) {
            exceptionThrown = true;
            Assert.assertTrue(e.getMessage().equals("String would be truncated"));
        }
        Assert.assertTrue(exceptionThrown);
        final ConnectorDataTypeConverter.StringToString converter2 = new ConnectorDataTypeConverter.StringToString(true, 4);
        Assert.assertTrue(converter2.convert(input).toString().equals("1234"));
        converter2.setLength(0);
        Assert.assertTrue(converter2.convert(input).toString().equals("12345"));
        converter2.setLength(-1);
        Assert.assertTrue(converter2.convert(input).toString().equals("12345"));
    }

    @Test
    public void testStringToDate() {
        final ConnectorDataTypeConverter converter = new ConnectorDataTypeConverter.StringToDate();
        final String input = "2014-01-22";
        final Date output = (Date) converter.convert(input);
        Assert.assertTrue(output.toString().equals(new Date(114, 0, 22).toString()));
    }

    @Test
    public void testStringToDateWithFormat() {
        final ConnectorDataTypeConverter converter = new ConnectorDataTypeConverter.StringToDateWithFormat("yyyy/MM/dd");
        final String input = "2014/01/22";
        final Date output = (Date) converter.convert(input);
        Assert.assertTrue(output.toString().equals(new Date(114, 0, 22).toString()));
    }

    @Test
    public void testStringToTime() {
        final ConnectorDataTypeConverter converter = new ConnectorDataTypeConverter.StringToTime();
        final String input = "14:39:40";
        final Time output = (Time) converter.convert(input);
        Assert.assertTrue(output.toString().equals(new Time(14, 39, 40).toString()));
    }

    @Test
    public void testStringToTimeWithFormat() {
        final ConnectorDataTypeConverter converter = new ConnectorDataTypeConverter.StringToTimeWithFormat("HH//mm:ss");
        final String input = "14//39:40";
        final Time output = (Time) converter.convert(input);
        Assert.assertTrue(output.toString().equals(new Time(14, 39, 40).toString()));
    }

    @Test
    public void testStringToTimestamp() {
        final ConnectorDataTypeConverter converter = new ConnectorDataTypeConverter.StringToTimestamp();
        final String input = "2014-01-22 14:39:40.000003";
        final Timestamp output = (Timestamp) converter.convert(input);
        Assert.assertTrue(output.toString().equals(new Timestamp(114, 0, 22, 14, 39, 40, 3000).toString()));
    }

    @Test
    public void testStringToTimestampWithFormat() {
        final ConnectorDataTypeConverter converter = new ConnectorDataTypeConverter.StringToTimestampWithFormat("yyyy/MM/dd HH//mm:ss.SSS");
        final String input = "2014/01/22 14//39:40.123";
        final Timestamp output = (Timestamp) converter.convert(input);
        final Timestamp compareTimestamp = new Timestamp(114, 0, 22, 14, 39, 40, 123000000);
        Assert.assertTrue(compareTimestamp.equals(output));
    }

    @Test
    public void testStringToBinary() {
        final ConnectorDataTypeConverter converter = new ConnectorDataTypeConverter.StringToBinary();
        final String input = "1";
        byte[] output = new byte[input.length()];
        output = (byte[]) converter.convert(input);
        Assert.assertTrue(output[0] == 1);
    }

    @Test
    public void testStringToPeriod() throws SQLException, ClassNotFoundException {
        final ConnectorDataTypeConverter converter = new ConnectorDataTypeConverter.StringToPeriod();
        final String input = "(2013-12-18, 2013-12-19)";
        final Object output = converter.convert(input);
        Assert.assertTrue(output.toString().equals(input));
    }

    @Test
    public void testStringToInterval() {
        final ConnectorDataTypeConverter converter = new ConnectorDataTypeConverter.StringToInterval();
        final String input = "1";
        final Object output = converter.convert(input);
        Assert.assertTrue(output.toString().equals(input));
    }

    @Category({SlowTests.class})
    @Test
    public void testPeriodToString() throws ClassNotFoundException, SQLException {
        final ConnectorDataTypeConverter converter = new ConnectorDataTypeConverter.PeriodToString();
        final TeradataConnection conn = new TeradataConnection("com.teradata.jdbc.TeraDriver", "jdbc:teradata://" + this.dbs + "/database=" + this.databasename, this.user, this.password, false);
        conn.connect();
        final PreparedStatement query = conn.getConnection().prepareStatement("select f16 from TD_ALL_DATATYPE_small");
        final ResultSet result = query.executeQuery();
        if (result.next()) {
            final Struct input = (Struct) result.getObject(1);
            final String output = (String) converter.convert(input);
            Assert.assertTrue(output.equals("(2013-12-18, 2013-12-19)"));
        }
        conn.close();
    }

    @Category({SlowTests.class})
    @Test
    public void testIntervalToString() throws SQLException, ClassNotFoundException {
        final ConnectorDataTypeConverter converter = new ConnectorDataTypeConverter.IntervalToString();
        final TeradataConnection conn = new TeradataConnection("com.teradata.jdbc.TeraDriver", "jdbc:teradata://" + this.dbs + "/database=" + this.databasename, this.user, this.password, false);
        conn.connect();
        final PreparedStatement query = conn.getConnection().prepareStatement("select f19 from TD_ALL_DATATYPE_small");
        final ResultSet result = query.executeQuery();
        if (result.next()) {
            final Object input = result.getObject(1);
            final String output = (String) converter.convert(input);
            Assert.assertTrue(output.equals("    1"));
        }
        conn.close();
    }

    @Category({SlowTests.class})
    @Test
    public void testClobToString() throws ClassNotFoundException, SQLException {
        final ConnectorDataTypeConverter converter = new ConnectorDataTypeConverter.ClobToString();
        final TeradataConnection conn = new TeradataConnection("com.teradata.jdbc.TeraDriver", "jdbc:teradata://" + this.dbs + "/database=" + this.databasename, this.user, this.password, false);
        conn.connect();
        final PreparedStatement query = conn.getConnection().prepareStatement("select f3 from TD_ALL_DATATYPE_other");
        final ResultSet result = query.executeQuery();
        if (result.next()) {
            final Clob input = result.getClob("f3");
            final String output = (String) converter.convert(input);
            Assert.assertTrue(output.equals("c:/noven/Clob.txt"));
        }
        conn.close();
    }

    @Category({SlowTests.class})
    @Test
    public void testBlobToBinary() throws ClassNotFoundException, SQLException {
        final ConnectorDataTypeConverter converter = new ConnectorDataTypeConverter.BlobToBinary();
        final TeradataConnection conn = new TeradataConnection("com.teradata.jdbc.TeraDriver", "jdbc:teradata://" + this.dbs + "/database=" + this.databasename, this.user, this.password, false);
        conn.connect();
        final PreparedStatement query = conn.getConnection().prepareStatement("select f2 from TD_ALL_DATATYPE_other");
        final ResultSet result = query.executeQuery();
        if (result.next()) {
            final Blob input = result.getBlob("f2");
            final byte[] output = (byte[]) converter.convert(input);
            Assert.assertTrue(output[0] == 0);
            Assert.assertTrue(output[1] == 17);
            Assert.assertTrue(output[2] == 16);
        }
        conn.close();
    }

    @Category({SlowTests.class})
    @Test
    public void testBlobToString() throws ClassNotFoundException, SQLException {
        final ConnectorDataTypeConverter converter = new ConnectorDataTypeConverter.BlobToString();
        final TeradataConnection conn = new TeradataConnection("com.teradata.jdbc.TeraDriver", "jdbc:teradata://" + this.dbs + "/database=" + this.databasename, this.user, this.password, false);
        conn.connect();
        final PreparedStatement query = conn.getConnection().prepareStatement("select f2 from TD_ALL_DATATYPE_other");
        final ResultSet result = query.executeQuery();
        if (result.next()) {
            final Blob input = result.getBlob("f2");
            final String output = (String) converter.convert(input);
            Assert.assertTrue(output.equals("001110"));
        }
        conn.close();
    }

    @Test
    public void testDateToDate() {
        final ConnectorDataTypeConverter converter = new ConnectorDataTypeConverter.DateToDate();
        final Date input = new Date(114, 0, 22);
        final Date output = (Date) converter.convert(input);
        Assert.assertTrue(output.toString().equals(new Date(114, 0, 22).toString()));
    }

    @Test
    public void testDateToLong() {
        final ConnectorDataTypeConverter converter = new ConnectorDataTypeConverter.DateToLong();
        final Date input = new Date(1L);
        final long output = 1L;
        Assert.assertTrue((long) converter.convert(input) == output);
    }

    @Test
    public void testDateToTimestamp() {
        final ConnectorDataTypeConverter converter = new ConnectorDataTypeConverter.DateToTimestamp();
        final Date input = new Date(114, 0, 22);
        final Timestamp output = (Timestamp) converter.convert(input);
        Assert.assertTrue(output.toString().equals(new Timestamp(114, 0, 22, 0, 0, 0, 0).toString()));
    }

    @Test
    public void testDateToString() {
        final ConnectorDataTypeConverter converter = new ConnectorDataTypeConverter.DateToString();
        final Date input = new Date(114, 0, 22);
        final String output = (String) converter.convert(input);
        Assert.assertTrue(input.toString().equals(output));
    }

    @Test
    public void testTimeToTime() {
        final ConnectorDataTypeConverter converter = new ConnectorDataTypeConverter.TimeToTime();
        final Time input = new Time(14, 39, 40);
        final Time output = (Time) converter.convert(input);
        Assert.assertTrue(output.toString().equals(input.toString()));
    }

    @Test
    public void testTimeToTimeWithTimeZone() {
        final ConnectorDataTypeConverter converter = new ConnectorDataTypeConverter.TimeToTimeWithTimeZone("0");
        final Time input = new Time(14, 39, 40);
        final Time output = (Time) converter.convert(input);
        long diff = TimeZone.getDefault().getRawOffset();
        if (TimeZone.getDefault().inDaylightTime(new java.util.Date(1000L * output.getTime())) || TimeZone.getDefault().inDaylightTime(new java.util.Date(1000L * input.getTime()))) {
            diff += 3600000L;
        }
        Assert.assertTrue(output.getTime() - input.getTime() == diff);
    }

    @Test
    public void testTimeToString() {
        final ConnectorDataTypeConverter converter = new ConnectorDataTypeConverter.TimeToString();
        final Time input = new Time(14, 39, 40);
        final String output = (String) converter.convert(input);
        Assert.assertTrue(output.equals("14:39:40"));
    }

    @Test
    public void testTimeToStringWithFormat() {
        final ConnectorDataTypeConverter converter = new ConnectorDataTypeConverter.TimeToStringWithFormat("HH/mm:ss");
        final Time input = new Time(14, 39, 40);
        final String output = (String) converter.convert(input);
        Assert.assertTrue(output.equals("14/39:40"));
    }

    @Test
    public void testTimestampToLong() {
        final ConnectorDataTypeConverter converter = new ConnectorDataTypeConverter.TimestampToLong();
        final Timestamp input = new Timestamp(114, 0, 22, 14, 39, 40, 3000);
        final Long output = (Long) converter.convert(input);
        Assert.assertTrue(input.getTime() == output);
    }

    @Test
    public void testTimeStampToDate() {
        final ConnectorDataTypeConverter converter = new ConnectorDataTypeConverter.TimestampToDate();
        final Timestamp input = new Timestamp(114, 0, 22, 14, 39, 40, 3000);
        final Date output = (Date) converter.convert(input);
        Assert.assertTrue(output.toString().equals(new Date(114, 0, 22).toString()));
    }

    @Test
    public void testTimeStampToTime() {
        final ConnectorDataTypeConverter converter = new ConnectorDataTypeConverter.TimestampToTime();
        final Timestamp input = new Timestamp(114, 0, 22, 14, 7, 0, 3000);
        final Time output = (Time) converter.convert(input);
        Assert.assertTrue(output.toString().equals(new Time(14, 7, 0).toString()));
    }

    @Test
    public void testTimestampToTimestamp() {
        final ConnectorDataTypeConverter converter = new ConnectorDataTypeConverter.TimestampToTimestamp();
        final Timestamp input = new Timestamp(114, 0, 22, 14, 39, 40, 3000);
        final Timestamp output = (Timestamp) converter.convert(input);
        Assert.assertTrue(input.equals(output));
    }

    @Test
    public void testTimestampToTimestampWithTimeZone() {
        final ConnectorDataTypeConverter converter = new ConnectorDataTypeConverter.TimestampToTimestampTZ("0");
        final Timestamp input = new Timestamp(114, 0, 22, 14, 39, 40, 3000);
        final Timestamp output = (Timestamp) converter.convert(input);
        long diff = TimeZone.getDefault().getRawOffset();
        if (TimeZone.getDefault().inDaylightTime(new java.util.Date(1000L * output.getTime())) || TimeZone.getDefault().inDaylightTime(new java.util.Date(1000L * input.getTime()))) {
            diff += 3600000L;
        }
        Assert.assertTrue(output.getTime() - input.getTime() == diff);
    }

    @Test
    public void testTimeStampToString() {
        final ConnectorDataTypeConverter converter = new ConnectorDataTypeConverter.TimestampToString();
        final Timestamp input = new Timestamp(114, 0, 22, 14, 39, 40, 3000);
        final String output = (String) converter.convert(input);
        Assert.assertTrue(output.equals("2014-01-22 14:39:40.000003"));
    }

    @Test
    public void testTimestampToStringWithFormat() {
        final ConnectorDataTypeConverter converter = new ConnectorDataTypeConverter.TimestampToStringWithFormat("yyyy/MM/dd HH:mm:ss.SSS");
        final Timestamp input = new Timestamp(114, 0, 22, 14, 39, 40, 3000000);
        final String output = (String) converter.convert(input);
        Assert.assertTrue(output.equals("2014/01/22 14:39:40.003"));
    }

    @Test
    public void testBinaryToBinary() {
        final ConnectorDataTypeConverter converter = new ConnectorDataTypeConverter.BinaryToBinary();
        final byte[] input = {1};
        byte[] output = {0};
        output = (byte[]) converter.convert(input);
        Assert.assertTrue(output[0] == input[0]);
    }

    @Test
    public void testBinaryToString() {
        final ConnectorDataTypeConverter converter = new ConnectorDataTypeConverter.BinaryToString();
        final byte[] input = {1};
        final String output = (String) converter.convert(input);
        Assert.assertTrue(output.equals("01"));
    }

    @Test
    public void testObjectToString() {
        final ConnectorDataTypeConverter converter = new ConnectorDataTypeConverter.ObjectToString();
        Object input = new Object();
        input = 1;
        final String output = (String) converter.convert(input);
        Assert.assertTrue(output.equals(input.toString()));
    }

    @Test
    public void testObjectToJsonString() {
        final ConnectorDataTypeConverter converter = new ConnectorDataTypeConverter.ObjectToJsonString();
        Object input = new Object();
        input = 1;
        final String output = (String) converter.convert(input);
        Assert.assertTrue(output.equals(input.toString()));
    }
}
