package com.teradata.connector.hive.converter;

import com.teradata.connector.common.converter.*;
import org.junit.Assert;
import org.junit.Test;

import java.util.ArrayList;
import java.util.HashMap;

/**
 * @author Administrator
 */
public class HiveDataTypeConverterTest {
    @Test
    public void testJsonStringToMap() {
        final HiveDataTypeConverter hiveconverter = new HiveDataTypeConverter.JsonStringToMap("test", "map<int,int>");
        final ConnectorDataTypeConverter converter = new ConnectorDataTypeConverter.ObjectToJsonString();
        final HashMap<Integer, Integer> inputMap = new HashMap<Integer, Integer>();
        inputMap.put(1, 2);
        final String input = (String) converter.convert(inputMap);
        final Object output = hiveconverter.convert(input);
        Assert.assertTrue(inputMap.equals(output));
    }

    @Test
    public void testJsonStringToArray() {
        final HiveDataTypeConverter hiveconverter = new HiveDataTypeConverter.JsonStringToStruct("test", "array<map<int,int>>");
        final ConnectorDataTypeConverter converter = new ConnectorDataTypeConverter.ObjectToJsonString();
        final ArrayList<Object> inputArray = new ArrayList<Object>();
        final HashMap<Integer, Integer> inputMap = new HashMap<Integer, Integer>();
        inputMap.put(1, 2);
        inputArray.add(inputMap);
        final String input = (String) converter.convert(inputArray);
        final Object output = hiveconverter.convert(input);
        Assert.assertTrue(inputArray.toString().equals(output.toString()));
    }

    @Test
    public void testJsonStringToStruct() {
        final HiveDataTypeConverter hiveconverter = new HiveDataTypeConverter.JsonStringToStruct("test", "struct<col1:int,col2:map<string,int>>");
        final ConnectorDataTypeConverter converter = new ConnectorDataTypeConverter.ObjectToJsonString();
        final HashMap<String, Integer> inputMap = new HashMap<String, Integer>();
        inputMap.put(String.valueOf(1), 2);
        final ArrayList<Object> input = new ArrayList<Object>(2);
        input.add(1);
        input.add(inputMap);
        final ArrayList<Object> output = (ArrayList<Object>) hiveconverter.convert(converter.convert(input));
        Assert.assertTrue(output.size() == 2);
        Assert.assertEquals(output.get(0), (Object) 1);
        Assert.assertEquals(output.get(1), (Object) inputMap);
    }
}
