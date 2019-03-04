package com.teradata.connector.common.utils;

import java.io.*;

import org.junit.Assert;
import org.junit.Test;

/**
 * @author Administrator
 */
public class ConnectorCsvParserTest {
    @Test
    public void testConvert1() {
        final String[] test = {"1\\23", "456"};
        final String text = "\"1\\\\23\",\"456\"";
        final ConnectorCsvParser csv = new ConnectorCsvParser();
        try {
            Assert.assertEquals(csv.parse(text)[0], test[0]);
            Assert.assertEquals(csv.parse(text)[1], test[1]);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    @Test
    public void testConvert2() {
        final String[] test = {"1.23", "456"};
        final String text = "'1..23';'456'";
        final ConnectorCsvParser csv = new ConnectorCsvParser(";", "'", ".");
        try {
            Assert.assertEquals(csv.parse(text)[0], test[0]);
            Assert.assertEquals(csv.parse(text)[1], test[1]);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    @Test
    public void testConvert3() {
        final String[] test = {"1.23", "456"};
        final String text = "\"1..23\";\"456\"";
        final ConnectorCsvParser csv = new ConnectorCsvParser(';', '\"', '.');
        try {
            Assert.assertEquals(csv.parse(text)[0], test[0]);
            Assert.assertEquals(csv.parse(text)[1], test[1]);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}
