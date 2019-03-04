package com.teradata.connector.common.utils;

import java.io.*;

import org.junit.Assert;
import org.junit.Test;

/**
 * @author Administrator
 */
public class ConnectorCsvPrinterTest {
    @Test
    public void testConvert1() {
        final String[] test = {"1\\23", "456"};
        final ConnectorCsvPrinter csv = new ConnectorCsvPrinter();
        try {
            Assert.assertEquals(csv.print(test), "\"1\\\\23\",\"456\"");
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    @Test
    public void testConvert2() {
        final String[] test = {"1.23", "456"};
        final ConnectorCsvPrinter csv = new ConnectorCsvPrinter(";", "'", ".");
        try {
            Assert.assertEquals(csv.print(test), "'1..23';'456'");
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    @Test
    public void testConvert3() {
        final String[] test = {"1.23", "456"};
        final ConnectorCsvPrinter csv = new ConnectorCsvPrinter(';', '\"', '.');
        try {
            Assert.assertEquals(csv.print(test), "\"1..23\";\"456\"");
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}
