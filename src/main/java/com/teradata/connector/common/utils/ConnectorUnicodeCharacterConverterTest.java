package com.teradata.connector.common.utils;

import org.junit.Assert;
import org.junit.Test;

/**
 * @author Administrator
 */
public class ConnectorUnicodeCharacterConverterTest {
    @Test
    public void testConvert() {
        final String input = "\u2267test";
        final String encoded = "\\u2267test";
        final String toUnicode = ConnectorUnicodeCharacterConverter.toEncodedUnicode(input);
        Assert.assertEquals(encoded, toUnicode);
        try {
            final String fromUnicode = ConnectorUnicodeCharacterConverter.fromEncodedUnicode(toUnicode);
            Assert.assertEquals(input, fromUnicode);
        } catch (Exception e) {
            Assert.fail("Expected converted string, but exception occured.");
        }
    }
}
