package com.teradata.connector.common.utils;


import org.junit.Assert;
import org.junit.Test;

/**
 * @author Administrator
 */
public class ConnectorSchemaParserTest
{
    @Test
    public void testConnectorSchemaParser() {
        final ConnectorSchemaParser parser = new ConnectorSchemaParser();
        Assert.assertEquals(false, parser.isQuoted("'one' "));
        parser.setTrimSpaces(true);
        Assert.assertEquals(true, parser.isQuoted("'one' "));
        final String text = "'one',, ('two', three), four,";
        Assert.assertEquals(1, parser.tokenize(text, 1, false).size());
        Assert.assertEquals(5, parser.tokenize(text).size());
        parser.setIgnoreContinousDelim(true);
        Assert.assertEquals(4, parser.tokenize(text).size());
        parser.setBracketChars('{', '}');
        Assert.assertEquals(5, parser.tokenize(text).size());
        parser.setDelimChar('/');
        Assert.assertEquals(1, parser.tokenize(text).size());
        parser.setDelimChar(',');
        parser.setQuoteChar('`');
        Assert.assertEquals(4, parser.tokenize("'one',, ('two', three), `four,`").size());
        parser.setIgnoreQuotes(true);
        Assert.assertEquals(5, parser.tokenize("'one',, ('two', three), `four,`").size());
        parser.setMatchBrackets(false);
        Assert.assertEquals(2, parser.tokenize("{one,two}").size());
        parser.setMatchBrackets(true);
        Assert.assertEquals(1, parser.tokenize("{one,two}").size());
        parser.setEscapeChar(',');
        Assert.assertEquals("one\\two", (String)parser.tokenize("one\\,two").get(0));
    }
}
