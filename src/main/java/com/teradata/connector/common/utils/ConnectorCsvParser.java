package com.teradata.connector.common.utils;

import com.opencsv.CSVParser;

import java.io.*;

/**
 * @author Administrator
 */
public class ConnectorCsvParser {
    private char fieldSeparator;
    private char quoteChar;
    private char escapeChar;
    private CSVParser csvParser;

    public ConnectorCsvParser() {
        this.fieldSeparator = ',';
        this.quoteChar = '\"';
        this.escapeChar = '\\';
        this.initialize();
    }

    public ConnectorCsvParser(final String fieldSeparator, final String quoteChar, final String escapeChar) {
        this.fieldSeparator = ',';
        this.quoteChar = '\"';
        this.escapeChar = '\\';
        if (!fieldSeparator.isEmpty()) {
            this.fieldSeparator = fieldSeparator.charAt(0);
        }
        if (!quoteChar.isEmpty()) {
            this.quoteChar = quoteChar.charAt(0);
        }
        if (!escapeChar.isEmpty()) {
            this.escapeChar = escapeChar.charAt(0);
        }
        this.initialize();
    }

    public ConnectorCsvParser(final char fieldSeparator, final char quoteChar, final char escapeChar) {
        this.fieldSeparator = ',';
        this.quoteChar = '\"';
        this.escapeChar = '\\';
        this.fieldSeparator = fieldSeparator;
        this.quoteChar = quoteChar;
        this.escapeChar = escapeChar;
        this.initialize();
    }

    private void initialize() {
        this.csvParser = new CSVParser(this.fieldSeparator, this.quoteChar, this.escapeChar);
    }

    public String[] parse(final String csvString) throws IOException {
        final String[] result = this.csvParser.parseLine(csvString);
        return result;
    }

    public char getFieldSeparator() {
        return this.fieldSeparator;
    }

    public char getQuoteChar() {
        return this.quoteChar;
    }

    public char getEscapeChar() {
        return this.escapeChar;
    }
}
