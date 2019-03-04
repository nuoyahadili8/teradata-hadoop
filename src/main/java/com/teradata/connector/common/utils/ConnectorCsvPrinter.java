package com.teradata.connector.common.utils;

import com.opencsv.CSVWriter;

import java.io.*;

/**
 * @author Administrator
 */
public class ConnectorCsvPrinter {
    private char fieldSeparator;
    private char quoteChar;
    private char escapeChar;
    private CSVWriter csvWriter;
    private CharArrayWriter out;

    public ConnectorCsvPrinter() {
        this.fieldSeparator = ',';
        this.quoteChar = '\"';
        this.escapeChar = '\\';
        this.initialize();
    }

    public ConnectorCsvPrinter(final String fieldSeparator, final String quoteChar, final String escapeChar) {
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

    public ConnectorCsvPrinter(final char fieldSeparator, final char quoteChar, final char escapeChar) {
        this.fieldSeparator = ',';
        this.quoteChar = '\"';
        this.escapeChar = '\\';
        this.fieldSeparator = fieldSeparator;
        this.quoteChar = quoteChar;
        this.escapeChar = escapeChar;
        this.initialize();
    }

    private void initialize() {
        this.out = new CharArrayWriter();
        this.csvWriter = new CSVWriter((Writer) this.out, this.fieldSeparator, this.quoteChar, this.escapeChar, "");
    }

    public String print(final String[] objects) throws IOException {
        this.out.reset();
        this.csvWriter.writeNext(objects);
        return this.out.toString();
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
