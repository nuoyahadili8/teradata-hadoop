package com.teradata.connector.common.utils;

import java.io.*;

public class ConnectorStringUtils
{
    public static boolean isEmpty(final String input) {
        return input == null || input.length() == 0;
    }
    
    public static String getExceptionStack(final Throwable t) {
        final StringWriter sw = new StringWriter();
        final PrintWriter ps = new PrintWriter(sw);
        t.printStackTrace(ps);
        return sw.toString();
    }
    
    public static boolean isRegexSpecialChar(final String s) {
        if (s == null || s.length() != 1) {
            return false;
        }
        switch (s.charAt(0)) {
            case '!':
            case '$':
            case '(':
            case ')':
            case '*':
            case '+':
            case '-':
            case '.':
            case ':':
            case '<':
            case '=':
            case '>':
            case '?':
            case '[':
            case '\\':
            case ']':
            case '^':
            case '{':
            case '|':
            case '}': {
                return true;
            }
            default: {
                return false;
            }
        }
    }
}
