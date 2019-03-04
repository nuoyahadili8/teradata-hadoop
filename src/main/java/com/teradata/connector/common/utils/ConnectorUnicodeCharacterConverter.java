package com.teradata.connector.common.utils;

import com.teradata.connector.common.exception.*;

/**
 * @author Administrator
 */
public class ConnectorUnicodeCharacterConverter {
    private static final char[] hexChars = new char[]{'0', '1', '2', '3', '4', '5', '6', '7', '8', '9', 'A', 'B', 'C', 'D', 'E', 'F'};;

    private static char charToHexChar(final int ch) {
        return hexChars[ch & 15];
    }

    public static String toEncodedUnicode(final String input) {
        if (input == null) {
            return null;
        }
        StringBuilder output = new StringBuilder();
        for (int x = 0; x < input.length(); x++) {
            char ch = input.charAt(x);
            if (ch <= '=' || ch >= 127) {
                if (ch < ' ' || ch > '~') {
                    output.append('\\');
                    output.append('u');
                    output.append(charToHexChar((ch >> 12) & 15));
                    output.append(charToHexChar((ch >> 8) & 15));
                    output.append(charToHexChar((ch >> 4) & 15));
                    output.append(charToHexChar(ch & 15));
                } else {
                    output.append(ch);
                }
            } else if (ch == '\\') {
                output.append('\\').append('\\');
            } else {
                output.append(ch);
            }
        }
        return output.toString();
    }

    public static String fromEncodedUnicode(final String input) throws ConnectorException {
        if (input == null) {
            return input;
        }
        final StringBuilder output = new StringBuilder();
        int x = 0;
        while (x < input.length()) {
            char ch = input.charAt(x++);
            if (ch == '\\' && input.length() != 1) {
                final char chtemp = ch;
                ch = input.charAt(x++);
                if (ch == 'u') {
                    int value = 0;
                    for (int i = 0; i < 4; ++i) {
                        ch = input.charAt(x++);
                        if (ch >= '0' && ch <= '9') {
                            value = (value << 4) + ch - 48;
                        } else if (ch >= 'a' && ch <= 'f') {
                            value = (value << 4) + 10 + ch - 97;
                        } else {
                            if (ch < 'A' || ch > 'F') {
                                throw new ConnectorException(15004);
                            }
                            value = (value << 4) + 10 + ch - 65;
                        }
                    }
                    output.append((char) value);
                } else {
                    if (ch != 'u' && chtemp == '\\') {
                        return "\\";
                    }
                    throw new ConnectorException(15004);
                }
            } else {
                output.append(ch);
            }
        }
        return output.toString();
    }
}
