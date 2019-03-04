package com.teradata.connector.common.utils;

import java.util.*;

/**
 * @author Administrator
 */
public class ConnectorSchemaParser {
    private HashSet<Character> delimChars;
    private HashSet<Character> quoteChars;
    private HashMap<Character, Character> bracketChars;
    private char escapeChar;
    private boolean matchBrackets;
    private boolean trimSpaces;
    private boolean ignoreQuotes;
    private boolean ignoreContinousDelim;

    public ConnectorSchemaParser() {
        this.escapeChar = '\\';
        this.matchBrackets = true;
        this.trimSpaces = false;
        this.ignoreQuotes = false;
        this.ignoreContinousDelim = false;
        this.delimChars = new HashSet<Character>();
        this.quoteChars = new HashSet<Character>();
        this.bracketChars = new HashMap<Character, Character>();
        this.delimChars.add(',');
        this.delimChars.add(';');
        this.quoteChars.add('\"');
        this.quoteChars.add('\'');
        this.bracketChars.put('(', ')');
        this.bracketChars.put('<', '>');
    }

    public void setDelimChar(final char delimChar) {
        this.delimChars.clear();
        this.delimChars.add(delimChar);
    }

    public void addDelimChar(final char delimChar) {
        this.delimChars.add(delimChar);
    }

    public void setQuoteChar(final char quoteChar) {
        this.quoteChars.clear();
        this.quoteChars.add(quoteChar);
    }

    public void addQuoteChar(final char quoteChar) {
        this.quoteChars.add(quoteChar);
    }

    public void setBracketChars(final char bracketBeginChar, final char bracketEndChar) {
        this.bracketChars.clear();
        this.bracketChars.put(bracketBeginChar, bracketEndChar);
    }

    public void addBracketChars(final char bracketBeginChar, final char bracketEndChar) {
        this.bracketChars.put(bracketBeginChar, bracketEndChar);
    }

    public void setEscapeChar(final char escapeChar) {
        this.escapeChar = escapeChar;
    }

    public void setIgnoreQuotes(final boolean ignoreQuotes) {
        this.ignoreQuotes = ignoreQuotes;
    }

    public void setMatchBrackets(final boolean matchBrackets) {
        this.matchBrackets = matchBrackets;
    }

    public void setTrimSpaces(final boolean trimSpaces) {
        this.trimSpaces = trimSpaces;
    }

    public void setIgnoreContinousDelim(final boolean ignoreContinousDelim) {
        this.ignoreContinousDelim = ignoreContinousDelim;
    }

    public boolean isQuoted(final String text) {
        return this.isQuoted(text, this.trimSpaces);
    }

    public boolean isQuoted(final String text, final boolean toTrimSpaces) {
        if (text == null || text.isEmpty()) {
            return false;
        }
        final String textCopy = toTrimSpaces ? text.trim() : text;
        int currPos = 0;
        final int endPos = textCopy.length();
        char currChar = textCopy.charAt(currPos++);
        final List<Character> quotes = new ArrayList<Character>();
        if (!this.ignoreQuotes && this.isQuoteChar(currChar)) {
            quotes.add(currChar);
            int quoteCount = quotes.size();
            while (currPos < endPos) {
                currChar = textCopy.charAt(currPos++);
                if (quoteCount == 0) {
                    return false;
                }
                if (this.ignoreQuotes || !this.isQuoteChar(currChar)) {
                    continue;
                }
                if (currChar == quotes.get(quoteCount - 1)) {
                    quotes.remove(quoteCount - 1);
                }
                quoteCount = quotes.size();
            }
            return quoteCount == 0;
        }
        return false;
    }

    public List<String> tokenize(final String text) {
        return this.tokenize(text, 0, false);
    }

    public List<String> tokenizeKeepEscape(final String text) {
        return this.tokenize(text, 0, true);
    }

    public List<String> tokenize(final String text, final int maxTokens, final boolean keepEscape) {
        final List<String> tokens = new ArrayList<String>();
        if (text != null && !text.isEmpty()) {
            if (maxTokens == 1) {
                tokens.add(text);
                return tokens;
            }
            int currPos = 0;
            final int endPos = text.length();
            boolean escaped = false;
            boolean continousDelim = false;
            char currChar = text.charAt(currPos++);
            final List<Character> quotes = new ArrayList<Character>();
            final List<Character> bracketBegins = new ArrayList<Character>();
            final StringBuilder builder = new StringBuilder();
            if (currChar == this.escapeChar) {
                if (endPos > 1) {
                    escaped = true;
                    if (keepEscape) {
                        builder.append(this.escapeChar);
                    }
                } else {
                    builder.append(this.escapeChar);
                }
            } else if (this.isDelimChar(currChar)) {
                tokens.add("");
                if (maxTokens == 2) {
                    tokens.add(this.trimSpaces ? text.substring(currPos).trim() : text.substring(currPos));
                    return tokens;
                }
            } else {
                builder.append(currChar);
                if (!this.ignoreQuotes && this.isQuoteChar(currChar)) {
                    quotes.add(currChar);
                } else if (this.matchBrackets && this.isBracketBeginChar(currChar)) {
                    bracketBegins.add(currChar);
                }
            }
            while (currPos < endPos) {
                currChar = text.charAt(currPos++);
                if (escaped) {
                    builder.append(currChar);
                    escaped = false;
                } else if (currChar == this.escapeChar) {
                    if (currPos == endPos) {
                        builder.append(this.escapeChar);
                    } else {
                        escaped = true;
                        if (!keepEscape) {
                            continue;
                        }
                        builder.append(this.escapeChar);
                    }
                } else if (this.isDelimChar(currChar) && tokens.size() != maxTokens - 1) {
                    if ((!this.ignoreQuotes && quotes.size() > 0) || (this.matchBrackets && bracketBegins.size() > 0)) {
                        builder.append(currChar);
                    } else {
                        continousDelim = (builder.length() == 0);
                        if (!continousDelim) {
                            tokens.add(this.trimSpaces ? builder.toString().trim() : builder.toString());
                            builder.setLength(0);
                        } else {
                            if (this.ignoreContinousDelim) {
                                continue;
                            }
                            tokens.add("");
                        }
                    }
                } else {
                    if (!this.ignoreQuotes && this.isQuoteChar(currChar)) {
                        final int quoteCount = quotes.size();
                        if (quoteCount == 0) {
                            quotes.add(currChar);
                        } else if (quoteCount > 0 && currChar == quotes.get(quoteCount - 1)) {
                            quotes.remove(quoteCount - 1);
                        }
                    } else if (this.matchBrackets && this.isBracketBeginChar(currChar)) {
                        if (quotes.size() == 0) {
                            bracketBegins.add(currChar);
                        }
                    } else if (this.matchBrackets && this.isBracketEndChar(currChar) && quotes.size() == 0) {
                        final int bracketBeginCount = bracketBegins.size();
                        if (bracketBeginCount > 0 && currChar == this.bracketChars.get(bracketBegins.get(bracketBeginCount - 1))) {
                            bracketBegins.remove(bracketBeginCount - 1);
                        }
                    }
                    builder.append(currChar);
                }
            }
            tokens.add(this.trimSpaces ? builder.toString().trim() : builder.toString());
        }
        return tokens;
    }

    private boolean isDelimChar(final char c) {
        return this.delimChars.contains(c);
    }

    private boolean isQuoteChar(final char c) {
        return this.quoteChars.contains(c);
    }

    private boolean isBracketBeginChar(final char c) {
        return this.bracketChars.containsKey(c);
    }

    private boolean isBracketEndChar(final char c) {
        return this.bracketChars.containsValue(c);
    }
}
