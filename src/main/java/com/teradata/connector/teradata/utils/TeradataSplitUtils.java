package com.teradata.connector.teradata.utils;

import com.teradata.connector.common.exception.ConnectorException;
import com.teradata.connector.common.utils.ConnectorConfiguration;
import com.teradata.connector.common.utils.ConnectorSchemaUtils;
import com.teradata.connector.teradata.TeradataInputFormat;
import java.math.BigDecimal;
import java.math.RoundingMode;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Time;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Date;
import java.util.List;
import org.apache.hadoop.conf.Configuration;


public class TeradataSplitUtils {
    private static final BigDecimal POINT_BASE;
    private static final int NUM_CHARS = 8;
    protected static boolean nullable;
    protected static Object defaultValue;

    public static List<BigDecimal> getSplitPoints(final BigDecimal minValue, final BigDecimal maxValue, final long num, final int type) {
        final BigDecimal numSplit = new BigDecimal(num);
        final BigDecimal one = new BigDecimal(1);
        final List<BigDecimal> list = new ArrayList<BigDecimal>();
        final BigDecimal remainder = maxValue.subtract(minValue).remainder(numSplit);
        final int intRemainder = remainder.intValue();
        final BigDecimal splitRange = bigDecimalDivision(maxValue.subtract(minValue), numSplit);
        BigDecimal currentPoint = minValue;
        list.add(currentPoint);
        for (long i = 1L; i < num; ++i) {
            currentPoint = currentPoint.add(splitRange);
            if (type == 4 && i <= intRemainder) {
                currentPoint = currentPoint.add(one);
            }
            list.add(currentPoint);
        }
        return list;
    }

    private static BigDecimal stringToBigDecimal(final String value) {
        BigDecimal denominatorValue = TeradataSplitUtils.POINT_BASE;
        BigDecimal charBigDecimal = BigDecimal.ZERO;
        for (int len = Math.min(value.length(), 8), i = 0; i < len; ++i) {
            final int codePoint = value.codePointAt(i);
            charBigDecimal = charBigDecimal.add(bigDecimalDivision(new BigDecimal(codePoint), denominatorValue));
            denominatorValue = denominatorValue.multiply(TeradataSplitUtils.POINT_BASE);
        }
        return charBigDecimal;
    }

    private static String bigDecimalToString(final BigDecimal bigDecimal) {
        BigDecimal currentValue = bigDecimal.stripTrailingZeros();
        final StringBuilder stringBuilder = new StringBuilder();
        for (int numConverted = 0; numConverted < 8; ++numConverted) {
            currentValue = currentValue.multiply(TeradataSplitUtils.POINT_BASE);
            final int currentCodeInt = currentValue.intValue();
            int transferredCodeInt;
            if (0 == (transferredCodeInt = currentCodeInt)) {
                break;
            }
            transferredCodeInt = getSupportedCharCode(currentCodeInt);
            currentValue = currentValue.subtract(new BigDecimal(currentCodeInt));
            stringBuilder.append(Character.toChars(transferredCodeInt));
        }
        return stringBuilder.toString();
    }

    private static Date longToDate(final long value, final int sqlDataType) throws ConnectorException {
        switch (sqlDataType) {
            case 91: {
                return new java.sql.Date(value);
            }
            case 92: {
                return new Time(value);
            }
            case 93: {
                return new Timestamp(value);
            }
            default: {
                throw new ConnectorException(23001);
            }
        }
    }

    private static long dateToLong(final ResultSet resultSet, final int columnIndex, final int sqlDataType) throws ConnectorException {
        try {
            switch (sqlDataType) {
                case 91: {
                    return resultSet.getDate(columnIndex).getTime();
                }
                case 92: {
                    return resultSet.getTime(columnIndex).getTime();
                }
                case 93: {
                    return resultSet.getTimestamp(columnIndex).getTime();
                }
                default: {
                    throw new ConnectorException(23001);
                }
            }
        } catch (SQLException e) {
            throw new ConnectorException(e.getMessage(), e);
        }
    }

    private static List<Long> getTimeSplitPoints(final long numSplits, final long minValue, final long maxValue) {
        final List<Long> splitPoints = new ArrayList<Long>();
        final List<BigDecimal> points = getSplitPoints(new BigDecimal(minValue), new BigDecimal(maxValue), numSplits, 92);
        for (int i = 0; i < points.size(); ++i) {
            splitPoints.add(points.get(i).longValue());
        }
        return splitPoints;
    }

    public static List<TeradataInputFormat.TeradataInputSplit> getSplitsByTimeType(final Configuration configuration, final String splitByColumn, final ResultSet resultSetMinMaxValues) throws ConnectorException {
        try {
            final int sqlDataType = resultSetMinMaxValues.getMetaData().getColumnType(1);
            final long minValue = dateToLong(resultSetMinMaxValues, 1, sqlDataType);
            final long maxValue = dateToLong(resultSetMinMaxValues, 2, sqlDataType);
            final String lowerBoundary = ConnectorSchemaUtils.quoteFieldName(splitByColumn) + " >= ";
            final String upperBoundary = ConnectorSchemaUtils.quoteFieldName(splitByColumn) + " < ";
            final String splitSql = TeradataPlugInConfiguration.getInputSplitSql(configuration);
            final int numSplits = ConnectorConfiguration.getNumMappers(configuration);
            final List<Long> splitPoints = getTimeSplitPoints(numSplits, minValue, maxValue);
            final List<TeradataInputFormat.TeradataInputSplit> splits = new ArrayList<TeradataInputFormat.TeradataInputSplit>();
            final long lowPoint = splitPoints.get(0);
            Date startDate = longToDate(lowPoint, sqlDataType);
            long highPoint = lowPoint;
            Date endDate = longToDate(highPoint, sqlDataType);
            if (sqlDataType == 93) {
                ((Timestamp) startDate).setNanos(resultSetMinMaxValues.getTimestamp(1).getNanos());
            }
            int i = 0;
            while (i < splitPoints.size()) {
                if (i == splitPoints.size() - 1) {
                    if (sqlDataType == 93) {
                        ((Timestamp) endDate).setNanos(resultSetMinMaxValues.getTimestamp(2).getNanos());
                    }
                    splits.add(new TeradataInputFormat.TeradataInputSplit(splitSql + " " + lowerBoundary + "'" + startDate + "'"));
                    ++i;
                } else {
                    ++i;
                    highPoint = splitPoints.get(i);
                    endDate = longToDate(highPoint, sqlDataType);
                    splits.add(new TeradataInputFormat.TeradataInputSplit(splitSql + " " + lowerBoundary + "'" + startDate + "' AND " + upperBoundary + "'" + endDate + "'"));
                }
                startDate = endDate;
            }
            return splits;
        } catch (SQLException e) {
            throw new ConnectorException(e.getMessage(), e);
        }
    }

    public static List<TeradataInputFormat.TeradataInputSplit> getSplitsByBooleanType(final Configuration conf, final String splitByColumn, final boolean minValue, final boolean maxValue) {
        final List<TeradataInputFormat.TeradataInputSplit> splits = new ArrayList<TeradataInputFormat.TeradataInputSplit>();
        final String splitSql = TeradataPlugInConfiguration.getInputSplitSql(conf);
        if (!minValue) {
            splits.add(new TeradataInputFormat.TeradataInputSplit(splitSql + " " + ConnectorSchemaUtils.quoteFieldName(splitByColumn) + " = FALSE"));
        }
        if (maxValue) {
            splits.add(new TeradataInputFormat.TeradataInputSplit(splitSql + " " + ConnectorSchemaUtils.quoteFieldName(splitByColumn) + " = TRUE"));
        }
        return splits;
    }

    private static List<Double> getDoubleSplitPoints(final long numSplits, final double minValue, final double maxValue) {
        final List<Double> splitPoints = new ArrayList<Double>();
        final List<BigDecimal> points = getSplitPoints(new BigDecimal(minValue), new BigDecimal(maxValue), numSplits, 8);
        for (int i = 0; i < points.size(); ++i) {
            splitPoints.add(points.get(i).doubleValue());
        }
        return splitPoints;
    }

    public static List<TeradataInputFormat.TeradataInputSplit> getSplitsByDoubleType(final Configuration configuration, final String splitByColumn, final double minValue, final double maxValue) {
        final List<TeradataInputFormat.TeradataInputSplit> splits = new ArrayList<TeradataInputFormat.TeradataInputSplit>();
        final String splitSql = TeradataPlugInConfiguration.getInputSplitSql(configuration);
        final double minDoubleValue = minValue;
        final double maxDoubleValue = maxValue;
        final long numSplits = ConnectorConfiguration.getNumMappers(configuration);
        final String lowerBoundary = ConnectorSchemaUtils.quoteFieldName(splitByColumn) + " >= ";
        final String upperBoundary = ConnectorSchemaUtils.quoteFieldName(splitByColumn) + " < ";
        final List<Double> splitPoints = getDoubleSplitPoints(numSplits, minDoubleValue, maxDoubleValue);
        double highPoint;
        double lowPoint = highPoint = splitPoints.get(0);
        int i = 0;
        while (i < splitPoints.size()) {
            if (i == splitPoints.size() - 1) {
                splits.add(new TeradataInputFormat.TeradataInputSplit(splitSql + " " + lowerBoundary + Double.toString(lowPoint)));
                ++i;
            } else {
                ++i;
                highPoint = splitPoints.get(i);
                splits.add(new TeradataInputFormat.TeradataInputSplit(splitSql + " " + lowerBoundary + Double.toString(lowPoint) + " AND " + upperBoundary + Double.toString(highPoint)));
            }
            lowPoint = highPoint;
        }
        return splits;
    }

    private static List<Long> getIntegerSplitPoints(final long numSplits, final long minValue, final long maxValue) {
        final List<Long> splitPoints = new ArrayList<Long>();
        final List<BigDecimal> points = getSplitPoints(new BigDecimal(minValue), new BigDecimal(maxValue), numSplits, 4);
        for (int i = 0; i < points.size(); ++i) {
            splitPoints.add(points.get(i).longValue());
        }
        return splitPoints;
    }

    public static List<TeradataInputFormat.TeradataInputSplit> getSplitsByIntType(final Configuration configuration, final String splitByColumn, final long minValue, final long maxValue) {
        final long minIntValue = minValue;
        final long maxIntValue = maxValue;
        final String lowerBoundary = ConnectorSchemaUtils.quoteFieldName(splitByColumn) + " >= ";
        final String upperBoundary = ConnectorSchemaUtils.quoteFieldName(splitByColumn) + " < ";
        final String splitSql = TeradataPlugInConfiguration.getInputSplitSql(configuration);
        final int numSplits = ConnectorConfiguration.getNumMappers(configuration);
        final List<Long> splitPoints = getIntegerSplitPoints(numSplits, minIntValue, maxIntValue);
        final List<TeradataInputFormat.TeradataInputSplit> splits = new ArrayList<TeradataInputFormat.TeradataInputSplit>();
        long highPoint;
        long lowPoint = highPoint = splitPoints.get(0);
        int i = 0;
        while (i < splitPoints.size()) {
            if (i == splitPoints.size() - 1) {
                splits.add(new TeradataInputFormat.TeradataInputSplit(splitSql + " " + lowerBoundary + Long.toString(lowPoint)));
                ++i;
            } else {
                ++i;
                highPoint = splitPoints.get(i);
                splits.add(new TeradataInputFormat.TeradataInputSplit(splitSql + " " + lowerBoundary + Long.toString(lowPoint) + " AND " + upperBoundary + Long.toString(highPoint)));
            }
            lowPoint = highPoint;
        }
        return splits;
    }

    private static BigDecimal bigDecimalDivision(final BigDecimal numerator, final BigDecimal denominator) {
        try {
            return numerator.divide(denominator);
        } catch (ArithmeticException ae) {
            return numerator.divide(denominator, 1);
        }
    }

    private static List<BigDecimal> getBigDecimalSplitPoints(final long numSplits, final BigDecimal minDecimalValue, final BigDecimal maxDecimalValue) {
        final List<BigDecimal> splits = getSplitPoints(minDecimalValue, maxDecimalValue, numSplits, 3);
        return splits;
    }

    public static List<TeradataInputFormat.TeradataInputSplit> getSplitsByDecimalType(final Configuration configuration, final String splitByColumn, final BigDecimal minValue, final BigDecimal maxValue) {
        final BigDecimal minDecimalValue = minValue;
        final BigDecimal maxDecimalValue = maxValue;
        final String lowerBoundary = ConnectorSchemaUtils.quoteFieldName(splitByColumn) + " >= ";
        final String upperBoundary = ConnectorSchemaUtils.quoteFieldName(splitByColumn) + " < ";
        final String splitSql = TeradataPlugInConfiguration.getInputSplitSql(configuration);
        final long numSplits = ConnectorConfiguration.getNumMappers(configuration);
        final List<BigDecimal> splitPoints = getBigDecimalSplitPoints(numSplits, minDecimalValue, maxDecimalValue);
        final List<TeradataInputFormat.TeradataInputSplit> splits = new ArrayList<TeradataInputFormat.TeradataInputSplit>();
        BigDecimal highPoint;
        BigDecimal lowPoint = highPoint = splitPoints.get(0);
        int i = 0;
        while (i < splitPoints.size()) {
            if (i == splitPoints.size() - 1) {
                splits.add(new TeradataInputFormat.TeradataInputSplit(splitSql + " " + lowerBoundary + lowPoint.toString()));
                ++i;
            } else {
                ++i;
                highPoint = splitPoints.get(i);
                splits.add(new TeradataInputFormat.TeradataInputSplit(splitSql + " " + lowerBoundary + lowPoint.toString() + " AND " + upperBoundary + highPoint.toString()));
            }
            lowPoint = highPoint;
        }
        return splits;
    }

    public static List<String> getStringSplitPoints(final int numSplits, final String minString, final String maxString) {
        List<String> splitStrings = new ArrayList<String>();
        if (minString.matches("[0-9]+") && maxString.matches("[0-9]+")) {
            splitStrings.add(minString);
            BigDecimal minVal = (BigDecimal) TeradataSplitUtils.defaultValue;
            BigDecimal maxVal = (BigDecimal) TeradataSplitUtils.defaultValue;
            minVal = StringToBigDecimal(minString);
            maxVal = StringToBigDecimal(maxString);
            final List<BigDecimal> splitPoints = getBigDecimalSplitPointsString(numSplits, minVal, maxVal);
            for (final BigDecimal BigDecimalValue : splitPoints) {
                splitStrings.add(BigDecimalToSTring(BigDecimalValue));
            }
            Collections.sort(splitStrings);
        } else {
            splitStrings = getAsciiStringSplitPoints(numSplits, minString, maxString);
            Collections.sort(splitStrings);
        }
        return splitStrings;
    }

    public static List<TeradataInputFormat.TeradataInputSplit> getSplitsByStringType(final Configuration configuration, final String splitByColumn, final String minValue, final String maxValue) {
        final String splitSql = TeradataPlugInConfiguration.getInputSplitSql(configuration);
        final int numSplits = ConnectorConfiguration.getNumMappers(configuration);
        final String lowClausePrefix = ConnectorSchemaUtils.quoteFieldName(splitByColumn) + " >= '";
        final String highClausePrefix = ConnectorSchemaUtils.quoteFieldName(splitByColumn) + " < '";
        final List<String> splitStrings = getStringSplitPoints(numSplits, minValue, maxValue);
        final List<TeradataInputFormat.TeradataInputSplit> splits = new ArrayList<TeradataInputFormat.TeradataInputSplit>();
        String endString;
        String startString = endString = splitStrings.get(0);
        int i = 0;
        while (i < splitStrings.size()) {
            if (i == splitStrings.size() - 1) {
                splits.add(new TeradataInputFormat.TeradataInputSplit(splitSql + " " + lowClausePrefix + startString.replace("'", "''") + "'"));
                ++i;
            } else {
                ++i;
                endString = splitStrings.get(i);
                splits.add(new TeradataInputFormat.TeradataInputSplit(splitSql + " " + lowClausePrefix + startString.replace("'", "''") + "' AND " + highClausePrefix + endString.replace("'", "''") + "'"));
            }
            startString = endString;
        }
        return splits;
    }

    private static List<BigDecimal> getBigDecimalSplitPointsString(final long numSplits, final BigDecimal minDecimalValue, final BigDecimal maxDecimalValue) {
        final List<BigDecimal> splits = getSplitPointsString(minDecimalValue, maxDecimalValue, numSplits);
        return splits;
    }

    public static BigDecimal StringToBigDecimal(final String str) {
        if (str == null || str.isEmpty()) {
            return TeradataSplitUtils.nullable ? null : ((BigDecimal) TeradataSplitUtils.defaultValue);
        }
        return new BigDecimal(str);
    }

    public static List<Integer> getStringAsciiList(final String input) {
        final char[] ascii = input.toCharArray();
        final List<Integer> asciiInt = new ArrayList<Integer>();
        for (final char ch : ascii) {
            asciiInt.add((int) ch);
        }
        return asciiInt;
    }

    public static List<String> getSplitInDiff(final List<Integer> asciiInMax, final List<Integer> asciiInMin, final List<String> inSplitStrings, final int index, final int numSplits) {
        final List<String> splitStrings = inSplitStrings;
        final int getNextMin = asciiInMin.get(index + 1);
        final int totRange = 255 - getNextMin;
        final int splitVal = totRange / numSplits;
        int nm = 1;
        if (numSplits == 0) {
            return splitStrings;
        }
        while (nm < numSplits) {
            final List<Integer> asciiInt = new ArrayList<Integer>();
            asciiInt.addAll(asciiInMin);
            int substValue = getNextMin + splitVal * nm;
            if (substValue > 255) {
                substValue = 255;
            }
            asciiInt.remove(index + 1);
            asciiInt.add(index + 1, substValue);
            splitStrings.add(getAsciiToString(asciiInt));
            ++nm;
        }
        return splitStrings;
    }

    public static List<String> getSplitInDiffSizeMismatch(final List<Integer> asciiInMax, final List<Integer> asciiInMin, final List<String> inSplitStrings, final int index, final int numSplits) {
        final List<String> splitStrings = inSplitStrings;
        final int getMax = asciiInMax.get(index);
        final int getNextMin = asciiInMin.get(index + 1);
        final int totRange = getNextMin + getMax;
        final int splitVal = totRange / numSplits;
        int nm = 1;
        int substValue = 0;
        if (numSplits == 0) {
            return splitStrings;
        }
        while (nm < numSplits) {
            final List<Integer> asciiInt = new ArrayList<Integer>();
            asciiInt.addAll(asciiInMin);
            substValue += getNextMin + splitVal;
            if (substValue > 255) {
                substValue = 255;
            }
            asciiInt.remove(index + 1);
            asciiInt.add(index + 1, substValue);
            splitStrings.add(getAsciiToString(asciiInt));
            ++nm;
        }
        return splitStrings;
    }

    public static String BigDecimalToSTring(final BigDecimal bigdecimal) {
        if (bigdecimal == null) {
            return TeradataSplitUtils.nullable ? null : "";
        }
        return bigdecimal.toPlainString();
    }

    protected static List<BigDecimal> getSplitPointsString(final BigDecimal minValue, final BigDecimal maxValue, final long num) {
        final int scale = 0;
        final BigDecimal numSplit = new BigDecimal(num);
        final List<BigDecimal> list = new ArrayList<BigDecimal>();
        final BigDecimal splitRange = bigDecimalDivision(maxValue.subtract(minValue), numSplit);
        BigDecimal currentPoint = minValue;
        for (long i = 1L; i < num; ++i) {
            currentPoint = currentPoint.add(splitRange);
            list.add(currentPoint.setScale(scale, RoundingMode.HALF_UP));
        }
        return list;
    }

    public static String getAsciiToString(final List<Integer> splitAsciiList) {
        String asciiToStr = "";
        for (int asciival : splitAsciiList) {
            if (asciival >= 127 && asciival <= 160) {
                asciival = 161 + (asciival - 127);
            }
            if (asciival >= 0 && asciival <= 32) {
                asciival += 33;
            }
            if (asciival == 173 || asciival > 255) {
                asciival = 256;
            }
            asciiToStr += Character.toString((char) asciival);
        }
        return asciiToStr;
    }

    public static List<String> getAsciiStringSplitPoints(final int numSplits, final String minString, final String maxString) {
        List<String> splitStrings = new ArrayList<String>();
        final List<Integer> asciiIntMax = getStringAsciiList(maxString);
        final List<Integer> asciiIntMin = getStringAsciiList(minString);
        int mx = 0;
        int mn = 0;
        if (minString.equals(maxString)) {
            splitStrings.add(getAsciiToString(asciiIntMin));
            return splitStrings;
        }
        for (final int max : asciiIntMax) {
            if (mx < asciiIntMin.size()) {
                for (final int min : asciiIntMin) {
                    if (mx == mn) {
                        if (max == min) {
                            break;
                        }
                        if (max > min) {
                            final int diff = max - min;
                            splitStrings.add(getAsciiToString(asciiIntMin));
                            if (diff == numSplits) {
                                int nm = 1;
                                int minNum = min;
                                while (nm < numSplits) {
                                    ++minNum;
                                    final List<Integer> asciiInt = new ArrayList<Integer>();
                                    asciiInt.addAll(asciiIntMin);
                                    asciiInt.remove(mn);
                                    asciiInt.add(mn, minNum);
                                    splitStrings.add(getAsciiToString(asciiInt));
                                    ++nm;
                                }
                                return splitStrings;
                            }
                            if (diff >= numSplits) {
                                final int rem = diff % numSplits;
                                final int iNumSplitsPerDiff = diff / numSplits;
                                int nm2 = 0;
                                final int getMin = asciiIntMin.get(mn);
                                final int getmax = asciiIntMax.get(mx);
                                while (nm2 < diff) {
                                    final List<Integer> asciiInt2 = new ArrayList<Integer>();
                                    if (diff - nm2 < rem) {
                                        int subValue = 1;
                                        if (rem != 1) {
                                            subValue = rem / 2;
                                        }
                                        asciiInt2.addAll(asciiIntMax);
                                        asciiInt2.remove(mn);
                                        asciiInt2.add(mn, getmax - subValue);
                                        nm2 = diff + 1;
                                    } else {
                                        asciiInt2.addAll(asciiIntMin);
                                        asciiInt2.remove(mn);
                                        if (getMin + (iNumSplitsPerDiff + nm2) >= getmax) {
                                            asciiInt2.add(mn, getmax - 1);
                                            nm2 = diff + 1;
                                        } else if (rem == 0) {
                                            asciiInt2.add(mn, getMin + (iNumSplitsPerDiff + nm2));
                                            nm2 += iNumSplitsPerDiff;
                                        } else if (iNumSplitsPerDiff != 1) {
                                            asciiInt2.add(mn, getMin + (iNumSplitsPerDiff + nm2 + 1));
                                            nm2 = iNumSplitsPerDiff + nm2 + 1;
                                        } else if (nm2 == numSplits - 1) {
                                            nm2 = diff + 1;
                                        } else {
                                            asciiInt2.add(mn, getMin + (iNumSplitsPerDiff + nm2));
                                            nm2 += iNumSplitsPerDiff;
                                        }
                                    }
                                    if (splitStrings.size() < numSplits) {
                                        splitStrings.add(getAsciiToString(asciiInt2));
                                    }
                                }
                                return splitStrings;
                            }
                            if (diff == 1) {
                                splitStrings = getSplitInDiff(asciiIntMax, asciiIntMin, splitStrings, mn, numSplits);
                                return splitStrings;
                            }
                            final int rem = numSplits % diff;
                            final int iNumSplitsPerDiff = numSplits / diff;
                            int nm2 = 0;
                            final int getMin = asciiIntMin.get(mn);
                            while (nm2 < diff) {
                                if (iNumSplitsPerDiff != 1) {
                                    final List<Integer> asciiMn = new ArrayList<Integer>();
                                    final List<Integer> asciiMx = new ArrayList<Integer>();
                                    asciiMn.addAll(asciiIntMin);
                                    asciiMx.addAll(asciiIntMax);
                                    asciiMn.remove(mn);
                                    asciiMn.add(mn, getMin + nm2);
                                    asciiMx.remove(mx);
                                    asciiMx.add(mx, getMin + nm2 + 1);
                                    if (diff - nm2 == 1) {
                                        splitStrings = getSplitInDiff(asciiMx, asciiMn, splitStrings, mn, iNumSplitsPerDiff + rem);
                                        if (splitStrings.size() < numSplits) {
                                            splitStrings.add(getAsciiToString(asciiMx));
                                        }
                                    } else {
                                        splitStrings = getSplitInDiff(asciiMx, asciiMn, splitStrings, mn, iNumSplitsPerDiff);
                                        if (splitStrings.size() < numSplits) {
                                            splitStrings.add(getAsciiToString(asciiMx));
                                        }
                                    }
                                } else {
                                    final List<Integer> asciiInt3 = new ArrayList<Integer>();
                                    asciiInt3.addAll(asciiIntMin);
                                    asciiInt3.remove(mn);
                                    asciiInt3.add(mn, getMin + nm2 + 1);
                                    if (diff - nm2 == 1) {
                                        final List<Integer> asciiMn2 = new ArrayList<Integer>();
                                        final List<Integer> asciiMx2 = new ArrayList<Integer>();
                                        asciiMn2.addAll(asciiIntMin);
                                        asciiMx2.addAll(asciiIntMax);
                                        asciiMn2.remove(mn);
                                        asciiMn2.add(mn, getMin + nm2);
                                        splitStrings = getSplitInDiff(asciiMx2, asciiMn2, splitStrings, mn, iNumSplitsPerDiff + rem);
                                        if (splitStrings.size() < numSplits) {
                                            splitStrings.add(getAsciiToString(asciiMx2));
                                        }
                                    } else if (splitStrings.size() < numSplits) {
                                        splitStrings.add(getAsciiToString(asciiInt3));
                                    }
                                }
                                ++nm2;
                            }
                            return splitStrings;
                        }
                    }
                    ++mn;
                }
                ++mx;
                mn = 0;
            } else {
                splitStrings.add(getAsciiToString(asciiIntMin));
                final int sizediff = asciiIntMax.size() - asciiIntMin.size();
                final int mxsize = asciiIntMax.size();
                if (sizediff == numSplits) {
                    for (int nm3 = 1; nm3 < numSplits; ++nm3) {
                        final List<Integer> asciiInt4 = asciiIntMin;
                        asciiInt4.add(asciiIntMax.get(mxsize - (numSplits - nm3 + 1)));
                        splitStrings.add(getAsciiToString(asciiInt4));
                    }
                    return splitStrings;
                }
                if (sizediff > numSplits) {
                    final int rem2 = sizediff % numSplits;
                    int nm = 1;
                    if (rem2 == 0) {
                        final int numSplitsDiff = sizediff / numSplits;
                        while (nm < sizediff) {
                            final List<Integer> asciiInt = asciiIntMin;
                            asciiInt.add(asciiIntMax.get(mxsize - (sizediff - nm + 1)));
                            if (nm % numSplitsDiff == 0) {
                                splitStrings.add(getAsciiToString(asciiInt));
                            }
                            ++nm;
                        }
                    } else {
                        final int numSplitsDiff = sizediff / numSplits;
                        while (nm <= sizediff) {
                            final List<Integer> asciiInt = asciiIntMin;
                            asciiInt.add(asciiIntMax.get(mxsize - (sizediff - nm + 1)));
                            if (nm % (numSplitsDiff + 1) == 0 && asciiInt.size() != asciiIntMax.size()) {
                                splitStrings.add(getAsciiToString(asciiInt));
                            }
                            ++nm;
                        }
                        if (splitStrings.size() < numSplits) {
                            final List<Integer> asciiInt = new ArrayList<Integer>();
                            asciiInt.addAll(asciiIntMax);
                            asciiInt.remove(asciiIntMax.size() - 1);
                            asciiInt.add(asciiIntMax.size() - 1, 0);
                            splitStrings = getSplitInDiffSizeMismatch(asciiIntMax, asciiInt, splitStrings, asciiInt.size() - 2, numSplits - splitStrings.size() + 1);
                        }
                    }
                    return splitStrings;
                }
                final int rem2 = numSplits % sizediff;
                final int iNumSplitsPerDiff2 = numSplits / sizediff;
                for (int nm4 = 0; nm4 < sizediff; ++nm4) {
                    final List<Integer> asciiInt = asciiIntMin;
                    asciiInt.add(asciiIntMax.get(mxsize - (sizediff - nm4)));
                    final List<Integer> asciiMn3 = new ArrayList<Integer>();
                    final List<Integer> asciiMx3 = new ArrayList<Integer>();
                    asciiMx3.addAll(asciiInt);
                    asciiMn3.addAll(asciiInt);
                    asciiMn3.remove(asciiMn3.size() - 1);
                    asciiMn3.add(0);
                    if (sizediff - nm4 == 1) {
                        splitStrings = getSplitInDiffSizeMismatch(asciiMx3, asciiMn3, splitStrings, asciiMn3.size() - 2, iNumSplitsPerDiff2 + rem2);
                    } else if (iNumSplitsPerDiff2 == 1) {
                        splitStrings.add(getAsciiToString(asciiInt));
                    } else {
                        splitStrings = getSplitInDiffSizeMismatch(asciiMx3, asciiMn3, splitStrings, asciiMn3.size() - 2, iNumSplitsPerDiff2 + 1);
                    }
                }
                return splitStrings;
            }
        }
        return splitStrings;
    }

    public static List<TeradataInputFormat.TeradataInputSplit> getSplitsByColumnType(final Configuration configuration, final String splitbyColumn, final ResultSet resultSetMinMaxValues) throws ConnectorException {
        try {
            final int splitColumnDataType = resultSetMinMaxValues.getMetaData().getColumnType(1);
            switch (splitColumnDataType) {
                case 2:
                case 3: {
                    return getSplitsByDecimalType(configuration, splitbyColumn, resultSetMinMaxValues.getBigDecimal(1), resultSetMinMaxValues.getBigDecimal(2));
                }
                case -7:
                case 16: {
                    return getSplitsByBooleanType(configuration, splitbyColumn, resultSetMinMaxValues.getBoolean(1), resultSetMinMaxValues.getBoolean(2));
                }
                case -6:
                case -5:
                case 4:
                case 5: {
                    return getSplitsByIntType(configuration, splitbyColumn, resultSetMinMaxValues.getLong(1), resultSetMinMaxValues.getLong(2));
                }
                case 6:
                case 7:
                case 8: {
                    return getSplitsByDoubleType(configuration, splitbyColumn, resultSetMinMaxValues.getDouble(1), resultSetMinMaxValues.getDouble(2));
                }
                case -1:
                case 1:
                case 12: {
                    return getSplitsByStringType(configuration, splitbyColumn, resultSetMinMaxValues.getString(1), resultSetMinMaxValues.getString(2));
                }
                case 91:
                case 92:
                case 93: {
                    return getSplitsByTimeType(configuration, splitbyColumn, resultSetMinMaxValues);
                }
                default: {
                    throw new ConnectorException(23001);
                }
            }
        } catch (SQLException e) {
            throw new ConnectorException(e.getMessage(), e);
        }
    }

    public static int getSupportedCharCode(int charCode) {
        if (charCode < 578) {
            return (charCode <= 32) ? 33 : charCode;
        }
        if (charCode <= 591) {
            charCode = 592;
        } else if (charCode >= 880 && charCode <= 909) {
            charCode = 910;
        } else if (charCode >= 930 && charCode <= 1424) {
            charCode = 1425;
        } else if (charCode >= 1466 && charCode <= 1599) {
            charCode = 1600;
        } else if (charCode == 1631) {
            charCode = 1632;
        } else if (charCode == 1806) {
            charCode = 1807;
        } else if (charCode == 1867 || charCode == 1868) {
            charCode = 1869;
        } else if (charCode >= 1902 && charCode <= 1919) {
            charCode = 1920;
        } else if (charCode >= 1970 && charCode <= 2304) {
            charCode = 2305;
        } else if (charCode >= 2362 && charCode <= 2564) {
            charCode = 2565;
        } else if (charCode >= 2571 && charCode <= 4255) {
            charCode = 4256;
        } else if (charCode >= 4294 && charCode <= 4351) {
            charCode = 4352;
        } else if (charCode >= 4442 && charCode <= 4446) {
            charCode = 4447;
        } else if (charCode >= 4515 && charCode <= 4519) {
            charCode = 4520;
        } else if (charCode >= 4602 && charCode <= 5120) {
            charCode = 5121;
        } else if (charCode >= 5751 && charCode <= 8207) {
            charCode = 8208;
        } else if (charCode >= 8266 && charCode <= 9311) {
            charCode = 9312;
        } else if (charCode >= 9840 && charCode <= 13311) {
            charCode = 13312;
        } else if (charCode >= 19894 && charCode <= 19967) {
            charCode = 19968;
        } else if (charCode >= 40892 && charCode <= 44031) {
            charCode = 44032;
        } else if (charCode >= 55204 && charCode <= 64466) {
            charCode = 64467;
        } else if (charCode >= 64832 && charCode <= 64847) {
            charCode = 64848;
        } else if (charCode == 64912 || charCode == 64913) {
            charCode = 64914;
        } else if (charCode >= 64968 && charCode <= 65071) {
            charCode = 65072;
        } else if (charCode >= 65107 && charCode <= 65141) {
            charCode = 65142;
        } else if (charCode >= 65277 && charCode <= 65280) {
            charCode = 65281;
        } else if (charCode >= 65471) {
            charCode = 65470;
        }
        return charCode;
    }

    static {
        POINT_BASE = new BigDecimal(65536);
        TeradataSplitUtils.nullable = false;
        TeradataSplitUtils.defaultValue = null;
    }
}
