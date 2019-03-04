package com.teradata.connector.sample.plugin.utils;

import com.teradata.connector.common.converter.ConnectorDataTypeDefinition;
import com.teradata.connector.common.exception.ConnectorException;
import com.teradata.connector.common.exception.ConnectorException.ErrorCode;
import com.teradata.connector.common.utils.ConnectorConfiguration;
import com.teradata.connector.sample.CommonDBInputFormat;
import com.teradata.connector.sample.CommonDBInputFormat.CommonDBInputSplit;
import java.math.BigDecimal;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Time;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import org.apache.hadoop.conf.Configuration;


/**
 * @author Administrator
 */
public class CommonDBSplitUtils {
    private static final BigDecimal POINT_BASE = new BigDecimal(65536);
    private static final int NUM_CHARS = 8;

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
        BigDecimal denominatorValue = CommonDBSplitUtils.POINT_BASE;
        BigDecimal charBigDecimal = BigDecimal.ZERO;
        for (int len = Math.min(value.length(), 8), i = 0; i < len; ++i) {
            final int codePoint = value.codePointAt(i);
            charBigDecimal = charBigDecimal.add(bigDecimalDivision(new BigDecimal(codePoint), denominatorValue));
            denominatorValue = denominatorValue.multiply(CommonDBSplitUtils.POINT_BASE);
        }
        return charBigDecimal;
    }

    private static String bigDecimalToString(final BigDecimal bigDecimal) {
        BigDecimal currentValue = bigDecimal.stripTrailingZeros();
        final StringBuilder stringBuilder = new StringBuilder();
        for (int numConverted = 0; numConverted < 8; ++numConverted) {
            currentValue = currentValue.multiply(CommonDBSplitUtils.POINT_BASE);
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

    public static List<CommonDBInputFormat.CommonDBInputSplit> getSplitsByTimeType(final Configuration configuration, final String splitByColumn, final ResultSet resultSetMinMaxValues) throws ConnectorException {
        try {
            final int sqlDataType = resultSetMinMaxValues.getMetaData().getColumnType(1);
            final long minValue = dateToLong(resultSetMinMaxValues, 1, sqlDataType);
            final long maxValue = dateToLong(resultSetMinMaxValues, 2, sqlDataType);
            final String lowerBoundary = CommonDBSchemaUtils.quoteFieldName(splitByColumn) + " >= ";
            final String upperBoundary = CommonDBSchemaUtils.quoteFieldName(splitByColumn) + " < ";
            final String splitSql = CommonDBConfiguration.getInputSplitSql(configuration);
            final int numSplits = ConnectorConfiguration.getNumMappers(configuration);
            final List<Long> splitPoints = getTimeSplitPoints(numSplits, minValue, maxValue);
            final List<CommonDBInputFormat.CommonDBInputSplit> splits = new ArrayList<CommonDBInputFormat.CommonDBInputSplit>();
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
                    splits.add(new CommonDBInputFormat.CommonDBInputSplit(splitSql + " " + lowerBoundary + "'" + startDate + "'"));
                    ++i;
                } else {
                    ++i;
                    highPoint = splitPoints.get(i);
                    endDate = longToDate(highPoint, sqlDataType);
                    splits.add(new CommonDBInputFormat.CommonDBInputSplit(splitSql + " " + lowerBoundary + "'" + startDate + "' AND " + upperBoundary + "'" + endDate + "'"));
                }
                startDate = endDate;
            }
            return splits;
        } catch (SQLException e) {
            throw new ConnectorException(e.getMessage(), e);
        }
    }

    public static List<CommonDBInputFormat.CommonDBInputSplit> getSplitsByBooleanType(final Configuration conf, final String splitByColumn, final boolean minValue, final boolean maxValue) {
        final List<CommonDBInputFormat.CommonDBInputSplit> splits = new ArrayList<CommonDBInputFormat.CommonDBInputSplit>();
        final String splitSql = CommonDBConfiguration.getInputSplitSql(conf);
        if (!minValue) {
            splits.add(new CommonDBInputFormat.CommonDBInputSplit(splitSql + " " + CommonDBSchemaUtils.quoteFieldName(splitByColumn) + " = FALSE"));
        }
        if (maxValue) {
            splits.add(new CommonDBInputFormat.CommonDBInputSplit(splitSql + " " + CommonDBSchemaUtils.quoteFieldName(splitByColumn) + " = TRUE"));
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

    public static List<CommonDBInputFormat.CommonDBInputSplit> getSplitsByDoubleType(final Configuration configuration, final String splitByColumn, final double minValue, final double maxValue) {
        final List<CommonDBInputFormat.CommonDBInputSplit> splits = new ArrayList<CommonDBInputFormat.CommonDBInputSplit>();
        final String splitSql = CommonDBConfiguration.getInputSplitSql(configuration);
        final double minDoubleValue = minValue;
        final double maxDoubleValue = maxValue;
        final long numSplits = ConnectorConfiguration.getNumMappers(configuration);
        final String lowerBoundary = CommonDBSchemaUtils.quoteFieldName(splitByColumn) + " >= ";
        final String upperBoundary = CommonDBSchemaUtils.quoteFieldName(splitByColumn) + " < ";
        final List<Double> splitPoints = getDoubleSplitPoints(numSplits, minDoubleValue, maxDoubleValue);
        double highPoint;
        double lowPoint = highPoint = splitPoints.get(0);
        int i = 0;
        while (i < splitPoints.size()) {
            if (i == splitPoints.size() - 1) {
                splits.add(new CommonDBInputFormat.CommonDBInputSplit(splitSql + " " + lowerBoundary + Double.toString(lowPoint)));
                ++i;
            } else {
                ++i;
                highPoint = splitPoints.get(i);
                splits.add(new CommonDBInputFormat.CommonDBInputSplit(splitSql + " " + lowerBoundary + Double.toString(lowPoint) + " AND " + upperBoundary + Double.toString(highPoint)));
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

    public static List<CommonDBInputFormat.CommonDBInputSplit> getSplitsByIntType(final Configuration configuration, final String splitByColumn, final long minValue, final long maxValue) {
        final long minIntValue = minValue;
        final long maxIntValue = maxValue;
        final String lowerBoundary = CommonDBSchemaUtils.quoteFieldName(splitByColumn) + " >= ";
        final String upperBoundary = CommonDBSchemaUtils.quoteFieldName(splitByColumn) + " < ";
        final String splitSql = CommonDBConfiguration.getInputSplitSql(configuration);
        final int numSplits = ConnectorConfiguration.getNumMappers(configuration);
        final List<Long> splitPoints = getIntegerSplitPoints(numSplits, minIntValue, maxIntValue);
        final List<CommonDBInputFormat.CommonDBInputSplit> splits = new ArrayList<CommonDBInputFormat.CommonDBInputSplit>();
        long highPoint;
        long lowPoint = highPoint = splitPoints.get(0);
        int i = 0;
        while (i < splitPoints.size()) {
            if (i == splitPoints.size() - 1) {
                splits.add(new CommonDBInputFormat.CommonDBInputSplit(splitSql + " " + lowerBoundary + Long.toString(lowPoint)));
                ++i;
            } else {
                ++i;
                highPoint = splitPoints.get(i);
                splits.add(new CommonDBInputFormat.CommonDBInputSplit(splitSql + " " + lowerBoundary + Long.toString(lowPoint) + " AND " + upperBoundary + Long.toString(highPoint)));
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

    public static List<CommonDBInputFormat.CommonDBInputSplit> getSplitsByDecimalType(final Configuration configuration, final String splitByColumn, final BigDecimal minValue, final BigDecimal maxValue) {
        final BigDecimal minDecimalValue = minValue;
        final BigDecimal maxDecimalValue = maxValue;
        final String lowerBoundary = CommonDBSchemaUtils.quoteFieldName(splitByColumn) + " >= ";
        final String upperBoundary = CommonDBSchemaUtils.quoteFieldName(splitByColumn) + " < ";
        final String splitSql = CommonDBConfiguration.getInputSplitSql(configuration);
        final long numSplits = ConnectorConfiguration.getNumMappers(configuration);
        final List<BigDecimal> splitPoints = getBigDecimalSplitPoints(numSplits, minDecimalValue, maxDecimalValue);
        final List<CommonDBInputFormat.CommonDBInputSplit> splits = new ArrayList<CommonDBInputFormat.CommonDBInputSplit>();
        BigDecimal highPoint;
        BigDecimal lowPoint = highPoint = splitPoints.get(0);
        int i = 0;
        while (i < splitPoints.size()) {
            if (i == splitPoints.size() - 1) {
                splits.add(new CommonDBInputFormat.CommonDBInputSplit(splitSql + " " + lowerBoundary + lowPoint.toString()));
                ++i;
            } else {
                ++i;
                highPoint = splitPoints.get(i);
                splits.add(new CommonDBInputFormat.CommonDBInputSplit(splitSql + " " + lowerBoundary + lowPoint.toString() + " AND " + upperBoundary + highPoint.toString()));
            }
            lowPoint = highPoint;
        }
        return splits;
    }

    private static List<String> getStringSplitPoints(final int numSplits, final String minString, final String maxString, final String commonPrefix) {
        final BigDecimal minVal = stringToBigDecimal(minString);
        final BigDecimal maxVal = stringToBigDecimal(maxString);
        final List<BigDecimal> splitPoints = getBigDecimalSplitPoints(numSplits, minVal, maxVal);
        final List<String> splitStrings = new ArrayList<String>();
        for (final BigDecimal BigDecimalValue : splitPoints) {
            splitStrings.add(commonPrefix + bigDecimalToString(BigDecimalValue));
        }
        return splitStrings;
    }

    public static List<CommonDBInputSplit> getSplitsByStringType(final Configuration configuration, final String splitByColumn, final String minValue, final String maxValue) {
        String minString = minValue;
        String maxString = maxValue;
        final String splitSql = CommonDBConfiguration.getInputSplitSql(configuration);
        final int numSplits = ConnectorConfiguration.getNumMappers(configuration);
        final String lowClausePrefix = CommonDBSchemaUtils.quoteFieldName(splitByColumn) + " >= '";
        final String highClausePrefix = CommonDBSchemaUtils.quoteFieldName(splitByColumn) + " < '";
        int maxPrefixLength;
        int commonLength;
        for (maxPrefixLength = Math.min(minString.length(), maxString.length()), commonLength = 0; commonLength < maxPrefixLength; ++commonLength) {
            final char c1 = minString.charAt(commonLength);
            final char c2 = maxString.charAt(commonLength);
            if (c1 != c2) {
                break;
            }
        }
        final String commonPrefix = minString.substring(0, commonLength);
        minString = minString.substring(commonLength);
        maxString = maxString.substring(commonLength);
        final List<String> splitStrings = getStringSplitPoints(numSplits, minString, maxString, commonPrefix);
        final List<CommonDBInputSplit> splits = new ArrayList<CommonDBInputFormat.CommonDBInputSplit>();
        String endString;
        String startString = endString = splitStrings.get(0);
        int i = 0;
        while (i < splitStrings.size()) {
            if (i == splitStrings.size() - 1) {
                splits.add(new CommonDBInputSplit(splitSql + " " + lowClausePrefix + startString.replace("'", "''") + "'"));
                ++i;
            } else {
                ++i;
                endString = splitStrings.get(i);
                splits.add(new CommonDBInputSplit(splitSql + " " + lowClausePrefix + startString.replace("'", "''") + "' AND " + highClausePrefix + endString.replace("'", "''") + "'"));
            }
            startString = endString;
        }
        return splits;
    }

    public static List<CommonDBInputSplit> getSplitsByColumnType(final Configuration configuration, final String splitbyColumn, final ResultSet resultSetMinMaxValues) throws ConnectorException {
        try {
            switch (resultSetMinMaxValues.getMetaData().getColumnType(1)) {
                case -7:
                case ConnectorDataTypeDefinition.TYPE_BOOLEAN /*16*/:
                    return getSplitsByBooleanType(configuration, splitbyColumn, resultSetMinMaxValues.getBoolean(1), resultSetMinMaxValues.getBoolean(2));
                case ConnectorDataTypeDefinition.TYPE_TINYINT /*-6*/:
                case ConnectorDataTypeDefinition.TYPE_BIGINT /*-5*/:
                case 4:
                case 5:
                    return getSplitsByIntType(configuration, splitbyColumn, resultSetMinMaxValues.getLong(1), resultSetMinMaxValues.getLong(2));
                case ConnectorDataTypeDefinition.TYPE_LONGVARCHAR /*-1*/:
                case 1:
                case 12:
                    return getSplitsByStringType(configuration, splitbyColumn, resultSetMinMaxValues.getString(1), resultSetMinMaxValues.getString(2));
                case 2:
                case ConnectorDataTypeDefinition.TYPE_DECIMAL /*3*/:
                    return getSplitsByDecimalType(configuration, splitbyColumn, resultSetMinMaxValues.getBigDecimal(1), resultSetMinMaxValues.getBigDecimal(2));
                case 6:
                case ConnectorDataTypeDefinition.TYPE_REAL /*7*/:
                case 8:
                    return getSplitsByDoubleType(configuration, splitbyColumn, resultSetMinMaxValues.getDouble(1), resultSetMinMaxValues.getDouble(2));
                case ConnectorDataTypeDefinition.TYPE_DATE /*91*/:
                case ConnectorDataTypeDefinition.TYPE_TIME /*92*/:
                case ConnectorDataTypeDefinition.TYPE_TIMESTAMP /*93*/:
                    return getSplitsByTimeType(configuration, splitbyColumn, resultSetMinMaxValues);
                default:
                    throw new ConnectorException((int) ErrorCode.SPLIT_COLUMN_DATATYPE_UNSUPPORTED);
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
}
