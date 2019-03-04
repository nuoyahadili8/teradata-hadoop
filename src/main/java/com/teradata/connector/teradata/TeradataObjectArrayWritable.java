package com.teradata.connector.teradata;

import com.teradata.connector.common.ConnectorRecord;
import com.teradata.connector.common.converter.ConnectorDataTypeConverter;
import com.teradata.connector.common.utils.ConnectorSchemaUtils;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.sql.*;
import java.util.Calendar;
import java.util.HashMap;
import java.util.Map;

import org.apache.hadoop.mapreduce.lib.db.DBWritable;

/**
 * @author Administrator
 */
public class TeradataObjectArrayWritable extends ConnectorRecord implements DBWritable {
    protected int objectCount;
    protected int[] nullJdbcTypes;
    protected int[] nullJdbcScales;
    protected int nullJdbcTypeCount;
    protected Object[] nullDefaultValues;
    protected int nullDefaultValueCount;
    protected int[] recordTypes;
    protected Calendar nullCal;
    private Map<Integer, Calendar> calendarCache;

    public TeradataObjectArrayWritable() {
        this.nullJdbcTypes = null;
        this.nullJdbcScales = null;
        this.nullJdbcTypeCount = 0;
        this.nullDefaultValues = null;
        this.nullDefaultValueCount = 0;
        this.recordTypes = null;
        this.nullCal = Calendar.getInstance();
        this.record = new Object[0];
        this.objectCount = 0;
        this.calendarCache = new HashMap<Integer, Calendar>();
    }

    public TeradataObjectArrayWritable(final int objectCount) {
        this.nullJdbcTypes = null;
        this.nullJdbcScales = null;
        this.nullJdbcTypeCount = 0;
        this.nullDefaultValues = null;
        this.nullDefaultValueCount = 0;
        this.recordTypes = null;
        this.nullCal = Calendar.getInstance();
        if (objectCount > 0) {
            this.record = new Object[objectCount];
            this.objectCount = objectCount;
        }
        this.calendarCache = new HashMap<Integer, Calendar>();
    }

    private Calendar getCachedCalendarObject(final int index) {
        Calendar cal = this.calendarCache.get(index);
        if (cal == null) {
            cal = Calendar.getInstance();
            this.calendarCache.put(index, cal);
        }
        return cal;
    }

    public Object[] getObjects() {
        return this.record;
    }

    public void setObjects(final Object[] objects) {
        this.record = objects;
        this.objectCount = objects.length;
    }

    @Override
    public Object get(final int position) {
        return this.record[position];
    }

    @Override
    public void set(final int position, final Object object) {
        this.record[position] = object;
    }

    public int[] getNullJdbcTypes() {
        return this.nullJdbcTypes;
    }

    public void setNullJdbcTypes(final int[] jdbcTypes) {
        if (jdbcTypes != null) {
            this.nullJdbcTypes = jdbcTypes;
            this.nullJdbcTypeCount = jdbcTypes.length;
        }
    }

    public int[] getNullJdbcScales() {
        return this.nullJdbcScales;
    }

    public void setNullJdbcScales(final int[] jdbcScales) {
        if (jdbcScales != null) {
            this.nullJdbcScales = jdbcScales;
        }
    }

    public Object[] getNullDefaultValues() {
        return this.nullDefaultValues;
    }

    public void setNullDefaultValues(final Object[] defaultValues) {
        if (defaultValues != null) {
            this.nullDefaultValues = defaultValues;
            this.nullDefaultValueCount = defaultValues.length;
        }
    }

    public void setRecordTypes(final String[] typeNames) {
        this.recordTypes = new int[typeNames.length];
        for (int i = 0; i < this.recordTypes.length; ++i) {
            if (ConnectorSchemaUtils.isTimeWithTimeZoneType(typeNames[i])) {
                this.recordTypes[i] = 1885;
            } else if (ConnectorSchemaUtils.isTimestampWithTimeZoneType(typeNames[i])) {
                this.recordTypes[i] = 1886;
            } else if (ConnectorSchemaUtils.isDateType(typeNames[i])) {
                this.recordTypes[i] = 91;
            } else if (ConnectorSchemaUtils.isTimeType(typeNames[i])) {
                this.recordTypes[i] = 92;
            } else {
                this.recordTypes[i] = 1882;
            }
        }
    }

    public int[] getRecordTypes() {
        return this.recordTypes;
    }

    @Override
    public void readFields(final ResultSet resultSet) throws SQLException {
        final ResultSetMetaData metadata = resultSet.getMetaData();
        this.objectCount = metadata.getColumnCount();
        if (this.objectCount > 0) {
            this.record = new Object[this.objectCount];
        }
        for (int i = 0; i < this.objectCount; ++i) {
            if (this.recordTypes[i] == 1885) {
                final Calendar calendar = this.getCachedCalendarObject(i + 1);
                final Time ts = resultSet.getTime(i + 1, calendar);
                if (ts == null) {
                    this.record[i] = null;
                } else {
                    long milliSec = ts.getTime();
                    milliSec = ConnectorDataTypeConverter.convertMillisecFromSourceToDefaultTZ(milliSec, calendar.getTimeZone());
                    calendar.setTimeInMillis(milliSec);
                    this.record[i] = calendar;
                }
            } else if (this.recordTypes[i] == 1886) {
                final Calendar calendar = this.getCachedCalendarObject(i + 1);
                final Timestamp ts2 = resultSet.getTimestamp(i + 1, calendar);
                if (ts2 == null) {
                    this.record[i] = null;
                } else {
                    long milliSec = ts2.getTime();
                    milliSec = ConnectorDataTypeConverter.convertMillisecFromSourceToDefaultTZ(milliSec, calendar.getTimeZone());
                    calendar.setTimeInMillis(milliSec);
                    this.record[i] = calendar;
                }
            } else if (this.recordTypes[i] == 92 && resultSet.getTimestamp(i + 1).getNanos() > 0) {
                this.record[i] = resultSet.getTimestamp(i + 1);
            } else {
                this.record[i] = resultSet.getObject(i + 1);
            }
        }
    }

    @Override
    public void write(final PreparedStatement statement) throws SQLException {
        for (int i = 0; i < this.objectCount; ++i) {
            if (this.record[i] == null) {
                if (this.nullDefaultValues != null && this.nullDefaultValueCount > i) {
                    statement.setObject(i + 1, this.nullDefaultValues[i]);
                } else if (this.nullJdbcTypes != null && this.nullJdbcTypeCount > i) {
                    switch (this.nullJdbcTypes[i]) {
                        case 2:
                        case 3: {
                            statement.setObject(i + 1, null, this.nullJdbcTypes[i], this.nullJdbcScales[i]);
                            continue;
                        }
                        case 1111:
                        case 2002:
                        case 2003: {
                            statement.setObject(i + 1, null, 12);
                            continue;
                        }
                        case 91:
                        case 92:
                        case 93: {
                            if (this.recordTypes[i] == 1886) {
                                statement.setTimestamp(i + 1, null, this.nullCal);
                                continue;
                            }
                            if (this.recordTypes[i] == 1885) {
                                statement.setTime(i + 1, null, this.nullCal);
                                continue;
                            }
                            if (this.recordTypes[i] == 91) {
                                statement.setNull(i + 1, this.nullJdbcTypes[i]);
                                continue;
                            }
                            break;
                        }
                    }
                    statement.setNull(i + 1, this.nullJdbcTypes[i]);
                } else {
                    statement.setObject(i + 1, "");
                }
            } else if (this.recordTypes[i] == 1886) {
                final Timestamp timestamp = new Timestamp(0L);
                final Calendar cal = (Calendar) this.record[i];
                long milliSec = cal.getTimeInMillis();
                milliSec = ConnectorDataTypeConverter.convertMillisecFromDefaultToTargetTZ(milliSec, cal.getTimeZone());
                timestamp.setTime(milliSec);
                statement.setTimestamp(i + 1, timestamp, cal);
            } else if (this.recordTypes[i] == 1885) {
                final Time time = new Time(0L);
                final Calendar cal = (Calendar) this.record[i];
                long milliSec = cal.getTimeInMillis();
                milliSec = ConnectorDataTypeConverter.convertMillisecFromDefaultToTargetTZ(milliSec, cal.getTimeZone());
                time.setTime(milliSec);
                statement.setTime(i + 1, time, cal);
            } else if (this.recordTypes[i] == 91) {
                if (this.record[i] instanceof String) {
                    statement.setDate(i + 1, Date.valueOf(this.record[i].toString()));
                } else {
                    statement.setObject(i + 1, this.record[i], 91);
                }
            } else {
                statement.setObject(i + 1, this.record[i]);
            }
        }
    }

    @Override
    public void readFields(final DataInput in) throws IOException {
        super.readFields(in);
        final int len = in.readInt();
        this.recordTypes = new int[len];
        for (int i = 0; i < len; ++i) {
            this.recordTypes[i] = in.readInt();
        }
    }

    @Override
    public void write(final DataOutput out) throws IOException {
        super.write(out);
        if (this.recordTypes == null) {
            out.writeInt(0);
        } else {
            out.writeInt(this.recordTypes.length);
            for (int i = 0; i < this.recordTypes.length; ++i) {
                out.writeInt(this.recordTypes[i]);
            }
        }
    }
}
