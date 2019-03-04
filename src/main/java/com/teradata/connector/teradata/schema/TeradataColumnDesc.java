package com.teradata.connector.teradata.schema;

import com.teradata.connector.common.converter.ConnectorDataTypeDefinition;
import com.teradata.connector.common.utils.ConnectorSchemaUtils;

/**
 * @author Administrator
 */
public class TeradataColumnDesc {
    public static final int BYTE_DEFAULT = 1;
    public static final int CHAR_DEFAULT = 1;
    public static final int DECIMAL_PRECISION_DEFAULT = 5;
    public static final int DECIMAL_SCALE_DEFAULT = 0;
    public static final int INTERVAL_M_DEFAULT = 6;
    public static final int INTERVAL_M_MAX = 6;
    public static final String INTERVAL_NM_SECOND = "SECOND";
    public static final String INTERVAL_NM_SEPARATOR = "TO";
    public static final int INTERVAL_N_DEFAULT = 2;
    public static final int INTERVAL_N_MAX = 4;
    public static final int KBYTES = 1024;
    public static final String NUMBER_DATA_TYPE = "NUMBER";
    public static final int NUMBER_PRECISION_MAX = 38;
    public static final int NUMBER_SCALE_DEFAULT = 0;
    public static final int TIME_SCALE_DEFAULT = 6;
    public static final int TIME_SCALE_MAX = 6;
    public static final int UNBOUNDED_NUMBER_SCALE_DEFAULT = 10;
    private boolean caseSensitive = false;
    private int charType = 0;
    private String className = "";
    private String format = "";
    private long length = 0;
    private String name = "";
    private boolean nullable = true;
    private int precision = 0;
    private int scale = 0;
    private int timeScale = -1;
    private int type = 0;
    private String typeName = "";

    public TeradataColumnDesc(){

    }

    public TeradataColumnDesc(TeradataColumnDesc inputColumn) {
        this.name = inputColumn.getName();
        this.typeName = inputColumn.getTypeName();
        this.className = inputColumn.getClassName();
        this.format = inputColumn.getFormat();
        this.type = inputColumn.getType();
        this.precision = inputColumn.getPrecision();
        this.scale = inputColumn.getScale();
        this.length = inputColumn.getLength();
        this.timeScale = inputColumn.getScale();
        this.nullable = inputColumn.isNullable();
        this.charType = inputColumn.getCharType();
        this.caseSensitive = inputColumn.isCaseSensitive();
    }

    public void setName(String name_) {
        this.name = name_;
    }

    public void setTypeName(String typeName_) {
        this.typeName = typeName_.toUpperCase();
    }

    public void setClassName(String className_) {
        this.className = className_;
    }

    public void setFormat(String format_) {
        this.format = format_;
    }

    public void setType(int type_) {
        this.type = type_;
    }

    public void setPrecision(int precision_) {
        if (precision_ >= 0) {
            this.precision = precision_;
        }
    }

    public void setScale(int scale_) {
        if (scale_ >= 0) {
            this.scale = scale_;
            this.timeScale = scale_;
        }
    }

    public void setLength(long length_) {
        if (length_ >= 0) {
            this.length = length_;
        }
    }

    public void setNullable(boolean nullable_) {
        this.nullable = nullable_;
    }

    public void setCaseSensitive(boolean caseSensitive_) {
        this.caseSensitive = caseSensitive_;
    }

    public String getName() {
        return this.name;
    }

    public String getTypeName() {
        return this.typeName;
    }

    public String getClassName() {
        return this.className;
    }

    public String getFormat() {
        return this.format;
    }

    public int getType() {
        return this.type;
    }

    public int getPrecision() {
        switch (this.type) {
            case 2:
            case ConnectorDataTypeDefinition.TYPE_DECIMAL /*3*/:
                return this.precision == 0 ? 5 : this.precision;
            default:
                return this.precision;
        }
    }

    public int getScale() {
        switch (this.type) {
            case ConnectorDataTypeDefinition.TYPE_TIME /*92*/:
            case ConnectorDataTypeDefinition.TYPE_TIMESTAMP /*93*/:
                if (this.timeScale < 0 || this.timeScale > 6) {
                    return 6;
                }
                return this.timeScale;
            case ConnectorDataTypeDefinition.TYPE_PERIOD /*2002*/:
                if (this.typeName.contains("PERIOD") && this.typeName.contains("TIME")) {
                    if (this.timeScale < 0 || this.timeScale > 6) {
                        return 6;
                    }
                    return this.timeScale;
                }
                break;
        }
        return this.scale;
    }

    public long getLength() {
        switch (this.type) {
            case ConnectorDataTypeDefinition.TYPE_BINARY /*-2*/:
                if (this.length != 0) {
                    return this.length;
                }
                return 1;
            case 1:
                if (this.length != 0) {
                    return this.length;
                }
                return 1;
            default:
                return this.length;
        }
    }

    public boolean isNullable() {
        return this.nullable;
    }

    public boolean isCaseSensitive() {
        return this.caseSensitive;
    }

    public void setCharType(int charType) {
        this.charType = charType;
    }

    public int getCharType() {
        return this.charType;
    }

    public String getLobLengthInKMG() {
        String unit = "";
        long len = this.length;
        if (len > 1024 && len % 1024 == 0) {
            len /= 1024;
            unit = "K";
        }
        if (len > 1024 && len % 1024 == 0) {
            len /= 1024;
            unit = "M";
        }
        if (len > 1024 && len % 1024 == 0) {
            len /= 1024;
            unit = "G";
        }
        return "" + len + unit;
    }

    public String getTypeString() {
        return getTypeStringWithoutNullability() + (this.nullable ? " NULL" : " NOT NULL");
    }

    public String getTypeString4Using(String charset, int timeScale, int timeStampScale) {
        int timeZoneLen = 0;
        int strTimeOrIntervalMultiplier;
        switch (this.type) {
            case 0:
                return getTypeStringWithoutNullability();
            case 1:
            case 12:
            case 1111:
                int strCharOrVarcharMultiplier = 1;
                if ("UTF16".equalsIgnoreCase(charset)) {
                    strCharOrVarcharMultiplier = 2;
                } else if ("UTF8".equalsIgnoreCase(charset)) {
                    strCharOrVarcharMultiplier = 3;
                }
                return "VARCHAR(" + (((long) strCharOrVarcharMultiplier) * getLength()) + ")";
            case 2:
                if (this.precision == 40 && this.length == 47 && this.scale == 0) {
                    this.scale = 10;
                }
                return "DECIMAL (38, " + this.scale + ")";
            case ConnectorDataTypeDefinition.TYPE_DECIMAL /*3*/:
                return "DECIMAL (38, " + this.scale + ")";
            case ConnectorDataTypeDefinition.TYPE_TIME /*92*/:
                strTimeOrIntervalMultiplier = 1;
                if ("UTF16".equalsIgnoreCase(charset)) {
                    strTimeOrIntervalMultiplier = 2;
                }
                int tnanoLength = timeScale > 0 ? (timeScale + 8) + 1 : 8;
                if (ConnectorSchemaUtils.isTimeWithTimeZoneType(this.typeName)) {
                    timeZoneLen = 6;
                }
                return "CHAR(" + ((tnanoLength + timeZoneLen) * strTimeOrIntervalMultiplier) + ")";
            case ConnectorDataTypeDefinition.TYPE_TIMESTAMP /*93*/:
                strTimeOrIntervalMultiplier = 1;
                if ("UTF16".equalsIgnoreCase(charset)) {
                    strTimeOrIntervalMultiplier = 2;
                }
                int tsnanoLength = timeStampScale > 0 ? (timeStampScale + 19) + 1 : 19;
                if (ConnectorSchemaUtils.isTimestampWithTimeZoneType(this.typeName)) {
                    timeZoneLen = 6;
                }
                return "CHAR(" + ((tsnanoLength + timeZoneLen) * strTimeOrIntervalMultiplier) + ")";
            case ConnectorDataTypeDefinition.TYPE_CLOB /*2005*/:
                return "CLOB(" + getLobLengthInKMG() + ")";
            default:
                return getTypeStringWithoutNullability();
        }
    }

    public String getTypeStringWithoutNullability() {
        String string = "";
        switch (this.type) {
            case ConnectorDataTypeDefinition.TYPE_TINYINT /*-6*/:
                return string + "BYTEINT";
            case ConnectorDataTypeDefinition.TYPE_BIGINT /*-5*/:
                return string + "BIGINT";
            case ConnectorDataTypeDefinition.TYPE_VARBINARY /*-3*/:
                return string + "VARBYTE(" + this.length + ")";
            case ConnectorDataTypeDefinition.TYPE_BINARY /*-2*/:
                return string + "BYTE(" + this.length + ")";
            case ConnectorDataTypeDefinition.TYPE_LONGVARCHAR /*-1*/:
                string = string + "LONG VARCHAR";
                if (this.charType == 1) {
                    string = string + " CHARACTER SET LATIN";
                } else if (this.charType == 2) {
                    string = string + " CHARACTER SET UNICODE";
                }
                if (this.caseSensitive) {
                    return string + " CASESPECIFIC";
                }
                return string + " NOT CASESPECIFIC";
            case 1:
                string = string + "CHAR(" + this.length + ")";
                if (this.charType == 1) {
                    string = string + " CHARACTER SET LATIN";
                } else if (this.charType == 2) {
                    string = string + " CHARACTER SET UNICODE";
                }
                if (this.caseSensitive) {
                    return string + " CASESPECIFIC";
                }
                return string + " NOT CASESPECIFIC";
            case 2:
                if (!this.typeName.equalsIgnoreCase(NUMBER_DATA_TYPE)) {
                    return string + "NUMERIC(" + this.precision + ", " + this.scale + ")";
                }
                if (this.precision <= 38) {
                    return string + "NUMBER(" + this.precision + ", " + this.scale + ")";
                }
                if (this.scale > 0) {
                    return string + "NUMBER(*, " + this.scale + ")";
                }
                return string + "NUMBER(*)";
            case ConnectorDataTypeDefinition.TYPE_DECIMAL /*3*/:
                return string + "DECIMAL(" + this.precision + ", " + this.scale + ")";
            case 4:
                return string + "INTEGER";
            case 5:
                return string + "SMALLINT";
            case 6:
                return string + "FLOAT";
            case ConnectorDataTypeDefinition.TYPE_REAL /*7*/:
                return string + "REAL";
            case ConnectorDataTypeDefinition.TYPE_DOUBLE /*8*/:
                return string + "DOUBLE PRECISION";
            case 12:
                string = string + "VARCHAR(" + this.length + ")";
                if (this.charType == 1) {
                    string = string + " CHARACTER SET LATIN";
                } else if (this.charType == 2) {
                    string = string + " CHARACTER SET UNICODE";
                }
                if (this.caseSensitive) {
                    return string + " CASESPECIFIC";
                }
                return string + " NOT CASESPECIFIC";
            case ConnectorDataTypeDefinition.TYPE_DATE /*91*/:
                return string + "DATE";
            case ConnectorDataTypeDefinition.TYPE_TIME /*92*/:
                string = string + "TIME";
                if (this.scale >= 0 && this.scale < 6) {
                    string = string + "(" + this.scale + ")";
                }
                if (ConnectorSchemaUtils.isTimeWithTimeZoneType(this.typeName)) {
                    string = string + " WITH TIME ZONE ";
                }
                if (isEmptyString(this.format)) {
                    return string;
                }
                return string + " FORMAT '" + this.format + "'";
            case ConnectorDataTypeDefinition.TYPE_TIMESTAMP /*93*/:
                string = string + "TIMESTAMP";
                if (this.scale >= 0 && this.scale < 6) {
                    string = string + "(" + this.scale + ")";
                }
                if (ConnectorSchemaUtils.isTimestampWithTimeZoneType(this.typeName)) {
                    string = string + " WITH TIME ZONE ";
                }
                if (isEmptyString(this.format)) {
                    return string;
                }
                return string + " FORMAT '" + this.format + "'";
            case 1111:
                if (!this.typeName.contains("INTERVAL")) {
                    return string;
                }
                int index = this.typeName.indexOf(INTERVAL_NM_SEPARATOR);
                int n;
                String formatString;
                int pos;
                if (index >= 0) {
                    String typeN = this.typeName.substring(0, index);
                    String typeM = this.typeName.substring(INTERVAL_NM_SEPARATOR.length() + index);
                    n = 2;
                    int m = 6;
                    formatString = this.format.trim();
                    pos = formatString.length();
                    if (typeM.contains(INTERVAL_NM_SECOND) && pos > 3 && formatString.charAt(pos - 1) == ')') {
                        try {
                            m = Integer.parseInt("" + formatString.charAt(pos - 2));
                            formatString = formatString.substring(0, pos - 3);
                        } catch (NumberFormatException e) {
                            m = 6;
                        }
                    }
                    pos = formatString.indexOf(40);
                    if (pos > 0) {
                        try {
                            n = Integer.parseInt("" + formatString.charAt(pos + 1));
                        } catch (NumberFormatException e2) {
                            n = 2;
                        }
                    }
                    string = string + typeN;
                    if (n != 2) {
                        string = string + "(" + n + ") ";
                    }
                    string = string + INTERVAL_NM_SEPARATOR + typeM;
                    if (m != 6) {
                        return string + " (" + m + ")";
                    }
                    return string;
                }
                n = 2;
                formatString = this.format.trim();
                pos = formatString.indexOf(40);
                if (pos > 0) {
                    try {
                        n = Integer.parseInt("" + formatString.charAt(pos + 1));
                    } catch (NumberFormatException e3) {
                        n = 2;
                    }
                }
                string = string + this.typeName;
                if (n != 2) {
                    return string + "(" + n + ")";
                }
                return string;
            case ConnectorDataTypeDefinition.TYPE_PERIOD /*2002*/:
                if (!this.typeName.contains("PERIOD")) {
                    return string + this.typeName;
                }
                if (this.typeName.contains("TIMESTAMP")) {
                    string = string + "PERIOD(TIMESTAMP";
                    if (this.scale >= 0 && this.scale < 6) {
                        string = string + "(" + this.scale + ")";
                    }
                    return string + ")";
                } else if (this.typeName.contains("TIME")) {
                    string = string + "PERIOD(TIME";
                    if (this.scale >= 0 && this.scale < 6) {
                        string = string + "(" + this.scale + ")";
                    }
                    return string + ")";
                } else if (this.typeName.contains("DATE")) {
                    return string + "PERIOD(DATE)";
                } else {
                    return string + this.typeName;
                }
            case ConnectorDataTypeDefinition.TYPE_ARRAY_TD /*2003*/:
                return string + this.typeName;
            case ConnectorDataTypeDefinition.TYPE_BLOB /*2004*/:
                return string + "BLOB(" + getLobLengthInKMG() + ")";
            case ConnectorDataTypeDefinition.TYPE_CLOB /*2005*/:
                string = string + "CLOB(" + getLobLengthInKMG() + ")";
                if (this.charType == 1) {
                    return string + " CHARACTER SET LATIN";
                }
                if (this.charType == 2) {
                    return string + " CHARACTER SET UNICODE";
                }
                return string;
            default:
                return string + this.typeName;
        }
    }

    /* Access modifiers changed, original: protected */
    public boolean isEmptyString(String text) {
        return text == null || text.isEmpty();
    }
}