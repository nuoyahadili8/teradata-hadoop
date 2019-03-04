package com.teradata.connector.idatastream.schema;

public class IDataStreamColumnDesc {
    private Integer JDBCtype;
    private Integer precision;
    private Integer scale;

    public IDataStreamColumnDesc(final Integer JDBCtype, final Integer precision, final Integer scale) {
        this.JDBCtype = JDBCtype;
        this.precision = precision;
        this.scale = scale;
    }

    public Integer getJDBCtype() {
        return this.JDBCtype;
    }

    public Integer getPrecision() {
        return this.precision;
    }

    public Integer getScale() {
        return this.scale;
    }
}
