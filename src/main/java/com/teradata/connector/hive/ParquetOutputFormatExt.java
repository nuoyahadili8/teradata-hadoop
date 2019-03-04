package com.teradata.connector.hive;

import org.apache.parquet.hadoop.api.*;
import org.apache.parquet.hadoop.*;

/**
 * @author Administrator
 */
public class ParquetOutputFormatExt {
    public ParquetOutputFormat getParquetOutputFormat(final WriteSupport ws) {
        return new ParquetOutputFormat(ws);
    }

    public parquet.hadoop.ParquetOutputFormat getParquetOutputFormat(final parquet.hadoop.api.WriteSupport ws) {
        return new parquet.hadoop.ParquetOutputFormat(ws);
    }
}
