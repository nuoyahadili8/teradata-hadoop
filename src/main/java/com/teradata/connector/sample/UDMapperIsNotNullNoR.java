package com.teradata.connector.sample;

import com.teradata.connector.common.ConnectorRecord;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.mapreduce.Mapper;


/**
 * @author Administrator
 */
public class UDMapperIsNotNullNoR extends Mapper<WritableComparable, ConnectorRecord, ConnectorRecord, WritableComparable> {
    private int objectLen;
    private LongWritable outKey;

    public UDMapperIsNotNullNoR() {
        this.objectLen = -1;
        this.outKey = new LongWritable();
    }

    @Override
    protected void map(final WritableComparable key, final ConnectorRecord value, final Context context) throws IOException, InterruptedException {
        if (this.objectLen == -1) {
            this.objectLen = value.getAllObject().length;
        }
        for (int i = 0; i < this.objectLen; ++i) {
            if (value.get(i) == null) {
                return;
            }
        }
        context.write(value, this.outKey);
    }
}
