package com.teradata.connector.sample;

import com.teradata.connector.common.ConnectorRecord;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.mapreduce.Mapper;


public class UDMapperIsNotNull extends Mapper<WritableComparable, ConnectorRecord, WritableComparable, ConnectorRecord> {
    private int objectLen;
    private LongWritable outKey;

    public UDMapperIsNotNull() {
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
        this.outKey.set((long) (int) value.get(0));
        context.write(this.outKey, value);
    }
}
