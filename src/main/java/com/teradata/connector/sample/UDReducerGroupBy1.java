package com.teradata.connector.sample;

import com.teradata.connector.common.ConnectorRecord;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.mapreduce.Reducer;


/**
 * @author Administrator
 */
public class UDReducerGroupBy1 extends Reducer<WritableComparable, ConnectorRecord, ConnectorRecord, Writable> {
    private ConnectorRecord outputRec;

    public UDReducerGroupBy1() {
        this.outputRec = new ConnectorRecord(2);
    }

    @Override
    protected void reduce(final WritableComparable key, final Iterable<ConnectorRecord> values, final Context context) throws IOException, InterruptedException {
        float sum = 0.0f;
        boolean isDouble = false;
        for (final ConnectorRecord value : values) {
            if (value.get(1) instanceof Float) {
                sum += (float) value.get(1);
            } else {
                sum += (float) (double) value.get(1);
                isDouble = true;
            }
        }
        this.outputRec.set(0, (int) ((LongWritable) key).get());
        if (isDouble) {
            this.outputRec.set(1, (double) sum);
        } else {
            this.outputRec.set(1, sum);
        }
        context.write(this.outputRec, NullWritable.get());
    }
}
