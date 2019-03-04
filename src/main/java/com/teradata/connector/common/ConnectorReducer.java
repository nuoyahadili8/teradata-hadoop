package com.teradata.connector.common;

import com.teradata.connector.common.exception.ConnectorException;
import com.teradata.connector.common.utils.ConnectorConfiguration;
import java.io.IOException;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.mapreduce.Reducer;

/**
 * @author Administrator
 */
public class ConnectorReducer extends Reducer<WritableComparable, ConnectorRecord, ConnectorRecord, Writable> {
    @Override
    public void run(final Context context) throws IOException, InterruptedException {
        final String jobReducer = ConnectorConfiguration.getJobReducer(context.getConfiguration());
        if (ConnectorConfiguration.getJobReducer(context.getConfiguration()).isEmpty()) {
            setup(context);
            while (context.nextKey()) {
                reduce((WritableComparable) context.getCurrentKey(), context.getValues(), (Context) context);
            }
            cleanup(context);
            return;
        }
        try {
            ((Reducer) Class.forName(ConnectorConfiguration.getJobReducer(context.getConfiguration())).newInstance()).run(context);
        } catch (ClassNotFoundException e) {
            throw new ConnectorException(e.getMessage(), e);
        } catch (InstantiationException e2) {
            throw new ConnectorException(e2.getMessage(), e2);
        } catch (IllegalAccessException e22) {
            throw new ConnectorException(e22.getMessage(), e22);
        }
    }

    @Override
    protected void reduce(final WritableComparable key, final Iterable<ConnectorRecord> values, final Context context) throws IOException, InterruptedException {
        for (ConnectorRecord value : values) {
            context.write(value, key);
        }
    }
}
