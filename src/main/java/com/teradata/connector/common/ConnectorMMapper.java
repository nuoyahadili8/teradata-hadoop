package com.teradata.connector.common;

import com.teradata.connector.common.exception.ConnectorException;
import com.teradata.connector.common.utils.ConnectorConfiguration;
import com.teradata.connector.common.utils.ConnectorStringUtils;

import java.io.IOException;
import java.lang.reflect.Field;
import java.net.InetAddress;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.mapred.Task;
import org.apache.hadoop.mapreduce.Counter;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.Mapper;


public class ConnectorMMapper extends Mapper<WritableComparable, ConnectorRecord, ConnectorRecord, Writable> {
    private static Log logger = LogFactory.getLog((Class) ConnectorMMapper.class);
    private boolean startupKeepAliveThread;

    public ConnectorMMapper() {
        this.startupKeepAliveThread = true;
    }

    public void setStartupKeepAliveThread(final boolean val) {
        this.startupKeepAliveThread = val;
    }

    @Override
    protected void setup(final Mapper.Context context) throws IOException, InterruptedException {
        if (ConnectorMMapper.logger.isDebugEnabled()) {
            final InputSplit inputSplit = context.getInputSplit();
            final Field[] fields2;
            final Field[] fields = fields2 = inputSplit.getClass().getFields();
            for (final Field field : fields2) {
                try {
                    ConnectorMMapper.logger.debug((Object) (field.getName() + " is: " + field.get(inputSplit)));
                } catch (IllegalArgumentException e) {
                    e.printStackTrace();
                } catch (IllegalAccessException e2) {
                    e2.printStackTrace();
                }
            }
            ConnectorMMapper.logger.debug((Object) ("task tracker ip address is " + InetAddress.getLocalHost().getHostAddress()));
        }
    }

    @Override
    public void run(final Mapper.Context context) throws IOException, InterruptedException {
        final Configuration conf = context.getConfiguration();
        final long timeout = Integer.parseInt(conf.get("mapred.task.timeout"));
        if (timeout != 0L && this.startupKeepAliveThread) {
            final KeepAliveThread updateCounterThread = new KeepAliveThread(context);
            updateCounterThread.setDaemon(true);
            updateCounterThread.start();
            ConnectorMMapper.logger.info((Object) "keep-alive thread started");
        }
        final String jobMapper = ConnectorConfiguration.getJobMapper(conf);
        if (!jobMapper.isEmpty()) {
            try {
                final Mapper<WritableComparable, ConnectorRecord, ConnectorRecord, Writable> mapperClass = (Mapper<WritableComparable, ConnectorRecord, ConnectorRecord, Writable>) Class.forName(jobMapper).newInstance();
                mapperClass.run(context);
                return;
            } catch (InstantiationException e) {
                throw new ConnectorException(e.getMessage(), e);
            } catch (IllegalAccessException e2) {
                throw new ConnectorException(e2.getMessage(), e2);
            } catch (ClassNotFoundException e3) {
                throw new ConnectorException(e3.getMessage(), e3);
            }
        }
        this.setup(context);
        while (context.nextKeyValue()) {
            this.map((WritableComparable) context.getCurrentKey(), (ConnectorRecord) context.getCurrentValue(), context);
        }
        this.cleanup(context);
    }

    @Override
    protected void map(final WritableComparable key, final ConnectorRecord value, final Mapper.Context context) throws IOException, InterruptedException {
        context.write((Object) value, (Object) key);
    }

    @Override
    protected void cleanup(final Mapper.Context context) throws IOException, InterruptedException {
        if (ConnectorMMapper.logger.isDebugEnabled()) {
            Counter counter = context.getCounter((Enum) Task.Counter.MAP_INPUT_BYTES);
            ConnectorMMapper.logger.debug((Object) ("     " + counter.getDisplayName() + "=" + counter.getValue()));
            counter = context.getCounter((Enum) Task.Counter.MAP_INPUT_RECORDS);
            ConnectorMMapper.logger.debug((Object) ("     " + counter.getDisplayName() + "=" + counter.getValue()));
            counter = context.getCounter((Enum) Task.Counter.MAP_OUTPUT_BYTES);
            ConnectorMMapper.logger.debug((Object) ("     " + counter.getDisplayName() + "=" + counter.getValue()));
            counter = context.getCounter((Enum) Task.Counter.MAP_OUTPUT_RECORDS);
            ConnectorMMapper.logger.debug((Object) ("     " + counter.getDisplayName() + "=" + counter.getValue()));
            counter = context.getCounter((Enum) Task.Counter.MAP_OUTPUT_MATERIALIZED_BYTES);
            ConnectorMMapper.logger.debug((Object) ("     " + counter.getDisplayName() + "=" + counter.getValue()));
            counter = context.getCounter((Enum) Task.Counter.MAP_SKIPPED_RECORDS);
            ConnectorMMapper.logger.debug((Object) ("     " + counter.getDisplayName() + "=" + counter.getValue()));
        }
    }

    public class KeepAliveThread extends Thread {
        private Mapper.Context context;
        private final long UPDATE_FREQ = 3000L;

        public KeepAliveThread(final Mapper.Context context) {
            super("Teradata Connector Keep-Alive Thread");
            this.context = context;
        }

        @Override
        public void run() {
            while (true) {
                this.context.progress();
                try {
                    Thread.sleep(3000L);
                } catch (InterruptedException e) {
                    ConnectorMMapper.logger.error((Object) ConnectorStringUtils.getExceptionStack(e));
                }
            }
        }
    }
}
