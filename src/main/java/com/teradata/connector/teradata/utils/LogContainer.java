package com.teradata.connector.teradata.utils;

import com.teradata.connector.common.exception.ConnectorException;
import com.teradata.connector.common.exception.ConnectorException.ErrorMessage;
import com.teradata.connector.common.utils.ConnectorConfiguration;

import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;


/**
 * @author Administrator
 */
public class LogContainer {
    private List<String> lstException;
    private static final LogContainer holder;

    public LogContainer() {
        this.lstException = new ArrayList<String>();
    }

    private List<String> getData() {
        return this.lstException;
    }

    private void setData(final List<String> data) {
        this.lstException = data;
    }

    public int listSize() {
        return this.lstException.size();
    }

    public static LogContainer getInstance() {
        return LogContainer.holder;
    }

    public void addLogToList(final String logExceptionRow, final Configuration configuration) throws ConnectorException {
        final List<String> lstException = this.getData();
        final int logRowLimit = ConnectorConfiguration.getLogRowsCount(configuration);
        final int numMappers = ConnectorConfiguration.getNumMappers(configuration);
        final int limitRowsQuo = logRowLimit / numMappers;
        final int limitRowsRem = logRowLimit % numMappers;
        if (this.listSize() < limitRowsQuo + limitRowsRem) {
            lstException.add(logExceptionRow);
            this.setData(lstException);
            return;
        }
        throw new ConnectorException(String.format("Number of rows in HDFS log exceeded limit", "-limitrows", "expected  more "));
    }

    public void writeHdfsLogs(final Configuration configuration) throws ConnectorException {
        final String path = ConnectorConfiguration.getHdfsLoggingPath(configuration) + TeradataPlugInConfiguration.getOutputTable(configuration) + System.currentTimeMillis() + ".log";
        try {
            final FileSystem fs = FileSystem.get(configuration);
            final Path filenamePath = new Path(path);
            if (fs.exists(filenamePath)) {
                fs.delete(filenamePath, true);
            }
            final FSDataOutputStream fin = fs.create(filenamePath);
            for (final String lstStr : this.lstException) {
                fin.writeChars(lstStr + "\n");
            }
            fin.close();
        } catch (Exception e) {
            throw new ConnectorException(String.format("Error while writing HDFS log", new Object[0]));
        }
    }

    static {
        holder = new LogContainer();
    }
}
