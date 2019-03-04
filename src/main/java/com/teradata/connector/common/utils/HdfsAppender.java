package com.teradata.connector.common.utils;

import com.teradata.connector.common.exception.ConnectorException;
import com.teradata.connector.common.exception.ConnectorException.ErrorCode;

import java.io.IOException;
import java.io.OutputStream;
import java.io.Writer;
import java.net.URI;
import java.util.Enumeration;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.log4j.Appender;
import org.apache.log4j.ConsoleAppender;
import org.apache.log4j.Layout;
import org.apache.log4j.LogManager;
import org.apache.log4j.PatternLayout;
import org.apache.log4j.WriterAppender;
import org.apache.log4j.helpers.LogLog;
import org.apache.log4j.helpers.QuietWriter;


/**
 * @author Administrator
 */
public class HdfsAppender extends WriterAppender {

    private String fileName;
    private final String DEFAULT_FORMAT = "%d{MM/dd HH:mm:ss} %5p %C{1}: %m%n";

    public HdfsAppender(final String filename) {
        this.fileName = filename;
    }

    public String getFileName() {
        return this.fileName;
    }

    private HdfsAppender initializeAppender() throws ConnectorException {
        Layout pl = null;
        final Enumeration<Appender> appender = (Enumeration<Appender>) LogManager.getRootLogger().getAllAppenders();
        while (appender.hasMoreElements()) {
            final Appender a = appender.nextElement();
            if (a instanceof ConsoleAppender && a.getLayout() != null) {
                pl = a.getLayout();
            }
        }
        if (pl == null) {
            final PatternLayout playout = new PatternLayout();
            playout.setConversionPattern("%d{MM/dd HH:mm:ss} %5p %C{1}: %m%n");
            pl = (Layout) playout;
        }
        this.layout = pl;
        this.openHDFSFile(this.fileName);
        return this;
    }

    private void openHDFSFile(final String filename) throws ConnectorException {
        try {
            final URI uri = new URI(this.fileName);
            if (uri.getScheme() != null && !uri.getScheme().equals("hdfs") && !uri.getScheme().equals("maprfs")) {
                throw new ConnectorException(15006);
            }
            final Path path = new Path(this.fileName);
            final FileSystem fs = path.getFileSystem(new Configuration());
            if (fs.exists(path)) {
                throw new ConnectorException(15007);
            }
            final FSDataOutputStream outputStream = fs.create(path);
            final Writer writer = this.createWriter((OutputStream) outputStream);
            this.qw = new QuietWriter(writer, this.errorHandler);
        } catch (Exception e) {
            throw new ConnectorException(e.getMessage(), e);
        }
    }

    protected void closeFile() {
        if (this.qw != null) {
            try {
                this.qw.close();
            } catch (IOException e) {
                LogLog.error(ConnectorStringUtils.getExceptionStack(e));
            } finally {
                this.qw = null;
            }
        }
    }

    @Override
    protected void reset() {
        this.closeFile();
        this.fileName = null;
        super.reset();
    }

    public static void addHDFSAppender(final String fileName) throws ConnectorException {
        final HdfsAppender appender = new HdfsAppender(fileName).initializeAppender();
        if (appender.qw != null) {
            LogManager.getRootLogger().addAppender((Appender) appender);
        }
    }
}
