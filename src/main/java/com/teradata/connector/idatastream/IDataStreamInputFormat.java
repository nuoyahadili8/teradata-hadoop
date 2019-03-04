package com.teradata.connector.idatastream;

import com.teradata.connector.common.exception.ConnectorException;
import com.teradata.connector.common.utils.ConnectorConfiguration;
import com.teradata.connector.common.utils.HadoopConfigurationUtils;
import com.teradata.connector.idatastream.utils.IDataStreamPlugInConfiguration;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.ArrayList;
import java.util.List;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.InputFormat;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;


/**
 * @author Administrator
 */
public class IDataStreamInputFormat extends InputFormat<LongWritable, IDataStreamByteArray> {
    protected static final int SPLIT_LOCATIONS_MAX = 6;

    @Override
    public RecordReader<LongWritable, IDataStreamByteArray> createRecordReader(final InputSplit split, final TaskAttemptContext context) throws IOException, InterruptedException {
        return new IDataStreamRecordReader();
    }

    @Override
    public List<InputSplit> getSplits(final JobContext context) throws IOException, InterruptedException {
        final int numMappers = ConnectorConfiguration.getNumMappers(context.getConfiguration());
        final String[] locations = HadoopConfigurationUtils.getAllActiveHosts(context);
        final List<InputSplit> splits = new ArrayList<InputSplit>();
        for (int i = 0; i < numMappers; ++i) {
            final IDataStreamInputSplit split = new IDataStreamInputSplit(HadoopConfigurationUtils.selectUniqueActiveHosts(locations, 6));
            splits.add(split);
        }
        return splits;
    }

    public static class IDataStreamInputSplit extends InputSplit implements Writable {
        private String[] locations;

        public IDataStreamInputSplit() {
            this.locations = null;
        }

        public IDataStreamInputSplit(final String[] locations) {
            this.locations = null;
            this.locations = locations;
        }

        @Override
        public void readFields(final DataInput arg0) throws IOException {
        }

        @Override
        public void write(final DataOutput arg0) throws IOException {
        }

        @Override
        public long getLength() throws IOException, InterruptedException {
            return 0L;
        }

        @Override
        public String[] getLocations() throws IOException, InterruptedException {
            return (this.locations == null) ? new String[0] : this.locations;
        }
    }

    public class IDataStreamRecordReader extends RecordReader<LongWritable, IDataStreamByteArray> {
        private Log logger;
        private boolean receivedEOD;
        private int rowStart;
        private int rowEnd;
        private int bytesInBuffer;
        private long resultCount;
        private long start_timestamp;
        private long end_timestamp;
        private byte[] buffer;
        private IDataStreamConnection connection;
        private InputStream inputStream;
        private IDataStreamByteArray curValue;
        private final int BUFFER_SIZE = 130000;

        public IDataStreamRecordReader() {
            this.logger = LogFactory.getLog((Class) IDataStreamRecordReader.class);
            this.receivedEOD = false;
            this.rowStart = 0;
            this.rowEnd = 0;
            this.bytesInBuffer = 0;
            this.resultCount = 0L;
            this.start_timestamp = 0L;
            this.end_timestamp = 0L;
            this.buffer = null;
            this.connection = null;
            this.inputStream = null;
            this.curValue = null;
            this.buffer = new byte[130000];
        }

        @Override
        public void close() throws IOException {
            try {
                if (!this.connection.isClosed()) {
                    this.connection.disconnect();
                }
            } catch (Exception e) {
                throw new ConnectorException(e.getMessage());
            } finally {
                this.end_timestamp = System.currentTimeMillis();
                this.logger.info((Object) ("recordreader class " + this.getClass().getName() + "close time is:  " + this.end_timestamp));
                this.logger.info((Object) ("the total elapsed time of recordreader " + this.getClass().getName() + (this.end_timestamp - this.start_timestamp) / 1000L + "s"));
            }
        }

        @Override
        public LongWritable getCurrentKey() throws IOException, InterruptedException {
            return new LongWritable(this.resultCount);
        }

        @Override
        public IDataStreamByteArray getCurrentValue() throws IOException, InterruptedException {
            return this.curValue;
        }

        @Override
        public float getProgress() throws IOException, InterruptedException {
            return 0.0f;
        }

        @Override
        public void initialize(final InputSplit split, final TaskAttemptContext context) throws IOException, InterruptedException {
            this.start_timestamp = System.currentTimeMillis();
            this.logger.info((Object) ("recordreader class " + this.getClass().getName() + "initialize time is:  " + this.start_timestamp));
            final Configuration configuration = context.getConfiguration();
            final String host = IDataStreamPlugInConfiguration.getInputSocketHost(configuration);
            final int port = Integer.parseInt(IDataStreamPlugInConfiguration.getInputSocketPort(configuration));
            (this.connection = new IDataStreamConnection(host, port)).connect();
            this.inputStream = this.connection.getInputStream();
            boolean leserver = false;
            try {
                final int leserverint = Integer.parseInt(IDataStreamPlugInConfiguration.getInputLittleEndianServer(configuration));
                leserver = (leserverint == 1);
            } catch (Exception e) {
                final String leserverstr = IDataStreamPlugInConfiguration.getInputLittleEndianServer(configuration);
                leserver = leserverstr.toLowerCase().equals("yes");
            }
            this.connection.setLEServer(leserver);
        }

        @Override
        public boolean nextKeyValue() throws IOException, InterruptedException {
            if (this.receivedEOD && this.rowEnd == this.bytesInBuffer - 1) {
                return false;
            }
            if (this.bytesInBuffer == 0) {
                this.bytesInBuffer = this.inputStream.read(this.buffer, 0, 130000);
                this.logger.debug((Object) ("Init read - Read in " + this.bytesInBuffer + " (" + String.format("%02x%02x", this.buffer[0], this.buffer[1]) + ")"));
                if (new String(this.buffer, this.bytesInBuffer - 3, 3).equals("EOD")) {
                    this.receivedEOD = true;
                    this.logger.debug((Object) "Init read - Received EOD");
                    this.bytesInBuffer -= 3;
                    if (this.bytesInBuffer == 0) {
                        return false;
                    }
                }
            }
            int rowLength = 0;
            this.rowStart = ((this.rowEnd == 0) ? 0 : (this.rowEnd + 1));
            if (this.connection.isLEServer()) {
                rowLength = ByteBuffer.wrap(this.buffer, this.rowStart, 2).order(ByteOrder.LITTLE_ENDIAN).getShort();
            } else {
                rowLength = ByteBuffer.wrap(this.buffer, this.rowStart, 2).getShort();
            }
            this.logger.debug((Object) ("Got row #" + this.resultCount + " of length " + rowLength + " (offset " + this.rowStart + "  " + String.format("%02x%02x", this.buffer[this.rowStart], this.buffer[this.rowStart + 1]) + ")"));
            this.rowStart += 2;
            this.rowEnd = this.rowStart + rowLength;
            if (this.rowEnd + 2 >= this.bytesInBuffer && !this.receivedEOD) {
                this.logger.debug((Object) ("Row start is " + this.rowStart + " row end is " + this.rowEnd + " bytesInBuffer is " + this.bytesInBuffer));
                this.rowStart -= 2;
                final int leftovers = this.bytesInBuffer - this.rowStart;
                System.arraycopy(this.buffer, this.rowStart, this.buffer, 0, leftovers);
                final int bytesRead = this.inputStream.read(this.buffer, leftovers, 130000 - leftovers);
                this.logger.debug((Object) ("Read in " + bytesRead + " bytes"));
                this.bytesInBuffer = leftovers + bytesRead;
                if (new String(this.buffer, this.bytesInBuffer - 3, 3).equals("EOD")) {
                    this.receivedEOD = true;
                    this.bytesInBuffer -= 3;
                    if (this.bytesInBuffer == 0) {
                        return false;
                    }
                }
                this.rowStart = 0;
                if (this.connection.isLEServer()) {
                    rowLength = ByteBuffer.wrap(this.buffer, this.rowStart, 2).order(ByteOrder.LITTLE_ENDIAN).getShort();
                } else {
                    rowLength = ByteBuffer.wrap(this.buffer, this.rowStart, 2).getShort();
                }
                this.rowStart += 2;
                this.rowEnd = this.rowStart + rowLength;
                this.logger.debug((Object) ("Got row #" + this.resultCount + " of length " + rowLength));
            } else if (this.rowEnd + 2 < this.bytesInBuffer || this.receivedEOD) {
            }
            (this.curValue = new IDataStreamByteArray(rowLength)).setByteArray(this.buffer, this.rowStart, rowLength);
            ++this.resultCount;
            return true;
        }
    }
}
