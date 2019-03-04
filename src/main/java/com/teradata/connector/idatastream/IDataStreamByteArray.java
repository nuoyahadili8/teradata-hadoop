package com.teradata.connector.idatastream;

import com.teradata.connector.common.exception.ConnectorException;
import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Arrays;
import org.apache.hadoop.io.Writable;

/**
 * @author Administrator
 */
public class IDataStreamByteArray implements Writable {
    int datalen;
    byte[] data;

    public IDataStreamByteArray(final int datalen) {
        this.datalen = 0;
        this.data = null;
        this.datalen = datalen;
        this.data = new byte[datalen];
    }

    public void setByteArray(final byte[] data, final int offset, final int length) throws ConnectorException {
        if (length != this.datalen) {
            throw new ConnectorException(1000);
        }
        System.arraycopy(data, offset, this.data, 0, this.datalen);
    }

    public byte[] getByteArray() {
        return Arrays.copyOf(this.data, this.data.length);
    }

    @Override
    public void readFields(final DataInput in) throws IOException {
        in.readFully(this.data, 0, this.datalen);
    }

    @Override
    public void write(final DataOutput out) throws IOException {
        out.write(this.data);
    }
}
