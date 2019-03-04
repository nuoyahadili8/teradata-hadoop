package com.teradata.connector.idatastream.serde;

import com.teradata.connector.common.ConnectorRecord;
import com.teradata.connector.common.ConnectorSerDe;
import com.teradata.connector.common.converter.ConnectorDataTypeDefinition;
import com.teradata.connector.common.exception.ConnectorException;
import com.teradata.connector.common.exception.ConnectorException.ErrorCode;
import com.teradata.connector.common.utils.ConnectorConfiguration;
import com.teradata.connector.common.utils.ConnectorConfiguration.direction;
import com.teradata.connector.idatastream.IDataStreamByteArray;
import com.teradata.connector.idatastream.schema.IDataStreamColumnDesc;
import com.teradata.connector.idatastream.utils.IDataStreamPlugInConfiguration;
import com.teradata.connector.idatastream.utils.IDataStreamUtils;
import java.io.ByteArrayInputStream;
import java.io.InputStream;
import java.io.UnsupportedEncodingException;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.sql.SQLException;
import java.util.Arrays;
import javax.sql.rowset.serial.SerialBlob;
import javax.sql.rowset.serial.SerialClob;
import javax.sql.rowset.serial.SerialException;
import org.apache.commons.lang.ArrayUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.JobContext;

/**
 * @author Administrator
 */
public class IDataStreamSerDe implements ConnectorSerDe {
    private static byte[] nullbytes = null;
    private IDataStreamColumnDesc[] colDescs = null;
    private byte[] indBytes = null;
    private int inputFieldCount = 0;
    private boolean leserver = true;
    private Log logger = LogFactory.getLog(IDataStreamSerDe.class);
    private boolean[] nullCols = null;
    private int numIndBytes = 0;
    private int outputFieldCount = 0;
    private ConnectorRecord record = null;

    private static class IDataStreamSerialBlob extends SerialBlob {
        private static final long serialVersionUID = 1;
        private byte[] data;

        public IDataStreamSerialBlob(byte[] bytes) throws SerialException, SQLException {
            super(bytes);
            this.data = new byte[bytes.length];
            System.arraycopy(bytes, 0, this.data, 0, bytes.length);
        }

        @Override
        public InputStream getBinaryStream() throws SerialException {
            return new ByteArrayInputStream(this.data);
        }
    }

    private static class IDataStreamSerialClob extends SerialClob {
        private static final long serialVersionUID = 1;
        private String data;

        public IDataStreamSerialClob(char[] bytes) throws SerialException, SQLException {
            super(bytes);
            this.data = new String(bytes);
        }

        @Override
        public InputStream getAsciiStream() throws SerialException, SQLException {
            try {
                return new ByteArrayInputStream(this.data.getBytes("UTF-8"));
            } catch (UnsupportedEncodingException e) {
                e.printStackTrace();
                return null;
            }
        }
    }

    @Override
    public void initialize(JobContext context, direction direction) throws ConnectorException {
        Configuration configuration = context.getConfiguration();
        int i;
        if (direction.equals(ConnectorConfiguration.direction.input)) {
            String[] inputFieldTypes = IDataStreamPlugInConfiguration.getInputFieldTypesArray(configuration);
            this.inputFieldCount = inputFieldTypes.length;
            this.colDescs = new IDataStreamColumnDesc[this.inputFieldCount];
            for (i = 0; i < this.inputFieldCount; i++) {
                this.colDescs[i] = IDataStreamUtils.getIDataStreamColumnDescFromString(inputFieldTypes[i]);
            }
            this.record = new ConnectorRecord(this.inputFieldCount);
            this.numIndBytes = (int) Math.ceil(((double) this.inputFieldCount) / 8.0d);
            this.nullCols = new boolean[((this.numIndBytes * 8) + 1)];
            try {
                boolean z;
                if (Integer.parseInt(IDataStreamPlugInConfiguration.getInputLittleEndianServer(configuration)) == 1) {
                    z = true;
                } else {
                    z = false;
                }
                this.leserver = z;
                return;
            } catch (Exception e) {
                this.leserver = IDataStreamPlugInConfiguration.getInputLittleEndianServer(configuration).toLowerCase().equals("yes");
                return;
            }
        }
        String[] outputFieldTypes = IDataStreamPlugInConfiguration.getOutputFieldTypesArray(configuration);
        this.outputFieldCount = outputFieldTypes.length;
        this.colDescs = new IDataStreamColumnDesc[this.outputFieldCount];
        for (i = 0; i < this.outputFieldCount; i++) {
            this.colDescs[i] = IDataStreamUtils.getIDataStreamColumnDescFromString(outputFieldTypes[i]);
        }
        this.numIndBytes = (int) Math.ceil(((double) this.outputFieldCount) / 8.0d);
        this.indBytes = new byte[this.numIndBytes];
        try {
            this.leserver = Integer.parseInt(IDataStreamPlugInConfiguration.getOutputLittleEndianServer(configuration)) == 1;
        } catch (Exception e2) {
            this.leserver = IDataStreamPlugInConfiguration.getOutputLittleEndianServer(configuration).toLowerCase().equals("yes");
        }
    }

    public Writable serialize(ConnectorRecord connectorRecord) throws ConnectorException {
        Long totallen = Long.valueOf(0);
        Long len = Long.valueOf(0);
        Integer bytelen = Integer.valueOf(0);
        int i = 0;
        while (i < this.numIndBytes) {
            this.indBytes[i] = (byte) 0;
            int j = 0;
            while (j < 8) {
                if ((i * 8) + j < this.outputFieldCount && connectorRecord.get((i * 8) + j) == null) {
                    byte[] bArr = this.indBytes;
                    bArr[i] = (byte) (bArr[i] | (1 << (7 - j)));
                }
                j++;
            }
            i++;
        }
        totallen = Long.valueOf(Long.valueOf(0).longValue() + ((long) this.numIndBytes));
        for (i = 0; i < this.outputFieldCount; i++) {
            bytelen = getByteLength(this.colDescs[i]);
            if (bytelen.intValue() == 0) {
                if (connectorRecord.get(i) != null) {
                    switch (this.colDescs[i].getJDBCtype().intValue()) {
                        case ConnectorDataTypeDefinition.TYPE_VARBINARY /*-3*/:
                            totallen = Long.valueOf(totallen.longValue() + ((long) (((byte[]) connectorRecord.get(i)).length + 2)));
                            break;
                        case ConnectorDataTypeDefinition.TYPE_BLOB /*2004*/:
                            totallen = Long.valueOf(totallen.longValue() + ((long) (((byte[]) connectorRecord.get(i)).length + 8)));
                            break;
                        case ConnectorDataTypeDefinition.TYPE_CLOB /*2005*/:
                            totallen = Long.valueOf(totallen.longValue() + ((long) (((String) connectorRecord.get(i)).getBytes().length + 8)));
                            break;
                        default:
                            totallen = Long.valueOf(totallen.longValue() + ((long) (((String) connectorRecord.get(i)).getBytes().length + 2)));
                            break;
                    }
                }
                switch (this.colDescs[i].getJDBCtype().intValue()) {
                    case ConnectorDataTypeDefinition.TYPE_BLOB /*2004*/:
                    case ConnectorDataTypeDefinition.TYPE_CLOB /*2005*/:
                        totallen = Long.valueOf(totallen.longValue() + 8);
                        break;
                    default:
                        totallen = Long.valueOf(totallen.longValue() + 2);
                        break;
                }
            }
            totallen = Long.valueOf(totallen.longValue() + ((long) bytelen.intValue()));
        }
        ByteBuffer lbb = ByteBuffer.allocate((totallen.intValue() + 2) + 1);
        if (this.leserver) {
            lbb.order(ByteOrder.LITTLE_ENDIAN);
        }
        lbb.putShort(totallen.shortValue());
        lbb.put(this.indBytes);
        i = 0;
        while (i < this.outputFieldCount) {
            if (getByteLength(this.colDescs[i]).intValue() == 0) {
                if (connectorRecord.get(i) != null) {
                    switch (this.colDescs[i].getJDBCtype().intValue()) {
                        case ConnectorDataTypeDefinition.TYPE_VARBINARY /*-3*/:
                            len = Long.valueOf((long) ((byte[]) connectorRecord.get(i)).length);
                            break;
                        case ConnectorDataTypeDefinition.TYPE_BLOB /*2004*/:
                            len = Long.valueOf((long) ((byte[]) connectorRecord.get(i)).length);
                            break;
                        case ConnectorDataTypeDefinition.TYPE_CLOB /*2005*/:
                            len = Long.valueOf((long) ((String) connectorRecord.get(i)).getBytes().length);
                            break;
                        default:
                            len = Long.valueOf((long) ((String) connectorRecord.get(i)).getBytes().length);
                            break;
                    }
                }
                len = Long.valueOf(0);
                if (this.colDescs[i].getJDBCtype().intValue() == ConnectorDataTypeDefinition.TYPE_CLOB || this.colDescs[i].getJDBCtype().intValue() == ConnectorDataTypeDefinition.TYPE_BLOB) {
                    lbb.putLong(len.longValue());
                } else {
                    lbb.putShort(len.shortValue());
                }
            }
            getBytesFromObject(lbb, this.colDescs[i], connectorRecord.get(i), this.leserver);
            i++;
        }
        lbb.put((byte) 10);
        IDataStreamByteArray barray = new IDataStreamByteArray(lbb.array().length);
        barray.setByteArray(lbb.array(), 0, lbb.array().length);
        return barray;
    }

    @Override
    public ConnectorRecord deserialize(Writable writable) throws ConnectorException {
        byte[] buffer = ((IDataStreamByteArray) writable).getByteArray();
        String nulColS = "";
        for (int i = 0; i < this.numIndBytes; i++) {
            for (int j = 0; j < 8; j++) {
                if (((buffer[0 + i] >> j) & 1) == 1) {
                    this.nullCols[(i * 8) + (7 - j)] = true;
                    nulColS = nulColS + ((i * 8) + (7 - j)) + ",";
                } else {
                    this.nullCols[(i * 8) + (7 - j)] = false;
                }
            }
        }
        this.logger.debug("Cols " + nulColS + " are null");
        int rowStart = 0 + this.numIndBytes;
        int columnStart = rowStart;
        int columnEnd = rowStart;
        int columnIndex = 0;
        while (columnIndex < this.inputFieldCount) {
            columnStart = columnEnd;
            int bytelen = getByteLength(this.colDescs[columnIndex]).intValue();
            if (bytelen == 0) {
                if (this.colDescs[columnIndex].getJDBCtype().intValue() == ConnectorDataTypeDefinition.TYPE_BLOB || this.colDescs[columnIndex].getJDBCtype().intValue() == ConnectorDataTypeDefinition.TYPE_CLOB) {
                    if (this.leserver) {
                        bytelen = (int) ByteBuffer.wrap(buffer, columnStart, 8).order(ByteOrder.LITTLE_ENDIAN).getLong();
                    } else {
                        bytelen = (int) ByteBuffer.wrap(buffer, columnStart, 8).getLong();
                    }
                    columnStart += 8;
                } else {
                    if (this.leserver) {
                        bytelen = ByteBuffer.wrap(buffer, columnStart, 2).order(ByteOrder.LITTLE_ENDIAN).getShort();
                    } else {
                        bytelen = ByteBuffer.wrap(buffer, columnStart, 2).getShort();
                    }
                    columnStart += 2;
                }
            }
            this.logger.debug("Bytelen for col " + columnIndex + " is " + bytelen);
            if (bytelen == 0) {
                columnEnd = columnStart;
            } else {
                columnEnd = columnStart + bytelen;
            }
            if (bytelen == 0 || this.nullCols[columnIndex]) {
                this.record.set(columnIndex, null);
            } else {
                this.logger.debug("Deser from offset " + columnStart + " to " + columnEnd);
                this.record.set(columnIndex, getObjectFromBytes(this.colDescs[columnIndex], Arrays.copyOfRange(buffer, columnStart, columnEnd), this.leserver));
            }
            columnIndex++;
        }
        return this.record;
    }

    public static Integer getByteLength(IDataStreamColumnDesc desc) {
        switch (desc.getJDBCtype().intValue()) {
            case -16:
            case ConnectorDataTypeDefinition.TYPE_VARBINARY /*-3*/:
            case 12:
            case ConnectorDataTypeDefinition.TYPE_BLOB /*2004*/:
            case ConnectorDataTypeDefinition.TYPE_CLOB /*2005*/:
                return Integer.valueOf(0);
            case ConnectorDataTypeDefinition.TYPE_TINYINT /*-6*/:
                return Integer.valueOf(1);
            case ConnectorDataTypeDefinition.TYPE_BIGINT /*-5*/:
                return Integer.valueOf(8);
            case ConnectorDataTypeDefinition.TYPE_BINARY /*-2*/:
            case 1:
                break;
            case 2:
            case ConnectorDataTypeDefinition.TYPE_DECIMAL /*3*/:
                if (1 <= desc.getPrecision().intValue() && desc.getPrecision().intValue() <= 2) {
                    return Integer.valueOf(1);
                }
                if (3 <= desc.getPrecision().intValue() && desc.getPrecision().intValue() <= 4) {
                    return Integer.valueOf(2);
                }
                if (5 <= desc.getPrecision().intValue() && desc.getPrecision().intValue() <= 9) {
                    return Integer.valueOf(4);
                }
                if (10 <= desc.getPrecision().intValue() && desc.getPrecision().intValue() <= 18) {
                    return Integer.valueOf(8);
                }
                if (19 <= desc.getPrecision().intValue() && desc.getPrecision().intValue() <= 38) {
                    return Integer.valueOf(16);
                }
                break;
            case 4:
                return Integer.valueOf(4);
            case 5:
                return Integer.valueOf(2);
            case 6:
                return Integer.valueOf(8);
            case ConnectorDataTypeDefinition.TYPE_REAL /*7*/:
                return Integer.valueOf(4);
            case ConnectorDataTypeDefinition.TYPE_DOUBLE /*8*/:
                return Integer.valueOf(8);
            case ConnectorDataTypeDefinition.TYPE_DATE /*91*/:
                return Integer.valueOf(10);
            case ConnectorDataTypeDefinition.TYPE_TIME /*92*/:
                if (desc.getPrecision().intValue() != 0) {
                    return Integer.valueOf(desc.getPrecision().intValue() + 9);
                }
                return Integer.valueOf(8);
            case ConnectorDataTypeDefinition.TYPE_TIMESTAMP /*93*/:
                if (desc.getPrecision().intValue() != 0) {
                    return Integer.valueOf(desc.getPrecision().intValue() + 20);
                }
                return Integer.valueOf(19);
            default:
                return Integer.valueOf(-1);
        }
        return desc.getPrecision();
    }

    public static Object getObjectFromBytes(IDataStreamColumnDesc desc, byte[] ba, boolean leserver) throws ConnectorException {
        try {
            double dvalue;
            switch (desc.getJDBCtype().intValue()) {
                case -16:
                case 1:
                case 12:
                    return new String(ba);
                case ConnectorDataTypeDefinition.TYPE_TINYINT /*-6*/:
                    return new Byte(ba[0]);
                case ConnectorDataTypeDefinition.TYPE_BIGINT /*-5*/:
                    long lvalue;
                    if (leserver) {
                        lvalue = ByteBuffer.wrap(ba).order(ByteOrder.LITTLE_ENDIAN).getLong();
                    } else {
                        lvalue = ByteBuffer.wrap(ba).getLong();
                    }
                    return new Long(lvalue);
                case ConnectorDataTypeDefinition.TYPE_VARBINARY /*-3*/:
                case ConnectorDataTypeDefinition.TYPE_BINARY /*-2*/:
                    return ba;
                case 2:
                case ConnectorDataTypeDefinition.TYPE_DECIMAL /*3*/:
                    if (leserver) {
                        ArrayUtils.reverse(ba);
                    }
                    return new BigDecimal(new BigInteger(ba), desc.getScale().intValue());
                case 4:
                    int ivalue;
                    if (leserver) {
                        ivalue = ByteBuffer.wrap(ba).order(ByteOrder.LITTLE_ENDIAN).getInt();
                    } else {
                        ivalue = ByteBuffer.wrap(ba).getInt();
                    }
                    return new Integer(ivalue);
                case 5:
                    short svalue;
                    if (leserver) {
                        svalue = ByteBuffer.wrap(ba).order(ByteOrder.LITTLE_ENDIAN).getShort();
                    } else {
                        svalue = ByteBuffer.wrap(ba).getShort();
                    }
                    return new Short(svalue);
                case 6:
                    if (leserver) {
                        dvalue = ByteBuffer.wrap(ba).order(ByteOrder.LITTLE_ENDIAN).getDouble();
                    } else {
                        dvalue = ByteBuffer.wrap(ba).getDouble();
                    }
                    return new Double(dvalue);
                case ConnectorDataTypeDefinition.TYPE_REAL /*7*/:
                    float fvalue;
                    if (leserver) {
                        fvalue = ByteBuffer.wrap(ba).order(ByteOrder.LITTLE_ENDIAN).getFloat();
                    } else {
                        fvalue = ByteBuffer.wrap(ba).getFloat();
                    }
                    return new Float(fvalue);
                case ConnectorDataTypeDefinition.TYPE_DOUBLE /*8*/:
                    if (leserver) {
                        dvalue = ByteBuffer.wrap(ba).order(ByteOrder.LITTLE_ENDIAN).getDouble();
                    } else {
                        dvalue = ByteBuffer.wrap(ba).getDouble();
                    }
                    return new Double(dvalue);
                case ConnectorDataTypeDefinition.TYPE_BLOB /*2004*/:
                    return new IDataStreamSerialBlob(ba);
                case ConnectorDataTypeDefinition.TYPE_CLOB /*2005*/:
                    return new IDataStreamSerialClob(new String(ba).toCharArray());
                default:
                    return null;
            }
        } catch (Exception e) {
            throw new ConnectorException((int) ErrorCode.IDATASTREAM_DATA_CONV_FAILED);
        }
    }

    public static void getBytesFromObject(ByteBuffer bb, IDataStreamColumnDesc desc, Object o, boolean leserver) {
        if (nullbytes == null) {
            nullbytes = new byte[16];
        }
        switch (desc.getJDBCtype().intValue()) {
            case -16:
            case 12:
            case ConnectorDataTypeDefinition.TYPE_CLOB /*2005*/:
                if (o != null) {
                    bb.put(((String) o).getBytes());
                    return;
                }
                return;
            case ConnectorDataTypeDefinition.TYPE_TINYINT /*-6*/:
                if (o == null) {
                    bb.put(nullbytes, 0, 1);
                    return;
                } else {
                    bb.put(((Byte) o).byteValue());
                    return;
                }
            case ConnectorDataTypeDefinition.TYPE_BIGINT /*-5*/:
                if (o == null) {
                    bb.put(nullbytes, 0, 8);
                    return;
                } else {
                    bb.putLong(((Long) o).longValue());
                    return;
                }
            case ConnectorDataTypeDefinition.TYPE_VARBINARY /*-3*/:
            case ConnectorDataTypeDefinition.TYPE_BLOB /*2004*/:
                if (o != null) {
                    bb.put((byte[]) o);
                    return;
                }
                return;
            case ConnectorDataTypeDefinition.TYPE_BINARY /*-2*/:
                byte[] ba = new byte[desc.getPrecision().intValue()];
                if (o != null) {
                    System.arraycopy((byte[]) o, 0, ba, 0, desc.getPrecision().intValue());
                }
                bb.put(ba);
                return;
            case 1:
                if (o == null) {
                    bb.put(new byte[desc.getPrecision().intValue()]);
                    return;
                } else {
                    bb.put(((String) o).getBytes());
                    return;
                }
            case 2:
            case ConnectorDataTypeDefinition.TYPE_DECIMAL /*3*/:
                int bytelen = getByteLength(desc).intValue();
                if (o == null) {
                    bb.put(new byte[bytelen]);
                    return;
                }
                BigDecimal bd = (BigDecimal) o;
                if (desc.getScale().intValue() != 0) {
                    bd = bd.movePointRight(desc.getScale().intValue());
                }
                byte[] dba = bd.toBigInteger().toByteArray();
                if (dba.length != bytelen) {
                    byte[] dba2 = new byte[bytelen];
                    if ((dba[0] & 128) == 128) {
                        Arrays.fill(dba2, (byte) -1);
                    }
                    System.arraycopy(dba, 0, dba2, dba2.length - dba.length, dba.length);
                    dba = dba2;
                }
                if (leserver) {
                    ArrayUtils.reverse(dba);
                }
                bb.put(dba);
                return;
            case 4:
                if (o == null) {
                    bb.put(nullbytes, 0, 4);
                    return;
                } else {
                    bb.putInt(((Integer) o).intValue());
                    return;
                }
            case 5:
                if (o == null) {
                    bb.put(nullbytes, 0, 2);
                    return;
                } else {
                    bb.putShort(((Short) o).shortValue());
                    return;
                }
            case 6:
                if (o == null) {
                    bb.put(nullbytes, 0, 8);
                    return;
                } else {
                    bb.putDouble(((Double) o).doubleValue());
                    return;
                }
            case ConnectorDataTypeDefinition.TYPE_REAL /*7*/:
                if (o == null) {
                    bb.put(nullbytes, 0, 4);
                    return;
                } else {
                    bb.putFloat(((Float) o).floatValue());
                    return;
                }
            case ConnectorDataTypeDefinition.TYPE_DOUBLE /*8*/:
                if (o == null) {
                    bb.put(nullbytes, 0, 8);
                    return;
                } else {
                    bb.putDouble(((Double) o).doubleValue());
                    return;
                }
            default:
                return;
        }
    }
}