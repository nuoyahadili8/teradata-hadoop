package com.teradata.connector.idatastream;

import com.teradata.connector.teradata.schema.TeradataColumnDesc;

import java.io.*;
import java.lang.reflect.Constructor;
import java.net.ServerSocket;
import java.net.Socket;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.BitSet;
import java.util.List;
import java.util.Random;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;


/**
 * @author Administrator
 */
public class IDataStreamImportServer implements Runnable {
    protected int port;
    protected int nummappers;
    protected String inputdatafile;
    protected String[] inputdatafiles;
    protected String outputdatafile;
    protected String[] outputdatafiles;
    protected String schema;
    protected Socket[] execSockets;
    protected int numRows;
    protected boolean noNulls;
    protected ServerSocket serverSocket;

    public static void main(final String[] args) {
        IDataStreamImportServer server = null;
        if (args.length >= 7) {
            int port;
            try {
                port = Integer.parseInt(args[0]);
            } catch (Exception e) {
                port = 5555;
            }
            int nummappers;
            try {
                nummappers = Integer.parseInt(args[1]);
            } catch (Exception e2) {
                nummappers = 2;
            }
            final String direction = args[2];
            final String inputdatafile = args[3];
            final String outputdatafile = args[4];
            final String schema = args[5];
            final int numRows = Integer.parseInt(args[6]);
            final boolean noNulls = "true".equals(args[7]);
            if (direction.equals("import")) {
                server = new IDataStreamImportServer(port, nummappers, inputdatafile, outputdatafile, schema, numRows, noNulls);
            } else {
                final IDataStreamImportServer Iserver = new IDataStreamImportServer(port, nummappers, inputdatafile, outputdatafile, schema, numRows, noNulls);
                server = Iserver.new IDataStreamExportServer(port, nummappers, outputdatafile);
            }
        } else {
            server = new IDataStreamImportServer();
        }
        final ExecutorService exe = Executors.newSingleThreadExecutor();
        exe.execute(server);
        exe.shutdown();
        boolean completed;
        try {
            completed = exe.awaitTermination(200L, TimeUnit.SECONDS);
        } catch (InterruptedException e3) {
            completed = false;
        }
        if (completed) {
            System.out.println("IDataStreami(Import|Export)Server shutdown gracefully");
        } else {
            System.out.println("IDataStream(Import|Export)Server shutdown by timeout/interruption");
            exe.shutdownNow();
            server.shutdownServer();
        }
    }

    public IDataStreamImportServer() {
        this.port = 5555;
        this.nummappers = 1;
        this.inputdatafile = "internal";
        this.outputdatafile = "internal";
        this.schema = "int,varchar";
        this.execSockets = null;
        this.numRows = 10;
        this.noNulls = true;
        this.serverSocket = null;
    }

    public IDataStreamImportServer(final int port, final int nummappers, final String inputdatafile, final String outputdatafile, final String schema, final int numRows, final boolean noNulls) {
        this.port = 5555;
        this.nummappers = 1;
        this.inputdatafile = "internal";
        this.outputdatafile = "internal";
        this.schema = "int,varchar";
        this.execSockets = null;
        this.numRows = 10;
        this.noNulls = true;
        this.serverSocket = null;
        this.port = port;
        this.nummappers = nummappers;
        this.inputdatafile = inputdatafile;
        this.inputdatafiles = inputdatafile.split(",");
        this.outputdatafile = outputdatafile;
        this.schema = schema;
        this.execSockets = new Socket[nummappers];
        this.numRows = numRows;
        this.noNulls = noNulls;
    }

    @Override
    public void run() {
        if (!this.inputdatafile.equals("internal") && this.inputdatafiles.length != this.nummappers) {
            System.out.println("User must supply input files for all mappers");
            return;
        }
        final ExecutorService exe = Executors.newFixedThreadPool(this.nummappers);
        try {
            if (!this.outputdatafile.equals("internal")) {
                this.serverSocket = new ServerSocket(this.port);
            }
            for (int i = 0; i < this.nummappers; ++i) {
                InputStream is;
                if (this.inputdatafile.equals("internal")) {
                    is = new IDataStreamGenerator(i, this.schema, this.numRows, this.noNulls);
                } else {
                    is = new FileInputStream(this.inputdatafile);
                }
                final List<OutputStream> oss = new ArrayList<OutputStream>();
                if (this.outputdatafile.equals("internal")) {
                    oss.add(new FileOutputStream("part_m_00000" + i));
                } else {
                    oss.add(new FileOutputStream("part_m_00000" + i));
                    System.out.println("Waiting for " + (this.nummappers - i) + " incoming connections");
                    final Socket s = this.serverSocket.accept();
                    this.execSockets[i] = s;
                    oss.add(s.getOutputStream());
                }
                exe.execute(new DataWriter(i, is, oss));
            }
            exe.shutdown();
            boolean completed;
            try {
                completed = exe.awaitTermination(50L, TimeUnit.SECONDS);
            } catch (InterruptedException e) {
                completed = false;
            }
            if (completed) {
                System.out.println("DataWriters shutdown gracefully");
            } else {
                System.out.println("DataWriters shutdown by timeout/interruption");
            }
        } catch (Exception ex) {
        } finally {
            try {
                if (!this.outputdatafile.equals("internal") && this.serverSocket != null) {
                    this.serverSocket.close();
                }
            } catch (Exception ex2) {
            }
        }
    }

    public void dataWriterComplete(final int id) {
        System.out.println("Datawriter " + id + " complete");
        try {
            if (!this.outputdatafile.equals("internal")) {
                while (this.execSockets[id].getInputStream().read() != -1) {
                    Thread.sleep(5000L);
                }
                this.execSockets[id].close();
            }
        } catch (Exception ex) {
        }
    }

    public void shutdownServer() {
        try {
            this.serverSocket.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public class IDataStreamExportServer extends IDataStreamImportServer implements Runnable {
        public IDataStreamExportServer(final int port, final int nummappers, final String outputdatafile) {
            this.port = port;
            this.nummappers = nummappers;
            this.outputdatafile = outputdatafile;
            this.execSockets = new Socket[nummappers];
        }

        @Override
        public void run() {
            this.outputdatafiles = new String[this.nummappers];
            if (this.outputdatafile.equals("internal") || this.outputdatafile.split(",").length != this.nummappers) {
                for (int i = 0; i < this.nummappers; ++i) {
                    this.outputdatafiles[i] = "part_m_00000" + i + ".out";
                }
            } else {
                final String[] datafiles = this.outputdatafile.split(",");
                for (int j = 0; j < this.nummappers; ++j) {
                    this.outputdatafiles[j] = datafiles[j];
                }
            }
            final ExecutorService exe = Executors.newFixedThreadPool(this.nummappers);
            try {
                this.serverSocket = new ServerSocket(this.port);
                for (int j = 0; j < this.nummappers; ++j) {
                    System.out.println("Waiting for " + (this.nummappers - j) + " incoming connections for export");
                    final Socket s = this.serverSocket.accept();
                    this.execSockets[j] = s;
                    final InputStream is = s.getInputStream();
                    final byte[] final_nummappers = new byte[2];
                    is.read(final_nummappers, 0, 2);
                    final short fnummappers = ByteBuffer.wrap(final_nummappers).getShort();
                    if (j == 0 && fnummappers != this.nummappers) {
                        this.nummappers = fnummappers;
                        System.out.println(" Updating nummappers with advertised number of mappers.");
                    } else if (fnummappers != this.nummappers) {
                        System.out.println(" Advertised number of mappers is not equal to the defined number of mappers!!!");
                    }
                    final OutputStream os = new FileOutputStream(this.outputdatafiles[j]);
                    System.out.println("created FOS for " + this.outputdatafiles[j]);
                    final List<OutputStream> oss = new ArrayList<OutputStream>();
                    oss.add(os);
                    exe.execute(new DataWriter(j, is, oss));
                }
                exe.shutdown();
                boolean completed;
                try {
                    completed = exe.awaitTermination(50L, TimeUnit.SECONDS);
                } catch (InterruptedException e2) {
                    completed = false;
                }
                if (completed) {
                    System.out.println("DataWriters shutdown gracefully");
                } else {
                    System.out.println("DataWriters shutdown by timeout/interruption");
                }
            } catch (Exception e) {
                e.printStackTrace();
            } finally {
                try {
                    this.serverSocket.close();
                } catch (Exception ex) {
                }
            }
        }

        @Override
        public void dataWriterComplete(final int id) {
            System.out.println("Datawriter " + id + " complete");
            try {
                this.execSockets[id].close();
            } catch (Exception ex) {
            }
        }
    }

    private class DataWriter implements Runnable {
        private static final int BUFFER_SIZE = 1024;
        private int myID;
        private InputStream inputStream;
        private List<OutputStream> outputStreams;
        private byte[] buffer;

        public DataWriter(final int myID, final InputStream inputStream, final List<OutputStream> outputStreams) {
            this.myID = myID;
            this.inputStream = inputStream;
            this.outputStreams = new ArrayList<OutputStream>();
            for (final OutputStream outputStream : outputStreams) {
                this.outputStreams.add(outputStream);
            }
            this.buffer = new byte[1024];
        }

        @Override
        public void run() {
            int bytesRead = 0;
            try {
                bytesRead = this.inputStream.read(this.buffer, 0, 1024);
            } catch (Exception e) {
                e.printStackTrace();
                bytesRead = -1;
            }
            while (bytesRead > 0) {
                try {
                    System.out.println("Writing " + bytesRead + " to the output stream");
                    for (final OutputStream outputStream : this.outputStreams) {
                        outputStream.write(this.buffer, 0, bytesRead);
                    }
                    bytesRead = this.inputStream.read(this.buffer, 0, 1024);
                } catch (Exception e) {
                    e.printStackTrace();
                    bytesRead = -1;
                }
            }
            try {
                this.inputStream.close();
                for (final OutputStream outputStream : this.outputStreams) {
                    outputStream.close();
                }
            } catch (Exception ex) {
            }
            IDataStreamImportServer.this.dataWriterComplete(this.myID);
        }
    }

    public class IDataStreamGenerator extends InputStream {
        private int id;
        private int numCols;
        private int numRows;
        private int rowCount;
        private DataGen[] gens;
        private boolean noNulls;
        private boolean[] nulCols;
        private BitSet bitset;
        private int curCol;
        private int rowSize;
        private int indByteSize;
        private Random rand;

        public IDataStreamGenerator() {
            this.id = 0;
            this.numCols = 0;
            this.numRows = 0;
            this.rowCount = 0;
            this.gens = null;
            this.noNulls = false;
            this.nulCols = null;
            this.bitset = null;
            this.curCol = 0;
            this.rowSize = 0;
            this.indByteSize = 0;
            this.rand = null;
        }

        public IDataStreamGenerator(final int id, final String schema, final int numRows, final boolean noNulls) {
            this.id = 0;
            this.numCols = 0;
            this.numRows = 0;
            this.rowCount = 0;
            this.gens = null;
            this.noNulls = false;
            this.nulCols = null;
            this.bitset = null;
            this.curCol = 0;
            this.rowSize = 0;
            this.indByteSize = 0;
            this.rand = null;
            this.id = id;
            final String[] cols = schema.split(",");
            this.numCols = cols.length;
            this.numRows = numRows;
            this.noNulls = noNulls;
            Arrays.fill(this.nulCols = new boolean[this.numCols], false);
            this.rand = new Random();
            this.indByteSize = (int) Math.ceil(this.numCols / 8.0f);
            this.bitset = new BitSet(this.indByteSize * 8);
            System.out.println("Bitset initialized with length " + this.indByteSize * 8);
            this.gens = new DataGen[this.numCols];
            for (int i = 0; i < this.numCols; ++i) {
                try {
                    final String classname = "IDataStreamImportServer$IDataStreamGenerator$" + cols[i].toLowerCase() + "Gen";
                    final Constructor<?>[] meth = Class.forName(classname).getConstructors();
                    this.gens[i] = (DataGen) meth[0].newInstance(this);
                } catch (Exception e) {
                    e.printStackTrace();
                }
            }
        }

        @Override
        public int read() throws IOException {
            return 0;
        }

        @Override
        public int read(final byte[] buffer, final int offset, final int size) {
            if (this.rowCount == this.numRows) {
                return -1;
            }
            String nulColS = "";
            int curSize = 0;
            if (this.curCol == 0) {
                if (!this.noNulls) {
                    for (int i = 0; i < this.numCols; ++i) {
                        this.nulCols[i] = (this.rand.nextInt(4) == 3);
                        this.bitset.set(i, this.nulCols[i]);
                    }
                }
                this.rowSize = 0;
                for (int i = 0; i < this.numCols; ++i) {
                    this.rowSize += this.gens[i].getSize(this.nulCols[i]);
                    if (!this.nulCols[i]) {
                        nulColS += "notnull,";
                    } else {
                        nulColS += "null,";
                    }
                }
                this.rowSize += this.indByteSize;
                nulColS = "";
                if (this.bitset.isEmpty()) {
                    nulColS = "00";
                } else {
                    System.out.println("Bitlength is " + this.bitset.length());
                    for (int i = 0; i < this.indByteSize; ++i) {
                        nulColS += String.format("%02x", this.bitsetToByteArray(this.bitset)[i]);
                    }
                }
                System.out.println("DSG" + this.id + ": Writing record " + this.rowCount + ", column 0, size " + this.rowSize + ", indicator bytes are " + nulColS);
                if (offset + 2 + this.indByteSize >= size) {
                    return -1;
                }
                System.arraycopy(ByteBuffer.allocate(2).order(ByteOrder.LITTLE_ENDIAN).putShort((short) this.rowSize).array(), 0, buffer, offset, 2);
                System.arraycopy(this.bitsetToByteArray(this.bitset), 0, buffer, offset + 2, this.indByteSize);
                curSize = 2 + this.indByteSize;
            }
            for (byte[] data = this.gens[this.curCol].genData(); offset + curSize + data.length < size; data = this.gens[this.curCol].genData()) {
                System.arraycopy(data, 0, buffer, offset + curSize, data.length);
                curSize += data.length;
                data = null;
                if (++this.curCol == this.numCols) {
                    buffer[curSize++] = 10;
                    this.curCol = 0;
                    if (++this.rowCount == this.numRows) {
                        System.arraycopy("EOD".getBytes(), 0, buffer, offset + curSize, 3);
                        curSize += 3;
                        break;
                    }
                    if (!this.noNulls) {
                        for (int j = 0; j < this.numCols; ++j) {
                            this.nulCols[j] = (this.rand.nextInt(4) == 3);
                            this.bitset.set(j, this.nulCols[j]);
                        }
                    }
                    this.rowSize = 0;
                    for (int j = 0; j < this.numCols; ++j) {
                        this.rowSize += this.gens[j].getSize(this.nulCols[j]);
                    }
                    this.rowSize += this.indByteSize;
                    nulColS = "";
                    if (this.bitset.isEmpty()) {
                        nulColS = "00";
                    } else {
                        for (int j = 0; j < this.indByteSize; ++j) {
                            nulColS += String.format("%02x", this.bitsetToByteArray(this.bitset)[j]);
                        }
                    }
                    System.out.println("DSG" + this.id + ": Writing record #" + this.rowCount + " size " + this.rowSize + ", indicator bytes are " + nulColS);
                    if (offset + curSize + 2 + this.indByteSize + this.rowSize >= size) {
                        System.out.println("Just kidding, not writing that record (or its header bytes to the output stream");
                        break;
                    }
                    System.arraycopy(ByteBuffer.allocate(2).order(ByteOrder.LITTLE_ENDIAN).putShort((short) this.rowSize).array(), 0, buffer, offset + curSize, 2);
                    System.arraycopy(this.bitsetToByteArray(this.bitset), 0, buffer, offset + curSize + 2, this.indByteSize);
                    curSize += 2 + this.indByteSize;
                }
            }
            return (curSize == 0) ? -1 : curSize;
        }

        private byte[] bitsetToByteArray(final BitSet bits) {
            final byte[] bytes = new byte[(bits.size() + 7) / 8];
            for (int i = 0; i < bits.size(); ++i) {
                if (bits.get(i)) {
                    final byte[] array = bytes;
                    final int n = i / 8;
                    array[n] |= (byte) (1 << 7 - i % 8);
                }
            }
            return bytes;
        }

        public abstract class DataGen {
            public Random r = new Random();

            public abstract int getSize(final boolean p0);

            public abstract byte[] genData();
        }

        public class intGen extends DataGen {
            private boolean isNull;

            public intGen() {
                this.isNull = false;
            }

            @Override
            public int getSize(final boolean isNull) {
                this.isNull = isNull;
                return 4;
            }

            @Override
            public byte[] genData() {
                int gen = 0;
                if (this.isNull) {
                    gen = 0;
                } else {
                    gen = this.r.nextInt(1024);
                }
                System.out.println("Generating int " + gen + " of size " + 4);
                return ByteBuffer.allocate(4).order(ByteOrder.LITTLE_ENDIAN).putInt(gen).array();
            }
        }

        public class varcharGen extends DataGen {
            private static final int MAX_VARCHAR_SIZE = 20;
            private byte[] buffer;

            public varcharGen() {
                this.buffer = null;
            }

            @Override
            public int getSize(final boolean isNull) {
                int datasize = 0;
                if (isNull) {
                    datasize = 0;
                } else {
                    datasize = 1 + this.r.nextInt(19);
                }
                System.out.println("Generating string of size " + datasize + ", isNull is " + isNull);
                this.buffer = new byte[datasize + 2];
                final byte[] dsize = ByteBuffer.allocate(2).order(ByteOrder.LITTLE_ENDIAN).putShort((short) datasize).array();
                System.out.println(String.format("Size bytes are %02x%02x", dsize[0], dsize[1]));
                Arrays.fill(this.buffer, (byte) 0);
                System.arraycopy(dsize, 0, this.buffer, 0, 2);
                for (int i = 2; i < datasize + 2; ++i) {
                    this.buffer[i] = (byte) (this.r.nextInt(26) + 65);
                }
                String bufstr = "";
                for (int j = 0; j < this.buffer.length; ++j) {
                    bufstr += String.format("%02x", this.buffer[j]);
                }
                System.out.println("varchar buffer is " + bufstr);
                return this.buffer.length;
            }

            @Override
            public byte[] genData() {
                return this.buffer;
            }
        }
    }
}
