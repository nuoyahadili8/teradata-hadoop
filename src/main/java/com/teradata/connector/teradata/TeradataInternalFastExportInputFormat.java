package com.teradata.connector.teradata;

import com.teradata.connector.common.ConnectorRecord;
import com.teradata.connector.common.exception.ConnectorException;
import com.teradata.connector.common.exception.ConnectorException.ErrorCode;
import com.teradata.connector.common.utils.ConnectorConfiguration;
import com.teradata.connector.common.utils.HadoopConfigurationUtils;
import com.teradata.connector.hive.utils.HiveUtils;
import com.teradata.connector.teradata.TeradataInputFormat.TeradataInputSplit;
import com.teradata.connector.teradata.db.TeradataConnection;
import com.teradata.connector.teradata.processor.TeradataInternalFastExportProcessor;
import com.teradata.connector.teradata.utils.TeradataPlugInConfiguration;
import com.teradata.connector.teradata.utils.TeradataSchemaUtils;
import com.teradata.connector.teradata.utils.TeradataUtils;
import com.teradata.jdbc.jdbc_4.TDSession;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.net.*;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;


public class TeradataInternalFastExportInputFormat extends TeradataInputFormat {
    private static Log logger;
    protected static Connection lsnConnection;
    protected static ServerSocket server;
    public static String MSG_FASTEXPORT_TRANSFER_BEGIN;
    public static String MSG_FASTEXPORT_TRANSFER_END;
    public static String MSG_FASTEXPORT_CONNECTION_READY;
    public static String MSG_FASTEXPORT_CONNECTION_CLOSE;
    public static String MSG_FASTEXPORT_PING;
    protected static String PARAM_FASTEXPORT_CONNECTION;
    protected static String PARAM_LSN_CONNECTION;
    protected static String PARAM_SESSION_CONNECTION;
    protected static String SQL_GET_LSN;
    protected static String SQL_REQUEST_TRACKING_ON;
    protected static String SQL_REQUEST_TRACKING_OFF;
    protected static String SQL_PROVIDE_REQUEST_TRACKING;
    protected static String SQL_PROVIDE_REQUESTS;
    protected static String SQL_CLEAR_REQUESTS;
    protected static String SQL_BEGIN_FASTEXPORT;
    protected static String SQL_END_FASTEXPORT;
    protected static String FASTEXPORT_PARAMS_FILE_NAME;
    private static final String CHECK_WORKLOAD_BEGIN = "CHECK WORKLOAD FOR ";
    private static final String CHECK_WORKLOAD_END = "CHECK WORKLOAD END";
    private static final String LSS_TYPE = "LSS_TYPE=E";
    private static final String FASTEXPORT_QUERY_BAND = "UtilityName=JDBCE;";
    private static final String LINE_SEP;
    public static int SOCKET_CONNECT_TIMEOUT;
    public static int SOCKET_CONNECT_RETRY_MAX;
    public static int SOCKET_CONNECT_RETRY_WAIT_MIN;
    public static int SOCKET_CONNECT_RETRY_WAIT_RANDOM_SEED;
    protected static int SOCKET_SERVER_ACCEPT_TIME_OUT;
    protected static int SOCKET_PING_TIME_OUT;
    protected String charset;
    protected boolean isGoverned;
    protected boolean isFastFail;
    protected boolean enableUnicodePassthrough;
    protected String jdbcUrl;
    protected int readyConnCount;
    protected int readySessionCount;

    public TeradataInternalFastExportInputFormat() {
        this.charset = "";
        this.isGoverned = false;
        this.isFastFail = false;
        this.enableUnicodePassthrough = false;
        this.jdbcUrl = "";
        this.readyConnCount = 0;
        this.readySessionCount = 0;
    }

    @Override
    public void validateConfiguration(final JobContext context) throws ConnectorException {
        super.validateConfiguration(context);
        final Configuration configuration = context.getConfiguration();
        this.inputTableName = TeradataConnection.getQuotedEscapedName(TeradataPlugInConfiguration.getInputDatabase(configuration), TeradataPlugInConfiguration.getInputTable(configuration));
        this.inputConditions = TeradataPlugInConfiguration.getInputConditions(configuration);
        this.inputFieldNamesArray = TeradataPlugInConfiguration.getInputFieldNamesArray(configuration);
        final boolean accessLock = TeradataPlugInConfiguration.getInputAccessLock(configuration);
        String inputQuery = TeradataConnection.getSelectSQL(this.inputTableName, this.inputFieldNamesArray, this.inputConditions);
        if (accessLock) {
            inputQuery = TeradataConnection.addAccessLockToSql(inputQuery);
        }
        TeradataPlugInConfiguration.setInputSplitSql(configuration, inputQuery);
        try {
            this.jdbcUrl = TeradataPlugInConfiguration.getInputJdbcUrl(configuration);
            this.charset = TeradataConnection.getURLParamValue(this.jdbcUrl, "CHARSET");
            if (this.charset == null || (!this.charset.equalsIgnoreCase("UTF8") && !this.charset.equalsIgnoreCase("UTF16"))) {
                this.charset = "";
            }
            this.isGoverned = TeradataConnection.getJDBCURLParameters(this.jdbcUrl).isGoverned();
            this.isFastFail = TeradataPlugInConfiguration.getInputFastFail(configuration);
        } catch (SQLException e) {
            throw new ConnectorException(e.getMessage(), e);
        }
        this.enableUnicodePassthrough = TeradataPlugInConfiguration.getUnicodePassthrough(configuration);
        TeradataUtils.closeConnection(this.connection);
    }

    public List<InputSplit> getSplits(final JobContext context) throws IOException, InterruptedException {
        final List<InputSplit> splits = new ArrayList<InputSplit>();
        this.validateConfiguration(context);
        final Configuration configuration = context.getConfiguration();
        final String classname = TeradataPlugInConfiguration.getOutputJdbcDriverClass(configuration);
        try {
            TeradataInternalFastExportInputFormat.server = HadoopConfigurationUtils.createServerSocket(TeradataPlugInConfiguration.getInputFastExportSocketPort(configuration), TeradataPlugInConfiguration.getInputFastExportSocketBacklog(configuration));
            String connectionString = TeradataInternalFastExportInputFormat.PARAM_LSN_CONNECTION;
            if (!this.charset.equals("")) {
                connectionString = connectionString + ",CHARSET=" + this.charset;
            }
            if (this.isGoverned || this.isFastFail) {
                connectionString += ",LSS_TYPE=E";
            }
            TeradataInternalFastExportInputFormat.lsnConnection = TeradataConnection.getConnection(classname, this.jdbcUrl, TeradataPlugInConfiguration.getInputTeradataUserName(context), TeradataPlugInConfiguration.getInputTeradataPassword(context), connectionString, this.enableUnicodePassthrough);
            final String queryBandProperty = TeradataPlugInConfiguration.getInputQueryBand(configuration) + ((this.isGoverned || this.isFastFail) ? "UtilityName=JDBCE;" : "");
            TeradataUtils.validateQueryBand(queryBandProperty);
            TeradataConnection.setQueryBandProperty(TeradataInternalFastExportInputFormat.lsnConnection, queryBandProperty);
            final int numSessions = getFastExportSessionCount(configuration);
            ConnectorConfiguration.setNumMappers(configuration, numSessions);
            this.configFastExportConnectivity(context, numSessions);
            TeradataInternalFastExportInputFormat.logger.info((Object) ("started load sessions: " + numSessions));
            final InternalFastExportCoordinator coordinator = new InternalFastExportCoordinator(numSessions, configuration);
            final Thread thread = new Thread(coordinator);
            thread.start();
            final String[] locations = HadoopConfigurationUtils.getAllActiveHosts(context);
            final String splitSql = TeradataPlugInConfiguration.getInputSplitSql(configuration);
            for (int i = 0; i < numSessions; ++i) {
                final TeradataInputSplit split = new TeradataInputSplit(splitSql);
                split.setLocations(HadoopConfigurationUtils.selectUniqueActiveHosts(locations, 6));
                splits.add(split);
            }
        } catch (SQLException e) {
            TeradataUtils.closeConnection(TeradataInternalFastExportInputFormat.lsnConnection);
            this.closeServer(TeradataInternalFastExportInputFormat.server);
            throw new ConnectorException(e.getMessage(), e);
        } catch (ClassNotFoundException e2) {
            TeradataUtils.closeConnection(TeradataInternalFastExportInputFormat.lsnConnection);
            this.closeServer(TeradataInternalFastExportInputFormat.server);
            throw new ConnectorException(e2.getMessage(), e2);
        }
        return splits;
    }

    private static int getFastExportSessionCount(final Configuration configuration) throws SQLException {
        final TDSession session = (TDSession) TeradataInternalFastExportInputFormat.lsnConnection;
        final int numMappers = ConnectorConfiguration.getNumMappers(configuration);
        final boolean isGoverned = session.getURLParameters().isGoverned();
        if (session.useCheckWorkload()) {
            final String inputDatabase = TeradataPlugInConfiguration.getInputDatabase(configuration);
            String inputTableName = TeradataPlugInConfiguration.getInputTable(configuration);
            inputTableName = TeradataConnection.getQuotedEscapedName(inputDatabase, inputTableName);
            final String inputQuery = TeradataPlugInConfiguration.getInputSplitSql(configuration);
            final String checkWorkloadBegin = "CHECK WORKLOAD FOR " + TeradataInternalFastExportInputFormat.SQL_BEGIN_FASTEXPORT;
            final Statement controlStmt = TeradataInternalFastExportInputFormat.lsnConnection.createStatement();
            controlStmt.executeUpdate(checkWorkloadBegin);
            final String checkWorkloadQuery = "CHECK WORKLOAD FOR " + inputQuery;
            controlStmt.executeQuery(checkWorkloadQuery);
            final String checkWorkloadEnd = (isGoverned ? "" : "{fn TERADATA_FAILFAST}") + "CHECK WORKLOAD END";
            final ResultSet rs = controlStmt.executeQuery(checkWorkloadEnd);
            final ResultSetMetaData rsmd = rs.getMetaData();
            if (rs.next() && rsmd.getColumnCount() >= 2 && rs.getString(1) != null) {
                if (rs.getString(1).trim().equalsIgnoreCase("Y")) {
                    final int count = rs.getInt(2);
                    if (count > 0) {
                        final int numSugSessn = Math.min(numMappers, count);
                        if (numSugSessn < numMappers) {
                            TeradataInternalFastExportInputFormat.logger.info((Object) ("user provided number of Mappers [" + numMappers + "] is overridden by [" + numSugSessn + "] returned from DBS"));
                        } else {
                            TeradataInternalFastExportInputFormat.logger.info((Object) ("user provided number of Mappers is NOT overridden by [" + count + "] DBS."));
                        }
                        return numSugSessn;
                    }
                    TeradataInternalFastExportInputFormat.logger.info((Object) ("invalid number " + count + " returned from DBS"));
                } else {
                    TeradataInternalFastExportInputFormat.logger.info((Object) "returned TASM-flag is N");
                }
            } else {
                TeradataInternalFastExportInputFormat.logger.info((Object) "unrecognized column returned");
            }
            rs.close();
            controlStmt.close();
        } else {
            TeradataInternalFastExportInputFormat.logger.info((Object) "TDCH is not controlled by TASM");
        }
        return numMappers;
    }

    private void configFastExportConnectivity(final JobContext context, final int numTasks) throws SQLException, IOException {
        final Configuration configuration = context.getConfiguration();
        String host = TeradataPlugInConfiguration.getInputFastExportSocketHost(configuration);
        int port = TeradataPlugInConfiguration.getInputFastExportSocketPort(configuration);
        final String lsnNumber = TeradataInternalFastExportInputFormat.lsnConnection.nativeSQL(TeradataInternalFastExportInputFormat.SQL_GET_LSN);
        if (lsnNumber != null && lsnNumber.length() != 0) {
            TeradataInternalFastExportInputFormat.logger.debug((Object) ("fastexport lsn number is " + lsnNumber));
            TeradataPlugInConfiguration.setInputFastExportLsn(configuration, lsnNumber);
            if (host.isEmpty() || host.equalsIgnoreCase("default") || host.equalsIgnoreCase("localhost")) {
                host = HadoopConfigurationUtils.getClusterNodeInterface(context);
                TeradataPlugInConfiguration.setInputFastExportSocketHost(configuration, host);
            }
            if (port == 0) {
                port = TeradataInternalFastExportInputFormat.server.getLocalPort();
                TeradataPlugInConfiguration.setInputFastExportSocketPort(configuration, port);
            }
            return;
        }
        throw new SQLException("lsn number missing");
    }

    protected void closeServer(final ServerSocket server) {
        if (server != null && !server.isClosed()) {
            try {
                server.close();
            } catch (IOException e1) {
                TeradataInternalFastExportInputFormat.logger.debug((Object) e1.getMessage());
            }
        }
    }

    protected void closeSocket(final Socket socket) {
        if (socket != null && !socket.isClosed()) {
            try {
                socket.close();
            } catch (IOException e1) {
                TeradataInternalFastExportInputFormat.logger.debug((Object) e1.getMessage());
            }
        }
    }

    public static boolean getJobStatus() {
        synchronized (TeradataInternalFastExportProcessor.class) {
            return TeradataInternalFastExportProcessor.jobSuccess;
        }
    }

    @Override
    public RecordReader<LongWritable, ConnectorRecord> createRecordReader(final InputSplit split, final TaskAttemptContext context) throws IOException, InterruptedException {
        final Configuration configuration = context.getConfiguration();
        final String submitJobHost = configuration.get("mapreduce.job.submithostname");
        final String lsn = TeradataPlugInConfiguration.getInputFastExportLsn(configuration);
        String host = TeradataPlugInConfiguration.getInputFastExportSocketHost(configuration);
        final int port = TeradataPlugInConfiguration.getInputFastExportSocketPort(configuration);
        TeradataInternalFastExportInputFormat.logger.debug((Object) ("FastExport mapper socket host is " + host));
        TeradataInternalFastExportInputFormat.logger.debug((Object) ("FastExport mapper socket port is " + port));
        TeradataInternalFastExportInputFormat.logger.debug((Object) ("FastExport lsn is " + lsn));
        Socket socket = null;
        Connection fastExportConnection = null;
        Connection sessionConnection = null;
        int retryCount = 0;
        try {
            while (socket == null) {
                try {
                    socket = new Socket();
                    socket.connect(new InetSocketAddress(host, port), TeradataInternalFastExportInputFormat.SOCKET_CONNECT_TIMEOUT);
                    socket.setTcpNoDelay(true);
                    TeradataInternalFastExportInputFormat.logger.debug((Object) "Connection to InternalFastExport coordinator successful");
                } catch (Exception e) {
                    if (++retryCount >= TeradataInternalFastExportInputFormat.SOCKET_CONNECT_RETRY_MAX) {
                        TeradataInternalFastExportInputFormat.logger.debug((Object) ("After " + retryCount + " retries, unable to connect to InternalFastExport coordinator"));
                        throw new ConnectorException(e.getMessage(), e);
                    }
                    socket = null;
                    if (!submitJobHost.isEmpty() && retryCount % 10 == 0) {
                        try {
                            socket = new Socket();
                            socket.connect(new InetSocketAddress(submitJobHost, port), TeradataInternalFastExportInputFormat.SOCKET_CONNECT_TIMEOUT);
                            socket.setTcpNoDelay(true);
                            host = submitJobHost;
                            TeradataInternalFastExportInputFormat.logger.debug((Object) "Connection to InternalFastExport coordinator successful via mapreduce.job.submithostname");
                        } catch (Exception e6) {
                            TeradataInternalFastExportInputFormat.logger.debug((Object) ("Caught exception " + e.getMessage()));
                        }
                    }
                    try {
                        final int waitSeconds = new Random().nextInt(TeradataInternalFastExportInputFormat.SOCKET_CONNECT_RETRY_WAIT_RANDOM_SEED) + TeradataInternalFastExportInputFormat.SOCKET_CONNECT_RETRY_WAIT_MIN;
                        Thread.sleep(waitSeconds);
                    } catch (InterruptedException e7) {
                        TeradataInternalFastExportInputFormat.logger.debug((Object) "InterruptedException occurred during random sleep between retries");
                    }
                }
            }
            final DataInputStream in = new DataInputStream(socket.getInputStream());
            final DataOutputStream out = new DataOutputStream(socket.getOutputStream());
            final String taskAttempId = context.getTaskAttemptID().toString();
            if (context.getTaskAttemptID().getId() > 0) {
                TeradataInternalFastExportInputFormat.logger.debug((Object) "Current task attempt ID is > 0, internal.fastexport doesn't support preemption/failover, FastExport job failed");
                out.writeUTF("FastExport job failed on task " + taskAttempId);
                out.flush();
                throw new ConnectorException(22102);
            }
            if (lsn == null || lsn.isEmpty()) {
                TeradataInternalFastExportInputFormat.logger.debug((Object) "No LSN found for FastExport job, FastExport job failed.");
                out.writeUTF("FastExport job failed on task " + taskAttempId);
                out.flush();
                throw new ConnectorException(22112);
            }
            String timeConnProperties = "";
            final String url = TeradataPlugInConfiguration.getInputJdbcUrl(configuration).toLowerCase();
            if (!url.contains("/tsnano=") && !url.contains(",tsnano=")) {
                timeConnProperties += "tsnano=6,";
            }
            if (!url.contains("/tnano=") && !url.contains(",tnano=")) {
                timeConnProperties += "tnano=6,";
            }
            final boolean isGoverned = TeradataConnection.getJDBCURLParameters(url).isGoverned();
            final boolean isFastFail = TeradataPlugInConfiguration.getInputFastFail(configuration);
            final String connection_param = (isGoverned || isFastFail) ? ("LSS_TYPE=E," + TeradataInternalFastExportInputFormat.PARAM_FASTEXPORT_CONNECTION) : TeradataInternalFastExportInputFormat.PARAM_FASTEXPORT_CONNECTION;
            TeradataInternalFastExportInputFormat.logger.debug((Object) "Creating JDBC FastExport connection to DBS");
            fastExportConnection = TeradataConnection.getConnection(TeradataPlugInConfiguration.getInputJdbcDriverClass(configuration), TeradataPlugInConfiguration.getInputJdbcUrl(configuration), TeradataPlugInConfiguration.getInputTeradataUserName((JobContext) context), TeradataPlugInConfiguration.getInputTeradataPassword((JobContext) context), timeConnProperties + connection_param + lsn);
            TeradataInternalFastExportInputFormat.logger.debug((Object) "Creating JDBC connection to DBS for session control");
            String connectionString = TeradataInternalFastExportInputFormat.PARAM_SESSION_CONNECTION;
            if (!this.charset.equals("")) {
                connectionString = connectionString + ",CHARSET=" + this.charset;
            }
            sessionConnection = TeradataConnection.getConnection(TeradataPlugInConfiguration.getInputJdbcDriverClass(configuration), TeradataPlugInConfiguration.getInputJdbcUrl(configuration), TeradataPlugInConfiguration.getInputTeradataUserName((JobContext) context), TeradataPlugInConfiguration.getInputTeradataPassword((JobContext) context), connectionString);
            fastExportConnection.setAutoCommit(false);
            TeradataInternalFastExportInputFormat.logger.info((Object) ("FastExport connection is created for task " + taskAttempId));
            final String splitSql = ((TeradataInputSplit) split).getSplitSql();
            out.writeUTF(TeradataInternalFastExportInputFormat.MSG_FASTEXPORT_CONNECTION_READY);
            out.flush();
            final String message = in.readUTF();
            if (!message.contains(TeradataInternalFastExportInputFormat.MSG_FASTEXPORT_TRANSFER_BEGIN)) {
                TeradataInternalFastExportInputFormat.logger.debug((Object) ("Received unsupported message from InternalFastExport coordinator: " + message));
                throw new ConnectorException(22101);
            }
            TeradataInternalFastExportInputFormat.logger.debug((Object) "Received FastExport transfer begin message from InternalFastExport coordinator");
            final int session_uniqueid = Integer.parseInt(in.readUTF());
            TeradataInternalFastExportInputFormat.logger.debug((Object) ("Received FastExport session unique id " + session_uniqueid + "  from InternalFastExport coordinator"));
            final String submitJobDir = configuration.get("mapreduce.job.dir");
            final Path path = new Path(submitJobDir, TeradataInternalFastExportInputFormat.FASTEXPORT_PARAMS_FILE_NAME);
            final FileSystem fileSystem = path.getFileSystem(configuration);
            boolean noSpoolEnabled = true;
            String blockCountString = null;
            if (fileSystem.exists(path)) {
                final FSDataInputStream inputStream = fileSystem.open(path);
                noSpoolEnabled = Boolean.parseBoolean(inputStream.readUTF());
                blockCountString = inputStream.readUTF();
                inputStream.close();
            }
            return new TeradataInternalFastExportRecordReader(splitSql, sessionConnection, fastExportConnection, host, port, noSpoolEnabled, blockCountString, session_uniqueid);
        } catch (SQLException e2) {
            TeradataUtils.closeConnection(fastExportConnection);
            TeradataUtils.closeConnection(sessionConnection);
            throw new ConnectorException(e2.getMessage(), e2);
        } catch (UnknownHostException e3) {
            TeradataUtils.closeConnection(fastExportConnection);
            TeradataUtils.closeConnection(sessionConnection);
            throw new ConnectorException(e3.getMessage(), e3);
        } catch (ClassNotFoundException e4) {
            TeradataUtils.closeConnection(fastExportConnection);
            TeradataUtils.closeConnection(sessionConnection);
            throw new ConnectorException(e4.getMessage(), e4);
        } catch (IOException e5) {
            TeradataUtils.closeConnection(fastExportConnection);
            TeradataUtils.closeConnection(sessionConnection);
            throw new ConnectorException(e5.getMessage(), e5);
        } finally {
            this.closeSocket(socket);
        }
    }

    static {
        TeradataInternalFastExportInputFormat.logger = LogFactory.getLog((Class) TeradataInternalFastExportInputFormat.class);
        TeradataInternalFastExportInputFormat.server = null;
        TeradataInternalFastExportInputFormat.MSG_FASTEXPORT_TRANSFER_BEGIN = "FASTEXPORT_TRANSFER_BEGIN";
        TeradataInternalFastExportInputFormat.MSG_FASTEXPORT_TRANSFER_END = "FASTEXPORT_TRANSFER_END";
        TeradataInternalFastExportInputFormat.MSG_FASTEXPORT_CONNECTION_READY = "FASTEXPORT_CONNECTION_READY";
        TeradataInternalFastExportInputFormat.MSG_FASTEXPORT_CONNECTION_CLOSE = "FASTEXPORT_CONNECTION_CLOSE";
        TeradataInternalFastExportInputFormat.MSG_FASTEXPORT_PING = "FASTEXPORT_PING";
        TeradataInternalFastExportInputFormat.PARAM_FASTEXPORT_CONNECTION = "PARTITION=EXPORT,CONNECT_FUNCTION=2,LSS_TYPE=E,LOGON_SEQUENCE_NUMBER=";
        TeradataInternalFastExportInputFormat.PARAM_LSN_CONNECTION = "TMODE=TERA,CONNECT_FUNCTION=1";
        TeradataInternalFastExportInputFormat.PARAM_SESSION_CONNECTION = "TMODE=TERA";
        TeradataInternalFastExportInputFormat.SQL_GET_LSN = "{fn teradata_logon_sequence_number()}";
        TeradataInternalFastExportInputFormat.SQL_REQUEST_TRACKING_ON = "{fn teradata_provide(request_tracking_on)}";
        TeradataInternalFastExportInputFormat.SQL_REQUEST_TRACKING_OFF = "{fn teradata_provide(request_tracking_off)}";
        TeradataInternalFastExportInputFormat.SQL_PROVIDE_REQUEST_TRACKING = "{fn teradata_provide(request_tracking)}";
        TeradataInternalFastExportInputFormat.SQL_PROVIDE_REQUESTS = "{fn teradata_provide(requests)}";
        TeradataInternalFastExportInputFormat.SQL_CLEAR_REQUESTS = "{fn teradata_provide(clear_requests)}";
        TeradataInternalFastExportInputFormat.SQL_BEGIN_FASTEXPORT = "BEGIN FASTEXPORT WITH NO SPOOL";
        TeradataInternalFastExportInputFormat.SQL_END_FASTEXPORT = "END FASTEXPORT";
        TeradataInternalFastExportInputFormat.FASTEXPORT_PARAMS_FILE_NAME = "fastexportparamsfile";
        LINE_SEP = System.getProperty("line.separator");
        TeradataInternalFastExportInputFormat.SOCKET_CONNECT_TIMEOUT = 5000;
        TeradataInternalFastExportInputFormat.SOCKET_CONNECT_RETRY_MAX = 50;
        TeradataInternalFastExportInputFormat.SOCKET_CONNECT_RETRY_WAIT_MIN = 200;
        TeradataInternalFastExportInputFormat.SOCKET_CONNECT_RETRY_WAIT_RANDOM_SEED = 500;
        TeradataInternalFastExportInputFormat.SOCKET_SERVER_ACCEPT_TIME_OUT = 20000;
        TeradataInternalFastExportInputFormat.SOCKET_PING_TIME_OUT = 60000;
    }

    class InternalFastExportCoordinator implements Runnable {
        private int numSessions;
        private Configuration configuration;
        private Log logger;
        private PreparedStatement preparedStatement;

        InternalFastExportCoordinator(final int numSessions, final Configuration configuration) {
            this.logger = LogFactory.getLog((Class) InternalFastExportCoordinator.class);
            this.numSessions = numSessions;
            this.configuration = configuration;
        }

        @Override
        public void run() {
            try {
                this.beginExport(this.numSessions, this.configuration, TeradataInternalFastExportInputFormat.server);
                this.logger.debug((Object) "Begin loading complete, starting end loading logic");
                this.endExport(this.numSessions, TeradataInternalFastExportInputFormat.server);
            } catch (IOException e) {
                this.logger.error((Object) e.getMessage());
            } finally {
                try {
                    TeradataUtils.closeConnection(TeradataInternalFastExportInputFormat.lsnConnection);
                } catch (ConnectorException e2) {
                    this.logger.error((Object) e2.getMessage());
                }
                TeradataInternalFastExportInputFormat.this.closeServer(TeradataInternalFastExportInputFormat.server);
            }
        }

        public void beginExport(final int sessionCount, final Configuration configuration, final ServerSocket server) throws ConnectorException {
            boolean noSpoolEnabled = false;
            final Socket[] sockets = new Socket[sessionCount];
            try {
                server.setSoTimeout(TeradataInternalFastExportInputFormat.SOCKET_SERVER_ACCEPT_TIME_OUT);
                final long timeoutValue = TeradataPlugInConfiguration.getInputFastExportSocketTimeout(configuration);
                final long startTime = System.currentTimeMillis();
                while (TeradataInternalFastExportInputFormat.this.readyConnCount < sessionCount) {
                    if (!TeradataInternalFastExportInputFormat.getJobStatus()) {
                        this.logger.debug((Object) "TeradataInternalFastExportProcessor.jobSuccess is false");
                        throw new ConnectorException(22102);
                    }
                    try {
                        (sockets[TeradataInternalFastExportInputFormat.this.readyConnCount] = server.accept()).setTcpNoDelay(true);
                        final DataInputStream in = new DataInputStream(sockets[TeradataInternalFastExportInputFormat.this.readyConnCount++].getInputStream());
                        final String command = in.readUTF();
                        if (!command.contains(TeradataInternalFastExportInputFormat.MSG_FASTEXPORT_CONNECTION_READY)) {
                            this.logger.debug((Object) ("Received failure message from mapper: " + command));
                            throw new ConnectorException(22102);
                        }
                        this.logger.debug((Object) ("Received FastExport ready message from mapper (" + TeradataInternalFastExportInputFormat.this.readyConnCount + "/" + sessionCount + ")"));
                    } catch (SocketTimeoutException e3) {
                        final long endTime = System.currentTimeMillis();
                        if (endTime - startTime >= timeoutValue) {
                            throw new ConnectorException(22103);
                        }
                        continue;
                    }
                }
                final Statement statement = TeradataInternalFastExportInputFormat.lsnConnection.createStatement();
                final String queryString = TeradataPlugInConfiguration.getInputSplitSql(configuration);
                this.preparedStatement = TeradataInternalFastExportInputFormat.lsnConnection.prepareStatement(queryString);
                final String beginFastExport = TeradataInternalFastExportInputFormat.SQL_BEGIN_FASTEXPORT;
                this.logger.debug((Object) ("Executing begin FastExport command: " + beginFastExport));
                statement.executeUpdate(beginFastExport);
                TeradataInternalFastExportInputFormat.lsnConnection.nativeSQL(TeradataInternalFastExportInputFormat.SQL_REQUEST_TRACKING_ON);
                TeradataInternalFastExportInputFormat.lsnConnection.nativeSQL(TeradataInternalFastExportInputFormat.SQL_PROVIDE_REQUEST_TRACKING);
                this.preparedStatement.execute();
                final String requests = TeradataInternalFastExportInputFormat.lsnConnection.nativeSQL(TeradataInternalFastExportInputFormat.SQL_PROVIDE_REQUESTS);
                final String[] requestFields = requests.split(TeradataInternalFastExportInputFormat.LINE_SEP);
                for (int i = 0; i < requestFields.length; ++i) {
                    if (requestFields[i].matches(".*activity_type=.*") && requestFields[i].split("=")[1].startsWith("196")) {
                        noSpoolEnabled = true;
                        break;
                    }
                }
                TeradataInternalFastExportInputFormat.lsnConnection.nativeSQL(TeradataInternalFastExportInputFormat.SQL_REQUEST_TRACKING_OFF);
                TeradataInternalFastExportInputFormat.lsnConnection.nativeSQL(TeradataInternalFastExportInputFormat.SQL_REQUEST_TRACKING_ON);
                TeradataInternalFastExportInputFormat.lsnConnection.nativeSQL(TeradataInternalFastExportInputFormat.SQL_CLEAR_REQUESTS);
                final ArrayList<Integer> blockCountList = new ArrayList<Integer>();
                do {
                    if (noSpoolEnabled) {
                        blockCountList.add(new Integer(2147483646));
                    } else {
                        blockCountList.add(new Integer(this.preparedStatement.getUpdateCount()));
                    }
                } while (this.preparedStatement.getMoreResults() || this.preparedStatement.getUpdateCount() >= 0);
                final String submitJobDir = configuration.get("mapreduce.job.dir");
                final Path path = new Path(submitJobDir, TeradataInternalFastExportInputFormat.FASTEXPORT_PARAMS_FILE_NAME);
                final FileSystem fileSystem = path.getFileSystem(configuration);
                if (fileSystem.exists(path)) {
                    throw new ConnectorException(24002);
                }
                final String maxReplicaString = configuration.get("dfs.replication.max");
                final short maxReplicaNum = (short) ((maxReplicaString == null || maxReplicaString.isEmpty()) ? 512 : Integer.parseInt(maxReplicaString));
                final int numMappers = ConnectorConfiguration.getNumMappers(configuration);
                FSDataOutputStream outputStream;
                if (numMappers < maxReplicaNum) {
                    outputStream = fileSystem.create(path, (short) numMappers);
                } else {
                    outputStream = fileSystem.create(path, maxReplicaNum);
                }
                outputStream.writeUTF(Boolean.valueOf(noSpoolEnabled).toString());
                outputStream.writeUTF(blockCountList.toString());
                outputStream.close();
                server.setSoTimeout(TeradataInternalFastExportInputFormat.this.readyConnCount = 0);
                this.logger.debug((Object) "Signaling to mappers to begin FastExport transfer");
                while (TeradataInternalFastExportInputFormat.this.readyConnCount < sessionCount) {
                    final DataOutputStream out = new DataOutputStream(sockets[TeradataInternalFastExportInputFormat.this.readyConnCount].getOutputStream());
                    out.writeUTF(TeradataInternalFastExportInputFormat.MSG_FASTEXPORT_TRANSFER_BEGIN);
                    out.writeUTF(Integer.valueOf(TeradataInternalFastExportInputFormat.this.readyConnCount++).toString());
                    out.flush();
                }
                TeradataInternalFastExportInputFormat.this.readyConnCount = 0;
            } catch (SQLException e) {
                throw new ConnectorException(e.getMessage(), e);
            } catch (IOException e2) {
                throw new ConnectorException(e2.getMessage(), e2);
            } finally {
                for (int j = 0; j < sockets.length; ++j) {
                    TeradataInternalFastExportInputFormat.this.closeSocket(sockets[j]);
                    sockets[j] = null;
                }
            }
        }

        private void sendPingMsg(final DataOutputStream[] outs) throws IOException {
            for (int i = 0; i < outs.length; ++i) {
                if (outs[i] != null) {
                    outs[i].writeUTF(TeradataInternalFastExportInputFormat.MSG_FASTEXPORT_PING);
                    outs[i].flush();
                }
            }
        }

        public void endExport(final int sessionCount, final ServerSocket server) throws ConnectorException {
            final Socket[] sockets = new Socket[sessionCount];
            try {
                final DataOutputStream[] outs = new DataOutputStream[sessionCount];
                server.setSoTimeout(TeradataInternalFastExportInputFormat.SOCKET_PING_TIME_OUT);
                TeradataInternalFastExportInputFormat.this.readySessionCount = 0;
                while (TeradataInternalFastExportInputFormat.this.readySessionCount < sessionCount) {
                    if (!TeradataInternalFastExportInputFormat.getJobStatus()) {
                        this.logger.debug((Object) "TeradataInternalFastExportProcessor.jobSuccess is false");
                        throw new ConnectorException(22102);
                    }
                    try {
                        (sockets[TeradataInternalFastExportInputFormat.this.readySessionCount] = server.accept()).setTcpNoDelay(true);
                    } catch (SocketTimeoutException e3) {
                        this.sendPingMsg(outs);
                        continue;
                    }
                    final DataInputStream in = new DataInputStream(sockets[TeradataInternalFastExportInputFormat.this.readySessionCount].getInputStream());
                    outs[TeradataInternalFastExportInputFormat.this.readySessionCount] = new DataOutputStream(sockets[TeradataInternalFastExportInputFormat.this.readySessionCount].getOutputStream());
                    final String command = in.readUTF();
                    if (!command.contains(TeradataInternalFastExportInputFormat.MSG_FASTEXPORT_TRANSFER_END)) {
                        this.logger.debug((Object) ("Received failure message from mapper: " + command));
                        throw new ConnectorException(22102);
                    }
                    final TeradataInternalFastExportInputFormat this$0 = TeradataInternalFastExportInputFormat.this;
                    ++this$0.readySessionCount;
                    this.logger.debug((Object) ("Received FastExport complete message from mapper (" + TeradataInternalFastExportInputFormat.this.readySessionCount + "/" + sessionCount + ")"));
                }
                server.setSoTimeout(0);
                this.preparedStatement.close();
                final Statement statement = TeradataInternalFastExportInputFormat.lsnConnection.createStatement();
                this.logger.debug((Object) ("Executing end export command: " + TeradataInternalFastExportInputFormat.SQL_END_FASTEXPORT));
                statement.executeUpdate(TeradataInternalFastExportInputFormat.SQL_END_FASTEXPORT);
                statement.close();
                this.logger.debug((Object) "Signaling to mappers FastExport transfer is complete");
                TeradataInternalFastExportInputFormat.this.readySessionCount = 0;
                while (TeradataInternalFastExportInputFormat.this.readySessionCount < sessionCount) {
                    outs[TeradataInternalFastExportInputFormat.this.readySessionCount].writeUTF(TeradataInternalFastExportInputFormat.MSG_FASTEXPORT_CONNECTION_CLOSE);
                    outs[TeradataInternalFastExportInputFormat.this.readySessionCount].flush();
                    final TeradataInternalFastExportInputFormat this$2 = TeradataInternalFastExportInputFormat.this;
                    ++this$2.readySessionCount;
                }
            } catch (SQLException e) {
                throw new ConnectorException(e.getMessage(), e);
            } catch (IOException e2) {
                throw new ConnectorException(e2.getMessage(), e2);
            } finally {
                for (int i = 0; i < sockets.length; ++i) {
                    TeradataInternalFastExportInputFormat.this.closeSocket(sockets[i]);
                    sockets[i] = null;
                }
            }
        }
    }

    public class TeradataInternalFastExportRecordReader extends RecordReader<LongWritable, ConnectorRecord> {
        private Log logger;
        private Socket socket;
        private DataInputStream in;
        private DataOutputStream out;
        private String host;
        private String blockCountString;
        private int port;
        private boolean noSpoolEnabled;
        private ArrayList<Integer> blockCountList;
        private Connection sessionConnection;
        private Connection fastExportConnection;
        private PreparedStatement sessionPreparedStatement;
        private PreparedStatement fastExportPreparedStatement;
        private ArrayList<ResultSetMetaData> metaDataList;
        private long resultCount;
        private TeradataObjectArrayWritable curValue;
        private ResultSet resultset;
        private String splitSQL;
        private long end_timestamp;
        private int uniqueId;
        private int blkNum;
        private long start_timestamp;
        private String[] sourceFields;
        private int[] sourceFieldMapping;
        private Configuration configuration;

        public TeradataInternalFastExportRecordReader(final String splitSQL, final Connection sessionConnection, final Connection fastExportConnection, final String host, final int port, final boolean noSpoolEnabled, final String blockCountString, final int uniqueid) {
            this.logger = LogFactory.getLog((Class) TeradataInternalFastExportRecordReader.class);
            this.in = null;
            this.out = null;
            this.host = null;
            this.blockCountString = null;
            this.port = 0;
            this.noSpoolEnabled = true;
            this.blockCountList = null;
            this.sessionConnection = null;
            this.fastExportConnection = null;
            this.sessionPreparedStatement = null;
            this.fastExportPreparedStatement = null;
            this.metaDataList = null;
            this.resultCount = 0L;
            this.curValue = null;
            this.resultset = null;
            this.end_timestamp = 0L;
            this.uniqueId = 0;
            this.blkNum = 1;
            this.start_timestamp = 0L;
            this.sourceFields = null;
            this.sourceFieldMapping = null;
            this.sessionConnection = sessionConnection;
            this.fastExportConnection = fastExportConnection;
            this.splitSQL = splitSQL;
            this.host = host;
            this.port = port;
            this.noSpoolEnabled = noSpoolEnabled;
            this.blockCountString = blockCountString;
            this.uniqueId = uniqueid;
            this.start_timestamp = System.currentTimeMillis();
            this.logger.info((Object) ("recordreader class " + this.getClass().getName() + "initialize time is:  " + this.start_timestamp));
        }

        public void initialize(final InputSplit split, final TaskAttemptContext context) throws IOException, InterruptedException {
            this.configuration = context.getConfiguration();
            this.blockCountList = new ArrayList<Integer>();
            final String[] strBlockCount = this.blockCountString.replaceAll("[\\[\\]]", "").split(",");
            for (int i = 0; i < strBlockCount.length; ++i) {
                this.blockCountList.add(new Integer(strBlockCount[i].trim()));
            }
            try {
                this.sessionPreparedStatement = this.sessionConnection.prepareStatement(this.splitSQL);
                this.fastExportPreparedStatement = this.fastExportConnection.prepareStatement(null);
                this.metaDataList = new ArrayList<ResultSetMetaData>();
                do {
                    this.metaDataList.add(this.sessionPreparedStatement.getMetaData());
                } while (this.sessionPreparedStatement.getMoreResults());
                final String sourceTableDescText = TeradataPlugInConfiguration.getInputTableDesc(this.configuration);
                this.sourceFields = TeradataPlugInConfiguration.getInputFieldNamesArray(this.configuration);
                this.sourceFieldMapping = TeradataSchemaUtils.lookupMappingFromTableDescText(sourceTableDescText, this.sourceFields);
                this.curValue = new TeradataObjectArrayWritable(this.sourceFieldMapping.length);
                final String[] typeNames = TeradataSchemaUtils.lookupTypeNamesFromTableDescText(sourceTableDescText, this.sourceFields);
                this.curValue.setRecordTypes(typeNames);
                final ResultSetMetaData metaData = this.metaDataList.get(0);
                this.fastExportPreparedStatement.setObject(1, metaData);
            } catch (SQLException e) {
                throw new ConnectorException(e.getMessage(), e);
            }
        }

        public LongWritable getCurrentKey() throws IOException, InterruptedException {
            return new LongWritable(this.resultCount);
        }

        public ConnectorRecord getCurrentValue() throws IOException, InterruptedException {
            return this.curValue;
        }

        public float getProgress() throws IOException, InterruptedException {
            return 0.0f;
        }

        public boolean nextKeyValue() throws IOException, InterruptedException {
            try {
                boolean exitLoop = false;
                final int res = 0;
                final int blkCnt = this.blockCountList.get(res);
                long stmtNum = 0L;
                final int actualFxpSessions = ConnectorConfiguration.getNumMappers(this.configuration);
                do {
                    if (this.resultset == null) {
                        if (this.noSpoolEnabled) {
                            this.fastExportPreparedStatement.setInt(2, 1);
                        } else {
                            if (this.blkNum == 1) {
                                this.blkNum = 1 + this.uniqueId;
                            }
                            if (this.blkNum > blkCnt) {
                                return false;
                            }
                            stmtNum = (long) (res + 1) << 32;
                            this.fastExportPreparedStatement.setLong(2, stmtNum + this.blkNum);
                            this.blkNum += actualFxpSessions;
                        }
                        try {
                            this.resultset = this.fastExportPreparedStatement.executeQuery();
                        } catch (SQLException e) {
                            SQLException se = e;
                            boolean endOfFastExport = false;
                            while (se != null) {
                                if (this.noSpoolEnabled && se.getErrorCode() == 9112) {
                                    endOfFastExport = true;
                                    break;
                                }
                                se = se.getNextException();
                            }
                            if (endOfFastExport) {
                                return false;
                            }
                            throw new ConnectorException(e.getMessage(), e);
                        }
                    }
                    if (this.resultset.next()) {
                        this.curValue.readFields(this.resultset);
                        ++this.resultCount;
                        exitLoop = true;
                    } else {
                        this.resultset.close();
                        this.resultset = null;
                    }
                } while (!exitLoop);
            } catch (SQLException e2) {
                throw new ConnectorException(e2.getMessage(), e2);
            }
            return true;
        }

        public void close() throws IOException {
            int retryCount = 0;
            this.logger.debug((Object) "Attempting to connect to InternalFastload coordinator to signal data transfer complete");
            while (this.socket == null) {
                try {
                    (this.socket = new Socket(this.host, this.port)).setTcpNoDelay(true);
                    this.logger.debug((Object) "Connected to InternalFastExport coordinator successfully");
                } catch (ConnectException e) {
                    if (++retryCount >= TeradataInternalFastExportInputFormat.SOCKET_CONNECT_RETRY_MAX) {
                        throw new ConnectorException(e.getMessage(), e);
                    }
                    this.socket = null;
                    try {
                        final int waitSeconds = new Random().nextInt(TeradataInternalFastExportInputFormat.SOCKET_CONNECT_RETRY_WAIT_RANDOM_SEED) + TeradataInternalFastExportInputFormat.SOCKET_CONNECT_RETRY_WAIT_MIN;
                        Thread.sleep(waitSeconds);
                    } catch (InterruptedException e3) {
                        this.logger.debug((Object) "InterruptedException occurred during random sleep between retries");
                    }
                }
            }
            this.in = new DataInputStream(this.socket.getInputStream());
            this.out = new DataOutputStream(this.socket.getOutputStream());
            try {
                if (this.sessionPreparedStatement != null) {
                    this.sessionPreparedStatement.close();
                    this.sessionPreparedStatement = null;
                }
                if (this.fastExportPreparedStatement != null) {
                    this.fastExportPreparedStatement.clearParameters();
                    this.fastExportPreparedStatement.close();
                    this.fastExportPreparedStatement = null;
                }
            } catch (SQLException e2) {
                throw new ConnectorException(e2.getMessage(), e2);
            }
            this.logger.debug((Object) "Signaling to InternalFastExport coordinator data transfer is complete for task");
            this.out.writeUTF(TeradataInternalFastExportInputFormat.MSG_FASTEXPORT_TRANSFER_END);
            this.out.flush();
            String message;
            while (true) {
                message = this.in.readUTF();
                if (!message.equals(TeradataInternalFastExportInputFormat.MSG_FASTEXPORT_PING)) {
                    break;
                }
                this.logger.info((Object) "ping message received. ignore.");
            }
            if (!message.equals(TeradataInternalFastExportInputFormat.MSG_FASTEXPORT_CONNECTION_CLOSE)) {
                this.logger.debug((Object) ("Received unsupported message from InternalFastExport coordinator: " + message));
                throw new IOException("FastExport: message " + TeradataInternalFastExportInputFormat.MSG_FASTEXPORT_CONNECTION_CLOSE + " expected");
            }
            this.socket.close();
            TeradataUtils.closeConnection(this.fastExportConnection);
            TeradataUtils.closeConnection(this.sessionConnection);
            this.end_timestamp = System.currentTimeMillis();
            this.logger.info((Object) ("TeradataInternalFastExportRecordReader ended at: " + this.end_timestamp));
            this.logger.info((Object) ("TeradataInternalFastExportRecordReader duration in seconds: " + (this.end_timestamp - this.start_timestamp) / 1000L));
        }
    }
}
