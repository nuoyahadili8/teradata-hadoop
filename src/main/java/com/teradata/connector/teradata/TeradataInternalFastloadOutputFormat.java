package com.teradata.connector.teradata;

import com.teradata.connector.common.exception.ConnectorException;
import com.teradata.connector.common.exception.ConnectorException.ErrorCode;
import com.teradata.connector.common.utils.ConnectorConfiguration;
import com.teradata.connector.common.utils.HadoopConfigurationUtils;
import com.teradata.connector.hive.utils.HiveUtils;
import com.teradata.connector.teradata.db.TeradataConnection;
import com.teradata.connector.teradata.processor.TeradataInternalFastloadProcessor;
import com.teradata.connector.teradata.schema.TeradataColumnDesc;
import com.teradata.connector.teradata.schema.TeradataTableDesc;
import com.teradata.connector.teradata.utils.LogContainer;
import com.teradata.connector.teradata.utils.TeradataPlugInConfiguration;
import com.teradata.connector.teradata.utils.TeradataSchemaUtils;
import com.teradata.connector.teradata.utils.TeradataUtils;
import com.teradata.jdbc.jdbc_4.TDSession;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.net.*;
import java.sql.BatchUpdateException;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Random;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.InputFormat;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.OutputCommitter;
import org.apache.hadoop.mapreduce.OutputFormat;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.db.DBWritable;
import org.apache.hadoop.mapreduce.lib.output.FileOutputCommitter;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.ReflectionUtils;

public class TeradataInternalFastloadOutputFormat<K, V extends DBWritable> extends OutputFormat<K, DBWritable> {
    private static Log logger;
    public static String MSG_FASTLOAD_TRANSFER_BEGIN;
    public static String MSG_FASTLOAD_TRANSFER_END;
    public static String MSG_FASTLOAD_CONNECTION_READY;
    public static String MSG_FASTLOAD_CONNECTION_CLOSE;
    protected static String PARAM_FASTLOAD_CONNECTION;
    protected static String SQL_GET_LSN;
    protected static String PARAM_LSN_CONNECTION;
    protected static String SQL_FASTLOAD_DATAFORM;
    protected static String SQL_CHECKPOINT_LOADING_END;
    protected static String SQL_BEGIN_LOADING;
    protected static String SQL_END_LOADING;
    protected static String FASTLOAD_FILE_NAME;
    private static final String CHECK_WORKLOAD = "CHECK WORKLOAD FOR ";
    private static final String CHECK_WORKLOAD_END = "CHECK WORKLOAD END";
    private static final String LSS_TYPE = "LSS_TYPE=L";
    private static final String FASTLOAD_QUERY_BAND = "UtilityName=JDBCL;";
    public static String MSG_FASTLOAD_PING;
    public static int SOCKET_CONNECT_TIMEOUT;
    public static int SOCKET_CONNECT_RETRY_MAX;
    public static int SOCKET_CONNECT_RETRY_WAIT_MIN;
    public static int SOCKET_CONNECT_RETRY_WAIT_RANDOM_SEED;
    protected static int SOCKET_SERVER_ACCEPT_TIME_OUT;
    protected static Connection lsnConnection;
    protected static ServerSocket server;
    protected String charset;
    protected int readyConnCount;
    protected int readyTaskCount;
    protected static int SOCKET_PING_TIME_OUT;

    public TeradataInternalFastloadOutputFormat() {
        this.charset = "";
        this.readyConnCount = 0;
        this.readyTaskCount = 0;
    }

    protected void validateConfiguration(final JobContext context) throws ConnectorException {
        final Configuration configuration = context.getConfiguration();
        final TeradataConnection connection = TeradataUtils.openOutputConnection(context);
        final String outputProcessor = ConnectorConfiguration.getPlugInOutputProcessor(configuration);
        if (outputProcessor.isEmpty()) {
            TeradataUtils.validateOutputTeradataProperties(configuration, connection);
            TeradataSchemaUtils.setupTeradataTargetTableSchema(configuration, connection);
        }
        connection.close();
    }

    private void configFastloadConnectivity(final JobContext context, final int numTasks) throws SQLException, IOException {
        final Configuration configuration = context.getConfiguration();
        String host = TeradataPlugInConfiguration.getOutputFastloadSocketHost(configuration);
        int port = TeradataPlugInConfiguration.getOutputFastloadSocketPort(configuration);
        final String lsnNumber = TeradataInternalFastloadOutputFormat.lsnConnection.nativeSQL(TeradataInternalFastloadOutputFormat.SQL_GET_LSN);
        if (lsnNumber == null || lsnNumber.length() == 0) {
            throw new SQLException("lsn number missing");
        }
        TeradataInternalFastloadOutputFormat.logger.debug((Object) ("fastload lsn number is " + lsnNumber));
        TeradataPlugInConfiguration.setOutputFastloadLsn(configuration, lsnNumber);
        if (host.isEmpty() || host.equalsIgnoreCase("default") || host.equalsIgnoreCase("localhost")) {
            host = HadoopConfigurationUtils.getClusterNodeInterface(context);
            TeradataPlugInConfiguration.setOutputFastloadSocketHost(configuration, host);
        }
        if (port == 0) {
            port = TeradataInternalFastloadOutputFormat.server.getLocalPort();
            TeradataPlugInConfiguration.setOutputFastloadSocketPort(configuration, port);
        }
        final String submitJobDir = configuration.get("mapreduce.job.dir");
        if (submitJobDir == null || submitJobDir.trim().equals("")) {
            return;
        }
        final Path path = new Path(submitJobDir, TeradataInternalFastloadOutputFormat.FASTLOAD_FILE_NAME);
        final FileSystem fileSystem = path.getFileSystem(configuration);
        if (fileSystem.exists(path)) {
            throw new ConnectorException(24001);
        }
        final String maxReplicaString = configuration.get("dfs.replication.max");
        final short maxReplicaNum = (short) ((maxReplicaString == null || maxReplicaString.isEmpty()) ? 512 : Integer.parseInt(maxReplicaString));
        FSDataOutputStream outputStream;
        if (numTasks < maxReplicaNum) {
            outputStream = fileSystem.create(path, (short) numTasks);
        } else {
            outputStream = fileSystem.create(path, maxReplicaNum);
        }
        outputStream.writeUTF(lsnNumber);
        outputStream.writeUTF(host);
        outputStream.writeUTF(Integer.valueOf(port).toString());
        outputStream.close();
    }

    @Override
    public void checkOutputSpecs(final JobContext context) throws IOException, InterruptedException {
        final Configuration configuration = context.getConfiguration();
        if (ConnectorConfiguration.getPlugInOutputProcessor(configuration).isEmpty()) {
            this.validateConfiguration(context);
        }
        final String classname = TeradataPlugInConfiguration.getOutputJdbcDriverClass(configuration);
        final String url = TeradataPlugInConfiguration.getOutputJdbcUrl(configuration);
        final boolean isFastFail = TeradataPlugInConfiguration.getOutputFastFail(configuration);
        try {
            final boolean isGoverned = TeradataConnection.getJDBCURLParameters(url).isGoverned();
            TeradataInternalFastloadOutputFormat.server = HadoopConfigurationUtils.createServerSocket(TeradataPlugInConfiguration.getOutputFastloadSocketPort(configuration), TeradataPlugInConfiguration.getOutputFastloadSocketBacklog(configuration));
            final boolean enableUnicodePassthrough = TeradataPlugInConfiguration.getUnicodePassthrough(configuration);
            TeradataInternalFastloadOutputFormat.lsnConnection = TeradataConnection.getConnection(classname, url, TeradataPlugInConfiguration.getOutputTeradataUserName(context), TeradataPlugInConfiguration.getOutputTeradataPassword(context), (isGoverned || isFastFail) ? ("LSS_TYPE=L," + TeradataInternalFastloadOutputFormat.PARAM_LSN_CONNECTION) : TeradataInternalFastloadOutputFormat.PARAM_LSN_CONNECTION, enableUnicodePassthrough);
            final String queryBandProperty = TeradataPlugInConfiguration.getOutputQueryBand(configuration) + ((isGoverned || isFastFail) ? "UtilityName=JDBCL;" : "");
            TeradataUtils.validateQueryBand(queryBandProperty);
            TeradataConnection.setQueryBandProperty(TeradataInternalFastloadOutputFormat.lsnConnection, queryBandProperty);
            final int TASMSessnNum = getFastloadSessionCount(configuration);
            ConnectorConfiguration.setNumMappers(configuration, TASMSessnNum);
            final int numTasks = this.validateAndGetTaskNumber(context);
            if (numTasks == 0) {
                return;
            }
            this.charset = TeradataConnection.getURLParamValue(url, "CHARSET");
            if (this.charset == null || (!this.charset.equalsIgnoreCase("UTF8") && !this.charset.equalsIgnoreCase("UTF16"))) {
                this.charset = "";
            }
            this.configFastloadConnectivity(context, numTasks);
            TeradataInternalFastloadOutputFormat.logger.info((Object) ("started load task: " + numTasks));
            final InternalFastloadCoordinator coordinator = new InternalFastloadCoordinator(numTasks, configuration);
            final Thread thread = new Thread(coordinator);
            thread.start();
        } catch (SQLException e) {
            TeradataUtils.closeConnection(TeradataInternalFastloadOutputFormat.lsnConnection);
            this.closeServer(TeradataInternalFastloadOutputFormat.server);
            throw new ConnectorException(e.getMessage(), e);
        } catch (ClassNotFoundException e2) {
            TeradataUtils.closeConnection(TeradataInternalFastloadOutputFormat.lsnConnection);
            this.closeServer(TeradataInternalFastloadOutputFormat.server);
            throw new ConnectorException(e2.getMessage(), e2);
        }
    }

    public RecordWriter<K, DBWritable> getRecordWriter(final TaskAttemptContext context) throws IOException, InterruptedException {
        final Configuration configuration = context.getConfiguration();
        final String submitJobHost = configuration.get("mapreduce.job.submithostname");
        final String submitJobDir = configuration.get("mapreduce.job.dir");
        final Path path = new Path(submitJobDir, TeradataInternalFastloadOutputFormat.FASTLOAD_FILE_NAME);
        final FileSystem fileSystem = path.getFileSystem(configuration);
        String lsn;
        String host;
        int port;
        if (fileSystem.exists(path)) {
            final FSDataInputStream inputStream = fileSystem.open(path);
            lsn = inputStream.readUTF();
            host = inputStream.readUTF();
            port = Integer.parseInt(inputStream.readUTF());
            inputStream.close();
        } else {
            lsn = TeradataPlugInConfiguration.getOutputFastloadLsn(configuration);
            host = TeradataPlugInConfiguration.getOutputFastloadSocketHost(configuration);
            port = TeradataPlugInConfiguration.getOutputFastloadSocketPort(configuration);
        }
        TeradataInternalFastloadOutputFormat.logger.debug((Object) ("fastload mapper socket host is " + host));
        TeradataInternalFastloadOutputFormat.logger.debug((Object) ("fastload mapper socket port is " + port));
        Socket socket = null;
        Connection fastLoadConnection = null;
        try {
            int retryCount = 0;
            while (socket == null) {
                try {
                    socket = new Socket();
                    socket.connect(new InetSocketAddress(host, port), TeradataInternalFastloadOutputFormat.SOCKET_CONNECT_TIMEOUT);
                    socket.setTcpNoDelay(true);
                    TeradataInternalFastloadOutputFormat.logger.debug((Object) "Connection to InternalFastload coordinator successful");
                } catch (Exception e) {
                    if (++retryCount >= TeradataInternalFastloadOutputFormat.SOCKET_CONNECT_RETRY_MAX) {
                        TeradataInternalFastloadOutputFormat.logger.debug((Object) ("After " + retryCount + " retries, unable to connect to InternalFastload coordinator"));
                        throw new ConnectorException(e.getMessage(), e);
                    }
                    socket = null;
                    if (!submitJobHost.isEmpty() && retryCount % 10 == 0) {
                        try {
                            socket = new Socket();
                            socket.connect(new InetSocketAddress(submitJobHost, port), TeradataInternalFastloadOutputFormat.SOCKET_CONNECT_TIMEOUT);
                            socket.setTcpNoDelay(true);
                            host = submitJobHost;
                            TeradataInternalFastloadOutputFormat.logger.debug((Object) "Connection to InternalFastload coordinator successful via mapreduce.job.submithostname");
                        } catch (Exception e6) {
                            TeradataInternalFastloadOutputFormat.logger.debug((Object) ("Caught exception " + e.getMessage()));
                        }
                    }
                    try {
                        final int waitSeconds = new Random().nextInt(TeradataInternalFastloadOutputFormat.SOCKET_CONNECT_RETRY_WAIT_RANDOM_SEED) + TeradataInternalFastloadOutputFormat.SOCKET_CONNECT_RETRY_WAIT_MIN;
                        Thread.sleep(waitSeconds);
                    } catch (InterruptedException e7) {
                        TeradataInternalFastloadOutputFormat.logger.debug((Object) "InterruptedException occurred during random sleep between retries");
                    }
                }
            }
            final DataInputStream in = new DataInputStream(socket.getInputStream());
            final DataOutputStream out = new DataOutputStream(socket.getOutputStream());
            final String taskAttempId = context.getTaskAttemptID().toString();
            if (context.getTaskAttemptID().getId() > 0) {
                TeradataInternalFastloadOutputFormat.logger.debug((Object) "Current task attempt ID is > 0, internal.fastload doesn't support preemption/failover, fastload job failed");
                out.writeUTF("Fastload job failed on task " + taskAttempId);
                out.flush();
                throw new ConnectorException(22002);
            }
            if (lsn == null || lsn.isEmpty()) {
                TeradataInternalFastloadOutputFormat.logger.debug((Object) "No LSN found for fastload job, fastload job failed.");
                out.writeUTF("Fastload job failed on task " + taskAttempId);
                out.flush();
                throw new ConnectorException(22012);
            }
            TeradataInternalFastloadOutputFormat.logger.debug((Object) ("fastload task lsn number is " + lsn));
            String timeConnProperties = "";
            final String url = TeradataPlugInConfiguration.getOutputJdbcUrl(configuration).toLowerCase();
            if (!url.contains("/tsnano=") && !url.contains(",tsnano=")) {
                timeConnProperties += "tsnano=6,";
            }
            if (!url.contains("/tnano=") && !url.contains(",tnano=")) {
                timeConnProperties += "tnano=6,";
            }
            final boolean isGoverned = TeradataConnection.getJDBCURLParameters(url).isGoverned();
            final boolean isFastFail = TeradataPlugInConfiguration.getOutputFastFail(configuration);
            final String connection_param = (isGoverned || isFastFail) ? ("LSS_TYPE=L," + TeradataInternalFastloadOutputFormat.PARAM_FASTLOAD_CONNECTION) : TeradataInternalFastloadOutputFormat.PARAM_FASTLOAD_CONNECTION;
            TeradataInternalFastloadOutputFormat.logger.debug((Object) "Creating JDBC fastload connection to DBS");
            fastLoadConnection = TeradataConnection.getConnection(TeradataPlugInConfiguration.getOutputJdbcDriverClass(configuration), TeradataPlugInConfiguration.getOutputJdbcUrl(configuration), TeradataPlugInConfiguration.getOutputTeradataUserName((JobContext) context), TeradataPlugInConfiguration.getOutputTeradataPassword((JobContext) context), timeConnProperties + connection_param + lsn);
            fastLoadConnection.setAutoCommit(false);
            final PreparedStatement prepareStatement = fastLoadConnection.prepareStatement(null);
            TeradataInternalFastloadOutputFormat.logger.info((Object) ("fastload connection is created for task " + taskAttempId));
            out.writeUTF(TeradataInternalFastloadOutputFormat.MSG_FASTLOAD_CONNECTION_READY);
            out.flush();
            final String message = in.readUTF();
            if (!message.contains(TeradataInternalFastloadOutputFormat.MSG_FASTLOAD_TRANSFER_BEGIN)) {
                TeradataInternalFastloadOutputFormat.logger.debug((Object) ("Received unsupported message from InternalFastload coordinator: " + message));
                throw new ConnectorException(22001);
            }
            TeradataInternalFastloadOutputFormat.logger.debug((Object) "Received fastload transfer begin message from InternalFastload coordinator");
            final int batchSize = TeradataPlugInConfiguration.getOutputBatchSize(configuration);
            return new TeradataInternalFastLoadRecordWriter(fastLoadConnection, prepareStatement, batchSize, host, port);
        } catch (SQLException e2) {
            throw new ConnectorException(e2.getMessage(), e2);
        } catch (UnknownHostException e3) {
            throw new ConnectorException(e3.getMessage(), e3);
        } catch (ClassNotFoundException e4) {
            throw new ConnectorException(e4.getMessage(), e4);
        } catch (IOException e5) {
            TeradataUtils.closeConnection(fastLoadConnection);
            fastLoadConnection = null;
            throw new ConnectorException(e5.getMessage(), e5);
        } finally {
            this.closeSocket(socket);
            socket = null;
        }
    }

    public OutputCommitter getOutputCommitter(final TaskAttemptContext context) throws IOException, InterruptedException {
        return (OutputCommitter) new FileOutputCommitter(FileOutputFormat.getOutputPath((JobContext) context), context);
    }

    protected void closeServer(final ServerSocket server) {
        if (server != null && !server.isClosed()) {
            try {
                server.close();
            } catch (IOException e1) {
                TeradataInternalFastloadOutputFormat.logger.debug((Object) e1.getMessage());
            }
        }
    }

    protected void closeSocket(final Socket socket) {
        if (socket != null && !socket.isClosed()) {
            try {
                socket.close();
            } catch (IOException e1) {
                TeradataInternalFastloadOutputFormat.logger.debug((Object) e1.getMessage());
            }
        }
    }

    public static boolean getJobStatus() {
        synchronized (TeradataInternalFastloadProcessor.class) {
            return TeradataInternalFastloadProcessor.jobSuccess;
        }
    }

    private static int getFastloadSessionCount(final Configuration configuration) throws SQLException {
        final TDSession session = (TDSession) TeradataInternalFastloadOutputFormat.lsnConnection;
        final int numMappers = ConnectorConfiguration.getNumMappers(configuration);
        final boolean isGoverned = session.getURLParameters().isGoverned();
        final String check_workload_end = (isGoverned ? "" : "{fn TERADATA_FAILFAST}") + "CHECK WORKLOAD END";
        if (session.useCheckWorkload()) {
            final String outputDatabase = TeradataPlugInConfiguration.getOutputDatabase(configuration);
            String errorTableDatabase = TeradataPlugInConfiguration.getOutputErrorTableDatabase(configuration);
            if (errorTableDatabase.isEmpty()) {
                errorTableDatabase = outputDatabase;
            }
            final String errorTable1 = TeradataConnection.getQuotedEscapedName(errorTableDatabase, TeradataPlugInConfiguration.getOutputErrorTable1Name(configuration));
            final String errorTable2 = TeradataConnection.getQuotedEscapedName(errorTableDatabase, TeradataPlugInConfiguration.getOutputErrorTable2Name(configuration));
            String outputTableName = TeradataPlugInConfiguration.getOutputTable(configuration);
            outputTableName = TeradataConnection.getQuotedEscapedName(outputDatabase, outputTableName);
            final String checkWorkLoad = "CHECK WORKLOAD FOR " + TeradataInternalFastloadOutputFormat.SQL_BEGIN_LOADING + " " + outputTableName + " ERRORFILES " + errorTable1 + ", " + errorTable2 + " WITH INTERVAL";
            final Statement controlStmt = TeradataInternalFastloadOutputFormat.lsnConnection.createStatement();
            controlStmt.executeUpdate(checkWorkLoad);
            final ResultSet rs = controlStmt.executeQuery(check_workload_end);
            final ResultSetMetaData rsmd = rs.getMetaData();
            if (rs.next() && rsmd.getColumnCount() >= 2 && rs.getString(1) != null) {
                if (rs.getString(1).trim().equalsIgnoreCase("Y")) {
                    final int count = rs.getInt(2);
                    if (count > 0) {
                        final int numSugSessn = Math.min(numMappers, count);
                        if (numSugSessn < numMappers) {
                            TeradataInternalFastloadOutputFormat.logger.info((Object) ("user provided number of Mappers [" + numMappers + "] is overridden by [" + numSugSessn + "] returned from DBS"));
                        } else {
                            TeradataInternalFastloadOutputFormat.logger.info((Object) ("user provided number of Mappers is NOT overridden by [" + count + "] DBS."));
                        }
                        return numSugSessn;
                    }
                    TeradataInternalFastloadOutputFormat.logger.info((Object) ("invalid number " + count + " returned from DBS"));
                } else {
                    TeradataInternalFastloadOutputFormat.logger.info((Object) "returned TASM-flag is N");
                }
            } else {
                TeradataInternalFastloadOutputFormat.logger.info((Object) "unrecognized column returned");
            }
            rs.close();
            controlStmt.close();
        } else {
            TeradataInternalFastloadOutputFormat.logger.info((Object) "TDCH is not controlled by TASM");
        }
        return numMappers;
    }

    private int validateAndGetTaskNumber(final JobContext context) throws IOException, InterruptedException {
        final Configuration configuration = context.getConfiguration();
        int numTasks = ConnectorConfiguration.getNumReducers(configuration);
        final int numAmps = TeradataPlugInConfiguration.getOutputNumAmps(configuration);
        if (numTasks == 0) {
            try {
                numTasks = ((InputFormat) ReflectionUtils.newInstance(context.getInputFormatClass(), configuration)).getSplits(context).size();
            } catch (ClassNotFoundException e) {
                throw new ConnectorException(e.getMessage(), e);
            }
            final int maxMapTasks = HadoopConfigurationUtils.getMaxMapTasks(context);
            if (numTasks == 0) {
                return 0;
            }
            if (numTasks > maxMapTasks && maxMapTasks > 0) {
                TeradataInternalFastloadOutputFormat.logger.warn((Object) ("The number of map tasks requested (" + numTasks + ") is greater than the maximum number of map tasks supported by the Hadoop cluster; in some situations the TDCH job may result in deadlock due to the internal.fastload requirement that all map tasks are run concurrently"));
            }
        } else {
            final int maxReduceTasks = HadoopConfigurationUtils.getMaxReduceTasks(context);
            if (numTasks > maxReduceTasks && maxReduceTasks > 0) {
                throw new ConnectorException(22005);
            }
        }
        if (numTasks > numAmps) {
            throw new ConnectorException(22006);
        }
        return numTasks;
    }

    static {
        TeradataInternalFastloadOutputFormat.logger = LogFactory.getLog((Class) TeradataInternalFastloadOutputFormat.class);
        TeradataInternalFastloadOutputFormat.MSG_FASTLOAD_TRANSFER_BEGIN = "FASTLOAD_TRANSFER_BEGIN";
        TeradataInternalFastloadOutputFormat.MSG_FASTLOAD_TRANSFER_END = "FASTLOAD_TRANSFER_END";
        TeradataInternalFastloadOutputFormat.MSG_FASTLOAD_CONNECTION_READY = "FASTLOAD_CONNECTION_READY";
        TeradataInternalFastloadOutputFormat.MSG_FASTLOAD_CONNECTION_CLOSE = "FASTLOAD_CONNECTION_CLOSE";
        TeradataInternalFastloadOutputFormat.PARAM_FASTLOAD_CONNECTION = "PARTITION=FASTLOAD,CONNECT_FUNCTION=2,LOGON_SEQUENCE_NUMBER=";
        TeradataInternalFastloadOutputFormat.SQL_GET_LSN = "{fn teradata_logon_sequence_number()}";
        TeradataInternalFastloadOutputFormat.PARAM_LSN_CONNECTION = "TMODE=TERA,CONNECT_FUNCTION=1";
        TeradataInternalFastloadOutputFormat.SQL_FASTLOAD_DATAFORM = "SET SESSION DateForm = IntegerDate";
        TeradataInternalFastloadOutputFormat.SQL_CHECKPOINT_LOADING_END = "CHECKPOINT LOADING END";
        TeradataInternalFastloadOutputFormat.SQL_BEGIN_LOADING = "BEGIN LOADING";
        TeradataInternalFastloadOutputFormat.SQL_END_LOADING = "END LOADING";
        TeradataInternalFastloadOutputFormat.FASTLOAD_FILE_NAME = "fastloadfile";
        TeradataInternalFastloadOutputFormat.MSG_FASTLOAD_PING = "FASTLOAD_PING";
        TeradataInternalFastloadOutputFormat.SOCKET_CONNECT_TIMEOUT = 5000;
        TeradataInternalFastloadOutputFormat.SOCKET_CONNECT_RETRY_MAX = 50;
        TeradataInternalFastloadOutputFormat.SOCKET_CONNECT_RETRY_WAIT_MIN = 200;
        TeradataInternalFastloadOutputFormat.SOCKET_CONNECT_RETRY_WAIT_RANDOM_SEED = 500;
        TeradataInternalFastloadOutputFormat.SOCKET_SERVER_ACCEPT_TIME_OUT = 20000;
        TeradataInternalFastloadOutputFormat.server = null;
        TeradataInternalFastloadOutputFormat.SOCKET_PING_TIME_OUT = 60000;
    }

    class InternalFastloadCoordinator implements Runnable {
        private int numTasks;
        private Configuration configuration;
        private Log logger;

        InternalFastloadCoordinator(final int numTasks, final Configuration configuration) {
            this.logger = LogFactory.getLog((Class) InternalFastloadCoordinator.class);
            this.numTasks = numTasks;
            this.configuration = configuration;
        }

        @Override
        public void run() {
            try {
                this.beginLoading(this.numTasks, this.configuration, TeradataInternalFastloadOutputFormat.server);
                this.logger.debug((Object) "Begin loading complete, starting end loading logic");
                this.endLoading(this.numTasks, TeradataInternalFastloadOutputFormat.server);
            } catch (IOException e) {
                this.logger.error((Object) e.getMessage());
                try {
                    TeradataUtils.closeConnection(TeradataInternalFastloadOutputFormat.lsnConnection);
                } catch (ConnectorException e2) {
                    this.logger.error((Object) e2.getMessage());
                }
                TeradataInternalFastloadOutputFormat.this.closeServer(TeradataInternalFastloadOutputFormat.server);
            }
        }

        public void beginLoading(final int taskCount, final Configuration configuration, final ServerSocket server) throws ConnectorException {
            final Socket[] sockets = new Socket[taskCount];
            try {
                server.setSoTimeout(TeradataInternalFastloadOutputFormat.SOCKET_SERVER_ACCEPT_TIME_OUT);
                final long timeoutValue = TeradataPlugInConfiguration.getOutputFastloadSocketTimeout(configuration);
                final long startTime = System.currentTimeMillis();
                while (TeradataInternalFastloadOutputFormat.this.readyConnCount < taskCount) {
                    if (!TeradataInternalFastloadOutputFormat.getJobStatus()) {
                        this.logger.debug((Object) "TeradataInternalFastloadProcessor.jobSuccess is false");
                        throw new ConnectorException(22002);
                    }
                    try {
                        (sockets[TeradataInternalFastloadOutputFormat.this.readyConnCount] = server.accept()).setTcpNoDelay(true);
                        final DataInputStream in = new DataInputStream(sockets[TeradataInternalFastloadOutputFormat.this.readyConnCount++].getInputStream());
                        final String command = in.readUTF();
                        if (!command.contains(TeradataInternalFastloadOutputFormat.MSG_FASTLOAD_CONNECTION_READY)) {
                            this.logger.debug((Object) ("Received failure message from mapper: " + command));
                            throw new ConnectorException(22002);
                        }
                        this.logger.debug((Object) ("Received fastload ready message from mapper (" + TeradataInternalFastloadOutputFormat.this.readyConnCount + "/" + taskCount + ")"));
                    } catch (SocketTimeoutException e3) {
                        final long endTime = System.currentTimeMillis();
                        if (endTime - startTime >= timeoutValue) {
                            throw new ConnectorException(22003);
                        }
                        continue;
                    }
                }
                this.logger.debug((Object) "Setting DBS session dateform = integerdate");
                final Statement statement = TeradataInternalFastloadOutputFormat.lsnConnection.createStatement();
                statement.executeUpdate(TeradataInternalFastloadOutputFormat.SQL_FASTLOAD_DATAFORM);
                final String outputDatabase = TeradataPlugInConfiguration.getOutputDatabase(configuration);
                String errorTableDatabase = TeradataPlugInConfiguration.getOutputErrorTableDatabase(configuration);
                if (errorTableDatabase.isEmpty()) {
                    errorTableDatabase = outputDatabase;
                }
                final String errorTable1 = TeradataConnection.getQuotedEscapedName(errorTableDatabase, TeradataPlugInConfiguration.getOutputErrorTable1Name(configuration));
                final String errorTable2 = TeradataConnection.getQuotedEscapedName(errorTableDatabase, TeradataPlugInConfiguration.getOutputErrorTable2Name(configuration));
                String outputTableName = TeradataPlugInConfiguration.getOutputTable(configuration);
                outputTableName = TeradataConnection.getQuotedEscapedName(outputDatabase, outputTableName);
                final String beginLoading = TeradataInternalFastloadOutputFormat.SQL_BEGIN_LOADING + " " + outputTableName + " ERRORFILES " + errorTable1 + ", " + errorTable2 + " WITH INTERVAL";
                this.logger.debug((Object) ("Executing beginloading command: " + beginLoading));
                statement.execute(beginLoading);
                TeradataInternalFastloadOutputFormat.lsnConnection.setAutoCommit(false);
                final TeradataTableDesc targetTableDesc = TeradataSchemaUtils.tableDescFromText(TeradataPlugInConfiguration.getOutputTableDesc(configuration));
                final TeradataColumnDesc[] fieldDescs = targetTableDesc.getColumns();
                final String[] fieldTypes4Using = new String[fieldDescs.length];
                final String[] fieldNames = new String[fieldDescs.length];
                int lowestScaleForTime = 6;
                int lowestScaleForTimeStamp = 6;
                for (int index = 0; index < fieldDescs.length; ++index) {
                    final TeradataColumnDesc fieldDesc = fieldDescs[index];
                    if (fieldDesc.getType() == 92) {
                        if (fieldDesc.getScale() < lowestScaleForTime) {
                            lowestScaleForTime = fieldDesc.getScale();
                        }
                    } else if (fieldDesc.getType() == 93 && fieldDesc.getScale() < lowestScaleForTimeStamp) {
                        lowestScaleForTimeStamp = fieldDesc.getScale();
                    }
                }
                for (int index = 0; index < fieldDescs.length; ++index) {
                    final TeradataColumnDesc fieldDesc = fieldDescs[index];
                    fieldNames[index] = fieldDesc.getName();
                    fieldTypes4Using[index] = fieldDesc.getTypeString4Using(TeradataInternalFastloadOutputFormat.this.charset, lowestScaleForTime, lowestScaleForTimeStamp);
                }
                final String usingInsertSQL = TeradataConnection.getUsingSQL(outputTableName, fieldNames, fieldTypes4Using, TeradataInternalFastloadOutputFormat.this.charset);
                this.logger.info((Object) usingInsertSQL);
                this.logger.debug((Object) ("Executing using insert command: " + usingInsertSQL));
                statement.executeUpdate(usingInsertSQL);
                statement.close();
                server.setSoTimeout(TeradataInternalFastloadOutputFormat.this.readyConnCount = 0);
                this.logger.debug((Object) "Signaling to mappers to begin fastload transfer");
                while (TeradataInternalFastloadOutputFormat.this.readyConnCount < taskCount) {
                    final DataOutputStream out = new DataOutputStream(sockets[TeradataInternalFastloadOutputFormat.this.readyConnCount++].getOutputStream());
                    out.writeUTF(TeradataInternalFastloadOutputFormat.MSG_FASTLOAD_TRANSFER_BEGIN);
                    out.flush();
                }
                TeradataInternalFastloadOutputFormat.this.readyConnCount = 0;
                while (TeradataInternalFastloadOutputFormat.this.readyConnCount < taskCount) {
                    sockets[TeradataInternalFastloadOutputFormat.this.readyConnCount++].close();
                }
            } catch (SQLException e) {
                throw new ConnectorException(e.getMessage(), e);
            } catch (IOException e2) {
                throw new ConnectorException(e2.getMessage(), e2);
            } finally {
                for (int i = 0; i < sockets.length; ++i) {
                    TeradataInternalFastloadOutputFormat.this.closeSocket(sockets[i]);
                    sockets[i] = null;
                }
            }
        }

        private void sendPingMsg(final DataOutputStream[] outs) throws IOException {
            for (int i = 0; i < outs.length; ++i) {
                if (outs[i] != null) {
                    outs[i].writeUTF(TeradataInternalFastloadOutputFormat.MSG_FASTLOAD_PING);
                    outs[i].flush();
                }
            }
        }

        public void endLoading(final int taskCount, final ServerSocket server) throws ConnectorException {
            final Socket[] sockets = new Socket[taskCount];
            try {
                final DataOutputStream[] outs = new DataOutputStream[taskCount];
                server.setSoTimeout(TeradataInternalFastloadOutputFormat.SOCKET_PING_TIME_OUT);
                TeradataInternalFastloadOutputFormat.this.readyTaskCount = 0;
                while (TeradataInternalFastloadOutputFormat.this.readyTaskCount < taskCount) {
                    if (!TeradataInternalFastloadOutputFormat.getJobStatus()) {
                        this.logger.debug((Object) "TeradataInternalFastloadProcessor.jobSuccess is false");
                        throw new ConnectorException(22002);
                    }
                    try {
                        (sockets[TeradataInternalFastloadOutputFormat.this.readyTaskCount] = server.accept()).setTcpNoDelay(true);
                    } catch (SocketTimeoutException e3) {
                        this.sendPingMsg(outs);
                        continue;
                    }
                    final DataInputStream in = new DataInputStream(sockets[TeradataInternalFastloadOutputFormat.this.readyTaskCount].getInputStream());
                    outs[TeradataInternalFastloadOutputFormat.this.readyTaskCount] = new DataOutputStream(sockets[TeradataInternalFastloadOutputFormat.this.readyTaskCount].getOutputStream());
                    final String command = in.readUTF();
                    if (!command.contains(TeradataInternalFastloadOutputFormat.MSG_FASTLOAD_TRANSFER_END)) {
                        this.logger.debug((Object) ("Received failure message from mapper: " + command));
                        throw new ConnectorException(22002);
                    }
                    final TeradataInternalFastloadOutputFormat this$0 = TeradataInternalFastloadOutputFormat.this;
                    ++this$0.readyTaskCount;
                    this.logger.debug((Object) ("Received fastload complete message from mapper (" + TeradataInternalFastloadOutputFormat.this.readyConnCount + "/" + taskCount + ")"));
                }
                server.setSoTimeout(0);
                final Statement statement = TeradataInternalFastloadOutputFormat.lsnConnection.createStatement();
                this.logger.debug((Object) ("Executing checkpoint loading command " + TeradataInternalFastloadOutputFormat.SQL_CHECKPOINT_LOADING_END));
                statement.executeUpdate(TeradataInternalFastloadOutputFormat.SQL_CHECKPOINT_LOADING_END);
                TeradataInternalFastloadOutputFormat.lsnConnection.commit();
                this.logger.debug((Object) ("Executing end loading command: " + TeradataInternalFastloadOutputFormat.SQL_END_LOADING));
                statement.executeUpdate(TeradataInternalFastloadOutputFormat.SQL_END_LOADING);
                TeradataInternalFastloadOutputFormat.lsnConnection.commit();
                TeradataInternalFastloadOutputFormat.lsnConnection.setAutoCommit(true);
                statement.close();
                this.logger.debug((Object) "Signaling to mappers fastload transfer is complete");
                TeradataInternalFastloadOutputFormat.this.readyTaskCount = 0;
                while (TeradataInternalFastloadOutputFormat.this.readyTaskCount < taskCount) {
                    outs[TeradataInternalFastloadOutputFormat.this.readyTaskCount].writeUTF(TeradataInternalFastloadOutputFormat.MSG_FASTLOAD_CONNECTION_CLOSE);
                    outs[TeradataInternalFastloadOutputFormat.this.readyTaskCount].flush();
                    final TeradataInternalFastloadOutputFormat this$2 = TeradataInternalFastloadOutputFormat.this;
                    ++this$2.readyTaskCount;
                }
                TeradataInternalFastloadOutputFormat.this.readyTaskCount = 0;
                while (TeradataInternalFastloadOutputFormat.this.readyTaskCount < taskCount) {
                    sockets[TeradataInternalFastloadOutputFormat.this.readyTaskCount++].close();
                }
            } catch (SQLException e) {
                throw new ConnectorException(e.getMessage(), e);
            } catch (IOException e2) {
                throw new ConnectorException(e2.getMessage(), e2);
            } finally {
                for (int i = 0; i < sockets.length; ++i) {
                    TeradataInternalFastloadOutputFormat.this.closeSocket(sockets[i]);
                    sockets[i] = null;
                }
            }
        }
    }

    public class TeradataInternalFastLoadRecordWriter extends RecordWriter<K, DBWritable> {
        private Log logger;
        private Socket socket;
        private DataInputStream in;
        private DataOutputStream out;
        private String host;
        private int port;
        private Connection connection;
        private PreparedStatement preparedStatement;
        int batchSize;
        int batchCount;
        protected long end_timestamp;
        protected long start_timestamp;

        public TeradataInternalFastLoadRecordWriter(final Connection connection, final PreparedStatement preparedStatement, final int batchSize, final String host, final int port) throws IOException {
            this.logger = LogFactory.getLog((Class) TeradataInternalFastLoadRecordWriter.class);
            this.in = null;
            this.out = null;
            this.host = null;
            this.port = 0;
            this.batchCount = 0;
            this.end_timestamp = 0L;
            this.start_timestamp = 0L;
            this.start_timestamp = System.currentTimeMillis();
            this.logger.info((Object) ("TeradataRecordWriter starts at " + this.start_timestamp));
            this.connection = connection;
            this.preparedStatement = preparedStatement;
            this.batchSize = batchSize;
            this.host = host;
            this.port = port;
        }

        @Override
        public void write(final K key, final DBWritable value) throws IOException {
            try {
                try {
                    value.write(this.preparedStatement);
                    this.preparedStatement.addBatch();
                    ++this.batchCount;
                } catch (NullPointerException ex) {
                }
                if (this.batchCount >= this.batchSize) {
                    try {
                        this.preparedStatement.executeBatch();
                    } catch (BatchUpdateException ex2) {
                    }
                    this.batchCount = 0;
                }
            } catch (SQLException e) {
                final ConnectorException start = new ConnectorException(e.getMessage(), e);
                while (e != null) {
                    final StackTraceElement[] elements = e.getStackTrace();
                    for (int i = 0, n = elements.length; i < n; ++i) {
                        this.logger.error((Object) (elements[i].getFileName() + ":" + elements[i].getLineNumber() + ">> " + elements[i].getMethodName() + "()"));
                    }
                    e = e.getNextException();
                }
                TeradataUtils.closeConnection(this.connection);
                throw start;
            }
        }

        public void close(final TaskAttemptContext context) throws IOException {
            int retryCount = 0;
            this.logger.debug((Object) "Attempting to connect to InternalFastload coordinator to signal data transfer complete");
            while (this.socket == null) {
                try {
                    (this.socket = new Socket(this.host, this.port)).setTcpNoDelay(true);
                    this.logger.debug((Object) "Connected to InternalFastload coordinator successfully");
                } catch (ConnectException e) {
                    if (++retryCount >= TeradataInternalFastloadOutputFormat.SOCKET_CONNECT_RETRY_MAX) {
                        throw new ConnectorException(e.getMessage(), e);
                    }
                    this.socket = null;
                    try {
                        final int waitSeconds = new Random().nextInt(TeradataInternalFastloadOutputFormat.SOCKET_CONNECT_RETRY_WAIT_RANDOM_SEED) + TeradataInternalFastloadOutputFormat.SOCKET_CONNECT_RETRY_WAIT_MIN;
                        Thread.sleep(waitSeconds);
                    } catch (InterruptedException e3) {
                        this.logger.debug((Object) "InterruptedException occurred during random sleep between retries");
                    }
                }
            }
            this.in = new DataInputStream(this.socket.getInputStream());
            this.out = new DataOutputStream(this.socket.getOutputStream());
            try {
                if (this.batchCount > 0) {
                    try {
                        this.preparedStatement.executeBatch();
                    } catch (BatchUpdateException ex) {
                    }
                    this.batchCount = 0;
                }
                this.logger.debug((Object) ("Signaling to InternalFastload coordinator data transfer is complete for task " + context.getTaskAttemptID().toString()));
                this.out.writeUTF(TeradataInternalFastloadOutputFormat.MSG_FASTLOAD_TRANSFER_END);
                this.out.flush();
                this.logger.info((Object) "fastload: transfer data finished");
                String message;
                while (true) {
                    message = this.in.readUTF();
                    if (!message.equals(TeradataInternalFastloadOutputFormat.MSG_FASTLOAD_PING)) {
                        break;
                    }
                    this.logger.info((Object) "ping message received. ignore.");
                }
                if (!message.equals(TeradataInternalFastloadOutputFormat.MSG_FASTLOAD_CONNECTION_CLOSE)) {
                    this.logger.debug((Object) ("Received unsupported message from InternalFastload coordinator: " + message));
                    throw new IOException("fastload: message " + TeradataInternalFastloadOutputFormat.MSG_FASTLOAD_CONNECTION_CLOSE + " expected");
                }
                this.connection.close();
                this.socket.close();
                this.end_timestamp = System.currentTimeMillis();
                this.logger.info((Object) ("TeradataRecordWriter ended at: " + this.end_timestamp));
                this.logger.info((Object) ("TeradataRecordWriter duration in seconds: " + (this.end_timestamp - this.start_timestamp) / 1000L));
            } catch (SQLException e2) {
                final SQLException root = e2;
                while (e2 != null) {
                    final StackTraceElement[] elements = e2.getStackTrace();
                    for (int i = 0, n = elements.length; i < n; ++i) {
                        this.logger.error((Object) (elements[i].getFileName() + ":" + elements[i].getLineNumber() + ">> " + elements[i].getMethodName() + "()"));
                    }
                    e2 = e2.getNextException();
                }
                throw new IOException(new ConnectorException(root.getMessage(), root));
            } finally {
                this.connection = null;
            }
            if (ConnectorConfiguration.getEnableHdfsLoggingFlag(context.getConfiguration()) && LogContainer.getInstance().listSize() > 0) {
                LogContainer.getInstance().writeHdfsLogs(context.getConfiguration());
            }
        }
    }
}
