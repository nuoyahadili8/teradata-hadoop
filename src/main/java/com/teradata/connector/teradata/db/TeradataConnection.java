package com.teradata.connector.teradata.db;

import com.teradata.connector.common.utils.ConnectorSchemaParser;
import com.teradata.connector.common.utils.ConnectorStringUtils;
import com.teradata.connector.common.utils.StandardCharsets;
import com.teradata.connector.teradata.processor.TeradataBatchInsertProcessor;
import com.teradata.connector.teradata.schema.TeradataColumnDesc;
import com.teradata.connector.teradata.schema.TeradataTableDesc;
import com.teradata.connector.teradata.schema.TeradataViewDesc;
import com.teradata.connector.teradata.utils.TeradataSchemaUtils;
import com.teradata.jdbc.URLParameters;
import com.teradata.jdbc.jdbc_4.ColumnProperties;
import com.teradata.jdbc.jdk6.JDK6_SQL_ResultSetMetaData;
import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.BytesWritable;
/**
 * @Project:
 * @Description:
 * @Version 1.0.0
 * @Throws SystemException:
 * @Author: <li>2019/2/21/021 Administrator Create 1.0
 * @Copyright ©2018-2019 al.github
 * @Modified By:
 */
public class TeradataConnection {
    private static Log logger = LogFactory.getLog(TeradataConnection.class);
    protected String className = null;
    protected String url = null;
    protected String userName = null;
    protected String password = null;
    protected String currentDatabase = "";
    protected int ampCount = 0;
    protected Connection connection = null;
    protected Configuration configuration;
    protected String dbProductName;
    protected int dbMajorVersion;
    protected int dbMinorVersion;
    protected int jdbcMajorVersion;
    protected int jdbcMinorVersion;
    protected int maxTableNameLength;
    protected boolean useXView = true;
    protected static final String TERADATA_PRODUCT_NAME = "TERADATA";
    protected static final int TERADATA_MIN_DB_MAJOR_VERSION = 13;
    protected static final int TERADATA_MIN_DB_MINOR_VERSION = 0;
    protected static final int TERADATA_MIN_JDBC_MAJOR_VERSION = 13;
    protected static final int TERADATA_MIN_JDBC_MINOR_VERSION = 0;
    protected static final int TERADATA_MIN_DB_MAJOR_VERSION_EON = 14;
    protected static final int TERADATA_MIN_DB_MINOR_VERSION_EON = 10;
    public static final String JDBC_CONNECTION_TYPE_PROPERTY = "TYPE";
    public static final String JDBC_CONNECTION_FASTLOAD_VALUE = "TYPE=FASTLOAD";
    public static final String FASTLOAD_ERROR_TABLE_EXT_ONE = "_ERR_1";
    public static final String FASTLOAD_ERROR_TABLE_EXT_TWO = "_ERR_2";
    public static final int DB_TABLE_BLOCKSIZE_MAX = 130048;
    protected static final String SQL_SET_QUERY_BAND = "SET QUERY_BAND = '%s' For Session";
    protected static final String SQL_ENABLE_UNICODE_PASSTHROUGH = "SET SESSION CHARACTER SET UNICODE PASS THROUGH ON";
    protected static final String ACCESS_LOCK_SQL = "LOCK ROW FOR ACCESS ";
    protected static final int JDBC_RESULTSET_BUFFER_MAX = 1048576;
    protected static final String JDBC_CONNECTION_DATABASE_PROPERTY = "DATABASE";
    protected static final int JDBC_STATEMENT_LENGTH_MAX = 524288;
    protected static final String SQL_GET_AMP_COUNT = "{fn teradata_amp_count()}";
    protected static final String SQL_GET_CURRENT_DATABASE = "SELECT DATABASE";
    protected static final String SQL_GET_CURRENT_TIMESTAMP = "SELECT CURRENT_TIMESTAMP";
    protected static final String SQL_GET_TABLE_ROW_COUNT = "SELECT CAST(COUNT(*) AS BIGINT) FROM %s %s";
    protected static final String SQL_IS_TABLE_NONEMPTY = "SELECT CAST(COUNT(*) AS BIGINT) FROM %s";
    protected static final String SQL_TABLE_DELETE_STMT = "DELETE FROM %s";
    protected static final String SQL_SELECT_FROM_SOURCE_WHERE = "SELECT %s FROM %s %s";
    protected static final String SQL_INSERT = "INSERT INTO %s (%s) VALUES (%s)";
    protected static final String SQL_INSERT_SELECT_INTO_TABLE = "INSERT INTO %s (%s) SELECT %s FROM %s";
    protected static final String SQL_CREATE_TABLE = "CREATE %s TABLE %s, %s NO FALLBACK, NO BEFORE JOURNAL, NO AFTER JOURNAL, CHECKSUM = DEFAULT (%s) %s %s";
    protected static final String SQL_DROP_TABLE = "DROP TABLE %s";
    protected static final String SQL_CREATE_VIEW = "CREATE VIEW %s (%s) AS %s";
    protected static final String SQL_DROP_VIEW = "DROP VIEW %s";
    protected static final String SQL_DELETE_FROM_TABLE = "DELETE FROM %s %s";
    protected static final String SQL_USING_INSERT_INTO_TABLE = "USING %s INSERT INTO %s ( %s ) VALUES ( %s )";
    protected static final String SQL_CHECK_TABLE_FASTLOADABLE = "SELECT COUNT(*) as NOTFASTLOADABLE FROM DBC.INDICESV IDX   WHERE IDX.DATABASENAME = %s AND IDX.TABLENAME = %s AND IDX.INDEXTYPE NOT IN ('P', 'Q')  UNION ALL SELECT COUNT(*) AS NOTFASTLOADABLE FROM DBC.COLUMNSV COL   WHERE COL.DATABASENAME = %s AND COL.TABLENAME = %s AND COL.COLUMNCONSTRAINT IS NOT NULL  UNION ALL SELECT COUNT(*) AS NOTFASTLOADABLE FROM DBC.TABLE_LEVELCONSTRAINTSV TL   WHERE TL.DATABASENAME = %s AND TL.TABLENAME = %s  UNION ALL SELECT COUNT(*) AS NOTFASTLOADABLE FROM DBC.TRIGGERSV TR   WHERE TR.DATABASENAME = %s AND TR.TABLENAME = %s  UNION ALL SELECT PARENTCOUNT + CHILDCOUNT AS NOTFASTLOADABLE FROM DBC.TABLESV T   WHERE T.DATABASENAME = %s AND T.TABLENAME = %s";
    protected static final String SQL_CHECK_TABLE_FASTLOADABLE_XVIEW = "SELECT COUNT(*) as NOTFASTLOADABLE FROM DBC.INDICESVX IDX   WHERE IDX.DATABASENAME = %s AND IDX.TABLENAME = %s AND IDX.INDEXTYPE NOT IN ('P', 'Q')  UNION ALL SELECT COUNT(*) AS NOTFASTLOADABLE FROM DBC.COLUMNSVX COL   WHERE COL.DATABASENAME = %s AND COL.TABLENAME = %s AND COL.COLUMNCONSTRAINT IS NOT NULL  UNION ALL SELECT COUNT(*) AS NOTFASTLOADABLE FROM DBC.TABLE_LEVELCONSTRAINTSVX TL   WHERE TL.DATABASENAME = %s AND TL.TABLENAME = %s  UNION ALL SELECT COUNT(*) AS NOTFASTLOADABLE FROM DBC.TRIGGERSVX TR   WHERE TR.DATABASENAME = %s AND TR.TABLENAME = %s  UNION ALL SELECT PARENTCOUNT + CHILDCOUNT AS NOTFASTLOADABLE FROM DBC.TABLESVX T   WHERE T.DATABASENAME = %s AND T.TABLENAME = %s";
    protected static final String SQL_GET_DATABASES = "SELECT TRIM(TRAILING FROM DBS.DATABASENAME) AS DATABASENAME FROM DBC.DATABASESV DBS";
    protected static final String SQL_GET_DATABASES_XVIEW = "SELECT TRIM(TRAILING FROM DBS.DATABASENAME) AS DATABASENAME FROM DBC.DATABASESVX DBS";
    protected static final String SQL_GET_TABLES = "SELECT TRIM(TRAILING FROM T.TABLENAME) AS TABLENAME FROM DBC.TABLESV T WHERE T.DATABASENAME = %s and (T.TABLEKIND IN ('O', 'T'))";
    protected static final String SQL_GET_TABLES_XVIEW = "SELECT TRIM(TRAILING FROM T.TABLENAME) AS TABLENAME FROM DBC.TABLESVX T WHERE T.DATABASENAME = %s and (T.TABLEKIND IN ('O', 'T'))";
    protected static final String SQL_GET_PRIMARY_KEY = "SELECT TRIM(TRAILING FROM RST.COLUMNNAME) AS COLUMNNAME FROM (   SELECT 1 AS INDEXORDER, I.INDEXNUMBER, I.COLUMNNAME, I.COLUMNPOSITION FROM DBC.INDICESV I   WHERE I.INDEXTYPE = 'K' AND I.UNIQUEFLAG = 'Y' AND I.DATABASENAME = %s AND I.TABLENAME = %s UNION ALL   SELECT 2 AS INDEXORDER, I.INDEXNUMBER, I.COLUMNNAME, I.COLUMNPOSITION FROM DBC.INDICESV I   WHERE I.INDEXTYPE = 'P' AND I.UNIQUEFLAG = 'Y' AND I.DATABASENAME = %s AND I.TABLENAME = %s UNION ALL   SELECT 3 AS INDEXORDER, I.INDEXNUMBER, I.COLUMNNAME, I.COLUMNPOSITION FROM DBC.INDICESV I   WHERE I.INDEXTYPE = 'S' AND I.UNIQUEFLAG = 'Y' AND I.DATABASENAME = %s AND I.TABLENAME = %s ) RST QUALIFY RANK(RST.INDEXORDER ASC, RST.INDEXNUMBER ASC) = 1 ORDER BY RST.COLUMNPOSITION ASC";
    protected static final String SQL_GET_PRIMARY_KEY_XVIEW = "SELECT TRIM(TRAILING FROM RST.COLUMNNAME) AS COLUMNNAME FROM (   SELECT 1 AS INDEXORDER, I.INDEXNUMBER, I.COLUMNNAME, I.COLUMNPOSITION FROM DBC.INDICESVX I   WHERE I.INDEXTYPE = 'K' AND I.UNIQUEFLAG = 'Y' AND I.DATABASENAME = %s AND I.TABLENAME = %s UNION ALL   SELECT 2 AS INDEXORDER, I.INDEXNUMBER, I.COLUMNNAME, I.COLUMNPOSITION FROM DBC.INDICESVX I   WHERE I.INDEXTYPE = 'P' AND I.UNIQUEFLAG = 'Y' AND I.DATABASENAME = %s AND I.TABLENAME = %s UNION ALL   SELECT 3 AS INDEXORDER, I.INDEXNUMBER, I.COLUMNNAME, I.COLUMNPOSITION FROM DBC.INDICESVX I   WHERE I.INDEXTYPE = 'S' AND I.UNIQUEFLAG = 'Y' AND I.DATABASENAME = %s AND I.TABLENAME = %s ) RST QUALIFY RANK(RST.INDEXORDER ASC, RST.INDEXNUMBER ASC) = 1 ORDER BY RST.COLUMNPOSITION ASC";
    protected static final String SQL_GET_TABLE_KIND = "SELECT TRIM(T.TABLEKIND) AS TABLEKIND FROM DBC.TABLESV T WHERE T.DATABASENAME = %s AND T.TABLENAME = %s";
    protected static final String SQL_GET_TABLE_KIND_XVIEW = "SELECT TRIM(T.TABLEKIND) AS TABLEKIND FROM DBC.TABLESVX T WHERE T.DATABASENAME = %s AND T.TABLENAME = %s";
    protected static final String SQL_GET_PRIMARY_INDEX = "SELECT TRIM(TRAILING FROM I.COLUMNNAME) AS COLUMNNAME FROM DBC.INDICESV I  WHERE I.DATABASENAME = %s AND I.TABLENAME = %s AND (I.INDEXTYPE IN ('P', 'Q'))";
    protected static final String SQL_GET_PRIMARY_INDEX_XVIEW = "SELECT TRIM(TRAILING FROM I.COLUMNNAME) AS COLUMNNAME FROM DBC.INDICESVX I  WHERE I.DATABASENAME = %s AND I.TABLENAME = %s AND (I.INDEXTYPE IN ('P', 'Q'))";
    protected static final String SQL_GET_PARTITIONED_PRIMARY_INDEX = "SELECT TRIM(TRAILING FROM I.COLUMNNAME) AS COLUMNNAME FROM DBC.INDICESV I  WHERE UPPER(I.DATABASENAME) = UPPER(%s) AND I.TABLENAME = %s AND (I.INDEXTYPE = 'Q')";
    protected static final String SQL_GET_PARTITIONED_PRIMARY_INDEX_XVIEW = "SELECT TRIM(TRAILING FROM I.COLUMNNAME) AS COLUMNNAME FROM DBC.INDICESVX I  WHERE UPPER(I.DATABASENAME) = UPPER(%s) AND I.TABLENAME = %s AND (I.INDEXTYPE = 'Q')";
    protected static final String SQL_GET_TABLE_COLUMNS = "SELECT TRIM(TRAILING FROM C.COLUMNNAME) AS COLUMNNAME FROM DBC.COLUMNSV C  WHERE C.DATABASENAME = %s AND C.TABLENAME = %s ORDER BY COLUMNID";
    protected static final String SQL_GET_TABLE_COLUMNS_XVIEW = "SELECT TRIM(TRAILING FROM C.COLUMNNAME) AS COLUMNNAME FROM DBC.COLUMNSVX C  WHERE C.DATABASENAME = %s AND C.TABLENAME = %s ORDER BY COLUMNID";
    protected static final String SQL_GET_TABLE_COLUMN_INFOS = "SELECT TRIM(TRAILING FROM C.COLUMNNAME) AS COLUMNNAME, CHARTYPE FROM DBC.COLUMNSV C  WHERE C.DATABASENAME = %s AND C.TABLENAME = %s ORDER BY COLUMNID";
    protected static final String SQL_GET_TABLE_COLUMN_INFOS_XVIEW = "SELECT TRIM(TRAILING FROM C.COLUMNNAME) AS COLUMNNAME, CHARTYPE FROM DBC.COLUMNSVX C  WHERE C.DATABASENAME = %s AND C.TABLENAME = %s ORDER BY COLUMNID";
    protected static final String SQL_GET_PARTITION_DISTINCT = "SELECT DISTINCT PARTITION FROM %s";
    protected static final String SQL_GET_PARTITION_COUNT = "SELECT CAST(COUNT(DISTINCT PARTITION) as BIGINT) FROM %s";
    protected static final String SQL_GET_PARTITION_MIN_MAX = "SELECT CAST(MIN(PARTITION) AS BIGINT), CAST(MAX(PARTITION) AS BIGINT) FROM %s";
    protected static final String SQL_UNION_ALL_SELECT = " UNION ALL ";
    private static final String REMOTE_CONN_PREFIX = "jdbc:teradata://";

    public TeradataConnection(String className, String url, String userName, String password, Boolean useXview) {
        this.className = className;
        this.url = url;
        this.userName = userName;
        this.password = password;
        this.useXView = useXview;
    }

    public String getDatabaseProductName() {
        return this.dbProductName;
    }

    public int getDatabaseMajorVersion() {
        return this.dbMajorVersion;
    }

    public int getDatabaseMinorVersion() {
        return this.dbMinorVersion;
    }

    public int getJDBCMajorVersion() {
        return this.jdbcMajorVersion;
    }

    public int getJDBCMinorVersion() {
        return this.jdbcMinorVersion;
    }

    public int getMaxTableNameLength() {
        return this.maxTableNameLength;
    }

    public Connection getConnection() throws SQLException {
        return this.connection != null && this.connection.isClosed() ? null : this.connection;
    }

    public void connect() throws SQLException, ClassNotFoundException {
        if (this.connection == null || this.connection.isClosed()) {
            Class.forName(this.className);
            if (this.userName == null) {
                this.connection = DriverManager.getConnection(this.url);
            } else {
                this.connection = DriverManager.getConnection(this.url, this.userName, this.password);
            }
        }

    }

    public void connect(byte[] userNameBytes, byte[] passwordBytes) throws SQLException, ClassNotFoundException {
        Class.forName(this.className);
        if (null != userNameBytes && null != passwordBytes) {
            this.connection = DriverManager.getConnection(this.url, new String(userNameBytes, StandardCharsets.UTF_8), new String(passwordBytes, StandardCharsets.UTF_8));
        } else {
            this.connection = DriverManager.getConnection(this.url);
        }

    }

    public String nativeSQL(String sql) throws SQLException {
        return this.connection != null && !this.connection.isClosed() ? this.connection.nativeSQL(sql) : null;
    }

    public void setAutoCommit(boolean autoCommit) throws SQLException {
        if (this.connection != null && !this.connection.isClosed()) {
            this.connection.setAutoCommit(autoCommit);
        }

    }

    public void setTransactionIsolation(int level) throws SQLException {
        if (this.connection != null && !this.connection.isClosed()) {
            this.connection.setTransactionIsolation(level);
        }

    }

    public void commit() throws SQLException {
        if (this.connection != null && !this.connection.isClosed()) {
            this.connection.commit();
        }

    }

    public void rollback() throws SQLException {
        if (this.connection != null && !this.connection.isClosed()) {
            this.connection.rollback();
        }

    }

    public void close() {
        try {
            if (this.connection != null && !this.connection.isClosed()) {
                this.connection.close();
            }
        } catch (SQLException var5) {
            ;
        } finally {
            this.connection = null;
        }

    }

    public void executeDDL(String sql) throws SQLException {
        if (this.connection != null && !this.connection.isClosed()) {
            Statement statement = this.connection.createStatement();
            statement.execute(sql);
            statement.close();
            this.connection.commit();
        }

    }

    public void executeUpdate(String sql) throws SQLException {
        if (this.connection != null && !this.connection.isClosed()) {
            Statement statement = this.connection.createStatement();
            logger.debug(sql);
            statement.executeUpdate(sql);
            statement.close();
        }

    }

    public void createTable(TeradataTableDesc tableDesc) throws SQLException {
        String createTableSQL = getCreateTableSQL(tableDesc);
        logger.debug(createTableSQL);
        this.executeDDL(createTableSQL);
    }

    public void createView(TeradataViewDesc viewDesc) throws SQLException {
        String createViewSQL = getCreateViewSQL(viewDesc);
        logger.debug(createViewSQL);
        this.executeDDL(createViewSQL);
    }

    public void dropTables(String[] tableNames) throws SQLException {
        for(int i = 0; i < tableNames.length; ++i) {
            String dropTableSQL = getDropTableSQL(tableNames[i]);
            logger.debug(dropTableSQL);
            this.executeDDL(dropTableSQL);
        }

    }

    public void dropTable(String tableName) throws SQLException {
        String dropTableSQL = getDropTableSQL(tableName);
        logger.debug(dropTableSQL);
        this.executeDDL(dropTableSQL);
    }

    public void dropView(String viewName) throws SQLException {
        String dropViewSQL = getDropViewSQL(viewName);
        logger.debug(dropViewSQL);
        this.executeDDL(dropViewSQL);
    }

    public void deleteTable(String tableName) throws SQLException {
        String deleteSQL = getDeleteTableSQL(tableName, (String)null);
        logger.debug(deleteSQL);
        this.executeUpdate(deleteSQL);
    }

    public void deleteTable(String tableName, String condition) throws SQLException {
        String deleteSQL = getDeleteTableSQL(tableName, condition);
        logger.debug(deleteSQL);
        this.executeUpdate(deleteSQL);
    }

    public void executeInsertUnionSelect(String targetTableName, String[] targetFieldNames, String[] sourceTableNames, String[] sourceFieldNames) throws SQLException {
        String unionInsertSQL = getInsertSelectSQL(targetTableName, targetFieldNames, sourceTableNames[0], sourceFieldNames);
        int length = unionInsertSQL.length();
        int unionRequestTextLength = " UNION ALL ".length();

        for(int i = 1; i < sourceTableNames.length; ++i) {
            String selectSQL = getSelectSQL(sourceTableNames[i], sourceFieldNames, (String)null);
            length += selectSQL.length();
            length += unionRequestTextLength;
            if (length <= 524288) {
                unionInsertSQL = unionInsertSQL + " UNION ALL " + selectSQL;
            } else {
                logger.debug(unionInsertSQL);
                this.executeUpdate(unionInsertSQL);
                unionInsertSQL = getInsertSelectSQL(targetTableName, targetFieldNames, sourceTableNames[i], sourceFieldNames);
                length = unionInsertSQL.length();
            }
        }

        logger.debug(unionInsertSQL);
        this.executeUpdate(unionInsertSQL);
    }

    public void executeInsertSelect(String sourceTableName, String[] sourceColumns, String targetTableName, String[] targetColumns) throws SQLException {
        String insertSelectSQL = getInsertSelectSQL(targetTableName, targetColumns, sourceTableName, sourceColumns);
        logger.debug(insertSelectSQL);
        this.executeUpdate(insertSelectSQL);
    }

    public String getTableKind(String tableName) throws SQLException {
        if (this.connection != null) {
            String tableKind = "";
            String sql = getTableKindSQL(tableName, this.useXView);
            logger.debug(sql);
            Statement statement = this.connection.createStatement();
            ResultSet resultSet = statement.executeQuery(sql);
            if (resultSet.next()) {
                tableKind = resultSet.getString(1);
            }

            resultSet.close();
            statement.close();
            return tableKind;
        } else {
            return "";
        }
    }

    public boolean isTable(String tableName) throws SQLException {
        String tableKind = this.getTableKind(tableName).trim();
        return tableKind.equalsIgnoreCase("T") || tableKind.equalsIgnoreCase("O");
    }

    public boolean isTableFastloadable(String tableName) throws SQLException {
        if (this.connection != null) {
            boolean isFastloadable = true;
            String checkFastloadableSql = getCheckTableFastloadableSQL(tableName, this.useXView);
            logger.debug("check fastloadableSql is " + checkFastloadableSql);
            String checkTableNonEmptySql = getIsTableNonEmptySQL(tableName);
            logger.debug("check table non empty sql is " + checkTableNonEmptySql);
            Statement stmt = this.connection.createStatement();
            ResultSet resultSet = stmt.executeQuery(checkTableNonEmptySql);
            if (resultSet.next()) {
                long rowCount = resultSet.getLong(1);
                isFastloadable = rowCount <= 0L;
            }

            resultSet.close();
            if (isFastloadable) {
                resultSet = stmt.executeQuery(checkFastloadableSql);

                while(resultSet.next()) {
                    if (resultSet.getLong(1) > 0L) {
                        isFastloadable = false;
                    }
                }

                resultSet.close();
            }

            return isFastloadable;
        } else {
            return false;
        }
    }

    public ArrayList<Long> getTablePartitions(String tableName, Boolean accessLock) throws SQLException {
        ArrayList<Long> partitionValue = new ArrayList();
        String sql = String.format("SELECT DISTINCT PARTITION FROM %s", tableName);
        if (accessLock) {
            sql = addAccessLockToSql(sql);
        }

        logger.debug(sql);
        if (this.connection != null) {
            Statement stmt = this.connection.createStatement();
            ResultSet resultSet = stmt.executeQuery(sql);

            while(resultSet.next()) {
                partitionValue.add(resultSet.getLong(1));
            }

            resultSet.close();
            stmt.close();
        }

        Collections.sort(partitionValue);
        return partitionValue;
    }

    public ArrayList<Long> getTablePartitionMinMax(String tableName) throws SQLException {
        ArrayList<Long> partitionValue = new ArrayList();
        if (this.connection != null) {
            String sql = String.format("SELECT CAST(MIN(PARTITION) AS BIGINT), CAST(MAX(PARTITION) AS BIGINT) FROM %s", tableName);
            logger.debug(sql);
            Statement stmt = this.connection.createStatement();
            ResultSet resultSet = stmt.executeQuery(sql);
            if (resultSet.next()) {
                partitionValue.add(resultSet.getLong(1));
                partitionValue.add(resultSet.getLong(2));
            }

            resultSet.close();
            stmt.close();
        }

        return partitionValue;
    }

    public long getTablePartitionCount(String tableName, Boolean accessLock) throws SQLException {
        if (this.connection != null) {
            String sql = String.format("SELECT CAST(COUNT(DISTINCT PARTITION) as BIGINT) FROM %s", tableName);
            if (accessLock) {
                sql = addAccessLockToSql(sql);
            }

            logger.debug(sql);
            long partitionCount = 0L;
            Statement stmt = this.connection.createStatement();
            ResultSet resultSet = stmt.executeQuery(sql);
            if (resultSet.next()) {
                partitionCount = resultSet.getLong(1);
            }

            resultSet.close();
            stmt.close();
            return partitionCount;
        } else {
            return 0L;
        }
    }

    public boolean isTableNoPrimaryIndex(String tableName) throws SQLException {
        return this.getPrimaryIndex(tableName).length == 0;
    }

    public boolean isTablePPI(String tableName) throws SQLException {
        return this.getPartitionedPrimaryIndex(tableName).length != 0;
    }

    public boolean isTableNonEmpty(String tableName) throws SQLException {
        if (this.connection != null) {
            Statement statement = this.connection.createStatement();
            String sql = getIsTableNonEmptySQL(tableName);
            logger.debug(sql);
            ResultSet resultSet = statement.executeQuery(sql);
            long rowCount = 0L;
            if (resultSet.next()) {
                rowCount = resultSet.getLong(1);
            }

            resultSet.close();
            statement.close();
            return rowCount > 0L;
        } else {
            return true;
        }
    }

    public boolean isSupportedDatabase() {
        return "TERADATA".equalsIgnoreCase(this.dbProductName) && (this.dbMajorVersion > 13 || this.dbMajorVersion == 13 && this.dbMinorVersion >= 0);
    }

    public boolean isSupportedJDBC() {
        return this.jdbcMajorVersion > 13 || this.jdbcMajorVersion == 13 && this.jdbcMinorVersion >= 0;
    }

    public String[] getColumnNamesForTable(String tableName) throws SQLException {
        TeradataColumnDesc[] columns = this.getColumnDescsForTable(tableName, (String[])null);
        String[] result = new String[columns.length];

        for(int i = 0; i < columns.length; ++i) {
            result[i] = columns[i].getName();
        }

        return result;
    }

    public String[] getColumnNamesForSQL(String sql) throws SQLException {
        TeradataColumnDesc[] columns = this.getColumnDescsForSQL(sql);
        String[] result = new String[columns.length];

        for(int i = 0; i < columns.length; ++i) {
            result[i] = columns[i].getName();
        }

        return result;
    }

    public Map<String, Integer> getColumnTypesForSQL(String sql) throws SQLException {
        TeradataColumnDesc[] columns = this.getColumnDescsForSQL(sql);
        Map<String, Integer> result = new HashMap(columns.length);

        for(int i = 0; i < columns.length; ++i) {
            result.put(columns[i].getName(), columns[i].getType());
        }

        return result;
    }

    public Map<String, String> getColumnTypeNamesForSQL(String sql) throws SQLException {
        TeradataColumnDesc[] columns = this.getColumnDescsForSQL(sql);
        Map<String, String> result = new HashMap(columns.length);

        for(int i = 0; i < columns.length; ++i) {
            result.put(columns[i].getName(), columns[i].getTypeName());
        }

        return result;
    }

    public TeradataColumnDesc[] getColumnDescsForSQL(String sql) throws SQLException {
        if (this.connection != null) {
            PreparedStatement stmt = this.connection.prepareStatement(sql);
            ResultSetMetaData metadata = stmt.getMetaData();
            stmt.close();
            return this.getColumnDescs(metadata);
        } else {
            return null;
        }
    }

    public TeradataColumnDesc[] getColumnDescsForSQLWithCharSet(String sql, String charset) throws SQLException {
        TeradataColumnDesc[] columnDescs = this.getColumnDescsForSQL(sql);
        if (columnDescs != null && columnDescs.length > 0) {
            int charType = 1;
            if (!ConnectorStringUtils.isEmpty(charset) && (charset.equalsIgnoreCase("UTF8") || charset.equalsIgnoreCase("UTF16"))) {
                charType = 2;
            }

            TeradataColumnDesc[] var5 = columnDescs;
            int var6 = columnDescs.length;

            for(int var7 = 0; var7 < var6; ++var7) {
                TeradataColumnDesc columnDesc = var5[var7];
                switch(columnDesc.getType()) {
                    case -1:
                    case 1:
                    case 12:
                    case 2005:
                        columnDesc.setCharType(charType);
                        break;
                    default:
                        columnDesc.setCharType(0);
                }
            }
        }

        return columnDescs;
    }

    public TeradataColumnDesc[] getColumnDescsForTable(String tableName, String[] columnNames) throws SQLException {
        String selectColumnsSQL = getSelectSQL(tableName, columnNames, (String)null);
        TeradataColumnDesc[] columnDescs = this.getColumnDescsForSQL(selectColumnsSQL);
        HashMap<String, Integer> columnNamesMap = new HashMap();

        for(int i = 0; i < columnDescs.length; ++i) {
            columnNamesMap.put(columnDescs[i].getName().toUpperCase(), i);
        }

        String selectColumnInfosSQL = getListColumnInfosSQL(tableName, this.useXView);
        Statement stmt = this.connection.createStatement();
        ResultSet resultSet = stmt.executeQuery(selectColumnInfosSQL);

        while(resultSet.next()) {
            String colName = resultSet.getString("COLUMNNAME").toUpperCase();
            Integer pos = (Integer)columnNamesMap.get(colName);
            if (pos != null) {
                TeradataColumnDesc columnDesc = columnDescs[pos];
                columnDesc.setCharType(resultSet.getInt("CHARTYPE"));
            }
        }

        return columnDescs;
    }

    public String[] getListColumns(String tableName) throws SQLException {
        List<String> tables = new ArrayList();
        Statement statement = this.connection.createStatement();
        String sql = getListColumnsSQL(tableName, this.useXView);
        ResultSet resultSet = statement.executeQuery(sql);
        logger.debug(sql);

        while(resultSet.next()) {
            tables.add(resultSet.getString(1));
        }

        resultSet.close();
        statement.close();
        return (String[])tables.toArray(new String[tables.size()]);
    }

    public String[] getPrimaryKey(String tableName) throws SQLException {
        List<String> primaryKeys = new ArrayList();
        Statement statement = this.connection.createStatement();
        String sql = getPrimaryKeySQL(tableName, this.useXView);
        ResultSet resultSet = statement.executeQuery(sql);

        while(resultSet.next()) {
            primaryKeys.add(resultSet.getString(1));
        }

        resultSet.close();
        statement.close();
        return (String[])primaryKeys.toArray(new String[0]);
    }

    public String[] getPrimaryIndex(String tableName) throws SQLException {
        List<String> primaryIndices = new ArrayList();
        Statement statement = this.connection.createStatement();
        String sql = getPrimaryIndexSQL(tableName, this.useXView);
        ResultSet resultSet = statement.executeQuery(sql);

        while(resultSet.next()) {
            primaryIndices.add(resultSet.getString(1));
        }

        resultSet.close();
        statement.close();
        return (String[])primaryIndices.toArray(new String[0]);
    }

    public void truncateTable(String tableName) throws SQLException {
        Statement statement = this.connection.createStatement();
        String sql = getTableDeleteStmt(tableName);
        statement.executeQuery(sql);
        statement.close();
    }

    public String[] getPartitionedPrimaryIndex(String tableName) throws SQLException {
        List<String> ppiIndices = new ArrayList();
        Statement statement = this.connection.createStatement();
        String sql = getPartitionedPrimaryIndexSQL(tableName, this.useXView);
        ResultSet resultSet = statement.executeQuery(sql);

        while(resultSet.next()) {
            ppiIndices.add(resultSet.getString(1));
        }

        resultSet.close();
        statement.close();
        return (String[])ppiIndices.toArray(new String[0]);
    }

    public String[] getTablesOfNameStartWith(String databaseName, String tablePattern) throws SQLException {
        List<String> tableList = new ArrayList();
        String selectTableSQL = null;
        int index = tablePattern.indexOf(46);
        String dbcTablesView = "DBC.TABLESV";
        if (this.useXView) {
            dbcTablesView = "DBC.TABLESVX";
        }

        String database;
        if (index > 0) {
            database = tablePattern.substring(0, index);
            String tableName = tablePattern.substring(index + 1);
            selectTableSQL = getSelectSQL(dbcTablesView, new String[]{"TRIM(TRAILING FROM TABLENAME)"}, "DATABASENAME = '" + database + "' AND TABLENAME LIKE '" + tableName + "%'", false);
        } else if (databaseName != null && !databaseName.isEmpty()) {
            selectTableSQL = getSelectSQL(dbcTablesView, new String[]{"TRIM(TRAILING FROM TABLENAME)"}, "DATABASENAME = '" + databaseName + "' AND TABLENAME LIKE '" + tablePattern + "%'", false);
        } else {
            database = "SELECT DATABASE";
            selectTableSQL = getSelectSQL(dbcTablesView, new String[]{"TRIM(TRAILING FROM TABLENAME)"}, "DATABASENAME = (" + database + ") AND TABLENAME LIKE '" + tablePattern + "%'", false);
        }

        logger.debug(selectTableSQL);
        Statement statement = this.connection.createStatement();
        ResultSet resultSet = statement.executeQuery(selectTableSQL);
        TeradataTableDesc tableDesc = new TeradataTableDesc();
        tableDesc.setDatabaseName(databaseName);

        while(resultSet.next()) {
            tableDesc.setName(resultSet.getString(1));
            tableList.add(tableDesc.getQualifiedName());
        }

        resultSet.close();
        statement.close();
        return (String[])tableList.toArray(new String[0]);
    }

    public long getTableRowCount(String tableName, String condition) throws SQLException {
        long rowCount = 0L;
        String sql = getTableRowCountSQL(tableName, condition);
        logger.debug(sql);
        Statement statement = this.connection.createStatement();
        ResultSet resultSet = statement.executeQuery(sql);
        if (resultSet.next()) {
            rowCount = resultSet.getLong(1);
        }

        resultSet.close();
        statement.close();
        return rowCount;
    }

    public void getDatabaseProperty() throws SQLException {
        DatabaseMetaData dbMetaData = this.connection.getMetaData();
        this.dbProductName = dbMetaData.getDatabaseProductName();
        this.dbMajorVersion = dbMetaData.getDatabaseMajorVersion();
        this.dbMinorVersion = dbMetaData.getDatabaseMinorVersion();
        this.jdbcMajorVersion = dbMetaData.getDriverMajorVersion();
        this.jdbcMinorVersion = dbMetaData.getDriverMinorVersion();
        this.maxTableNameLength = dbMetaData.getMaxTableNameLength();
        if (this.maxTableNameLength == 0) {
            this.maxTableNameLength = 2147483647;
        }

        dbMetaData = null;
    }

    public int getAMPCount() throws SQLException {
        if (this.ampCount == 0) {
            try {
                String result = this.nativeSQL("{fn teradata_amp_count()}");
                if (result != null) {
                    this.ampCount = Integer.parseInt(result);
                }
            } catch (NumberFormatException var2) {
                ;
            }
        }

        return this.ampCount;
    }

    public String toJavaType(int sqlType) {
        if (sqlType == 4) {
            return "Integer";
        } else if (sqlType == 12) {
            return "String";
        } else if (sqlType == 1) {
            return "String";
        } else if (sqlType == -1) {
            return "String";
        } else if (sqlType == -9) {
            return "String";
        } else if (sqlType == -15) {
            return "String";
        } else if (sqlType == -16) {
            return "String";
        } else if (sqlType == 2) {
            return "java.math.BigDecimal";
        } else if (sqlType == 3) {
            return "java.math.BigDecimal";
        } else if (sqlType == -7) {
            return "Boolean";
        } else if (sqlType == 16) {
            return "Boolean";
        } else if (sqlType == -6) {
            return "Integer";
        } else if (sqlType == 5) {
            return "Integer";
        } else if (sqlType == -5) {
            return "Long";
        } else if (sqlType == 7) {
            return "Float";
        } else if (sqlType == 6) {
            return "Double";
        } else if (sqlType == 8) {
            return "Double";
        } else if (sqlType == 91) {
            return "java.sql.Date";
        } else if (sqlType == 92) {
            return "java.sql.Time";
        } else if (sqlType == 93) {
            return "java.sql.Timestamp";
        } else if (sqlType != 2003 && sqlType != 2002 && sqlType != 1111) {
            if (sqlType != -2 && sqlType != -3) {
                if (sqlType == 2005) {
                    return "java.sql.Clob";
                } else {
                    return sqlType != 2004 && sqlType != -4 ? null : "java.sql.Blob";
                }
            } else {
                return BytesWritable.class.getName();
            }
        } else {
            return "String";
        }
    }

    public String[] listTables(String databaseName) throws SQLException {
        List<String> tables = new ArrayList();
        Statement statement = this.connection.createStatement();
        String sql = getListTablesSQL(databaseName, this.useXView);
        ResultSet resultSet = statement.executeQuery(sql);
        logger.debug(sql);

        while(resultSet.next()) {
            tables.add(resultSet.getString(1));
        }

        resultSet.close();
        statement.close();
        return (String[])tables.toArray(new String[tables.size()]);
    }

    public String[] listDatabases() throws SQLException {
        List<String> databases = new ArrayList();
        Statement statement = this.connection.createStatement();
        String sql = getListDatabasesSQL(this.useXView);
        ResultSet resultSet = statement.executeQuery(sql);
        logger.debug(sql);

        while(resultSet.next()) {
            databases.add(resultSet.getString(1));
        }

        resultSet.close();
        statement.close();
        return (String[])databases.toArray(new String[databases.size()]);
    }

    public void setQueryBandProperty(String queryBandProperty) throws SQLException {
        String queryBandSql = String.format("SET QUERY_BAND = '%s' For Session", queryBandProperty);
        Statement statement = this.connection.createStatement();
        statement.execute(queryBandSql);
        statement.close();
    }

    public void enableUnicodePassthrough() throws SQLException {
        Statement statement = this.connection.createStatement();
        statement.execute("SET SESSION CHARACTER SET UNICODE PASS THROUGH ON");
        statement.close();
    }

    public String getCurrentDatabase() throws SQLException {
        if (this.currentDatabase.isEmpty() && this.connection != null && !this.connection.isClosed()) {
            Statement statement = this.connection.createStatement();
            ResultSet resultSet = statement.executeQuery("SELECT DATABASE");
            if (resultSet.next()) {
                this.currentDatabase = resultSet.getString(1);
            }

            resultSet.close();
            statement.close();
        }

        return this.currentDatabase;
    }

    public static String getURLParamValue(String url, String paramName) {
        if (url != null && !url.isEmpty() && url.toLowerCase().indexOf("jdbc:") == 0) {
            if (paramName != null && !paramName.isEmpty()) {
                int pos = url.indexOf("://");
                if (pos < 0) {
                    return null;
                } else {
                    String urlparams = url.substring(pos + 3);
                    pos = urlparams.indexOf(47);
                    if (pos <= 0) {
                        return null;
                    } else {
                        urlparams = urlparams.substring(pos + 1);
                        ConnectorSchemaParser parser = new ConnectorSchemaParser();
                        parser.setDelimChar(',');
                        List<String> tokens = parser.tokenize(urlparams);
                        Iterator var6 = tokens.iterator();

                        String token;
                        do {
                            if (!var6.hasNext()) {
                                return null;
                            }

                            token = (String)var6.next();
                            pos = token.indexOf(61);
                        } while(pos <= 0 || !paramName.equalsIgnoreCase(token.substring(0, pos)));

                        return token.substring(pos + 1);
                    }
                }
            } else {
                return null;
            }
        } else {
            return null;
        }
    }

    public static Connection getConnection(String classname, String url, String username, String password, String properties) throws SQLException, ClassNotFoundException {
        if (url.charAt(url.length() - 1) == '/') {
            url = url + properties;
        } else {
            url = url + "," + properties;
        }

        Connection connection = null;
        Class.forName(classname);
        if (username == null) {
            connection = DriverManager.getConnection(url);
        } else {
            connection = DriverManager.getConnection(url, username, password);
        }

        return connection;
    }

    public static Connection getConnection(String classname, String url, byte[] usernameBytes, byte[] passwordBytes, String properties) throws SQLException, ClassNotFoundException {
        if (url.charAt(url.length() - 1) == '/') {
            url = url + properties;
        } else {
            url = url + "," + properties;
        }

        TeradataConnection teradata_connection = new TeradataConnection(classname, url, "", "", false);
        teradata_connection.connect(usernameBytes, passwordBytes);
        return teradata_connection.getConnection();
    }

    public static Connection getConnection(String classname, String url, byte[] usernameBytes, byte[] passwordBytes, String properties, boolean enableUnicodePassthrough) throws SQLException, ClassNotFoundException {
        if (url.charAt(url.length() - 1) == '/') {
            url = url + properties;
        } else {
            url = url + "," + properties;
        }

        TeradataConnection teradata_connection = new TeradataConnection(classname, url, "", "", false);
        teradata_connection.connect(usernameBytes, passwordBytes);
        if (enableUnicodePassthrough) {
            teradata_connection.enableUnicodePassthrough();
        }

        return teradata_connection.getConnection();
    }

    public static String getAMPCountSQL() {
        return "{fn teradata_amp_count()}";
    }

    public static String getTableRowCountSQL(String tableName, String condition) {
        String conditionExp = "";
        if (condition != null && !condition.isEmpty()) {
            conditionExp = " WHERE " + condition;
        }

        return String.format("SELECT CAST(COUNT(*) AS BIGINT) FROM %s %s", tableName, conditionExp);
    }

    public static String getIsTableNonEmptySQL(String tableName) {
        return String.format("SELECT CAST(COUNT(*) AS BIGINT) FROM %s", tableName);
    }

    public static String getTableDeleteStmt(String tableName) {
        return String.format("DELETE FROM %s", tableName);
    }

    public static String getCheckTableFastloadableSQL(String tableName, boolean useXView) {
        String dbExp;
        String tblExp;
        if (tableName != null && !tableName.isEmpty()) {
            dbExp = getDatabaseName(tableName);
            if (dbExp.isEmpty()) {
                dbExp = "(" + getDatabaseSQL() + ")";
            } else {
                dbExp = getQuotedValue(dbExp);
            }

            tblExp = getQuotedValue(getObjectName(tableName));
        } else {
            tblExp = "''";
            dbExp = "''";
        }

        return useXView ? String.format("SELECT COUNT(*) as NOTFASTLOADABLE FROM DBC.INDICESVX IDX   WHERE IDX.DATABASENAME = %s AND IDX.TABLENAME = %s AND IDX.INDEXTYPE NOT IN ('P', 'Q')  UNION ALL SELECT COUNT(*) AS NOTFASTLOADABLE FROM DBC.COLUMNSVX COL   WHERE COL.DATABASENAME = %s AND COL.TABLENAME = %s AND COL.COLUMNCONSTRAINT IS NOT NULL  UNION ALL SELECT COUNT(*) AS NOTFASTLOADABLE FROM DBC.TABLE_LEVELCONSTRAINTSVX TL   WHERE TL.DATABASENAME = %s AND TL.TABLENAME = %s  UNION ALL SELECT COUNT(*) AS NOTFASTLOADABLE FROM DBC.TRIGGERSVX TR   WHERE TR.DATABASENAME = %s AND TR.TABLENAME = %s  UNION ALL SELECT PARENTCOUNT + CHILDCOUNT AS NOTFASTLOADABLE FROM DBC.TABLESVX T   WHERE T.DATABASENAME = %s AND T.TABLENAME = %s", dbExp, tblExp, dbExp, tblExp, dbExp, tblExp, dbExp, tblExp, dbExp, tblExp) : String.format("SELECT COUNT(*) as NOTFASTLOADABLE FROM DBC.INDICESV IDX   WHERE IDX.DATABASENAME = %s AND IDX.TABLENAME = %s AND IDX.INDEXTYPE NOT IN ('P', 'Q')  UNION ALL SELECT COUNT(*) AS NOTFASTLOADABLE FROM DBC.COLUMNSV COL   WHERE COL.DATABASENAME = %s AND COL.TABLENAME = %s AND COL.COLUMNCONSTRAINT IS NOT NULL  UNION ALL SELECT COUNT(*) AS NOTFASTLOADABLE FROM DBC.TABLE_LEVELCONSTRAINTSV TL   WHERE TL.DATABASENAME = %s AND TL.TABLENAME = %s  UNION ALL SELECT COUNT(*) AS NOTFASTLOADABLE FROM DBC.TRIGGERSV TR   WHERE TR.DATABASENAME = %s AND TR.TABLENAME = %s  UNION ALL SELECT PARENTCOUNT + CHILDCOUNT AS NOTFASTLOADABLE FROM DBC.TABLESV T   WHERE T.DATABASENAME = %s AND T.TABLENAME = %s", dbExp, tblExp, dbExp, tblExp, dbExp, tblExp, dbExp, tblExp, dbExp, tblExp);
    }

    public static String getSelectSQL(String tableName, String[] columns, String condition) {
        return getSelectSQL(tableName, columns, condition, true);
    }

    public static String getSelectSQL(String tableName, String[] columns, String condition, boolean quoteColumnName) {
        StringBuilder colExpBuilder = new StringBuilder();
        if (columns != null && columns.length != 0) {
            for(int i = 0; i < columns.length; ++i) {
                if (i > 0) {
                    colExpBuilder.append(", ");
                }

                colExpBuilder.append(quoteColumnName ? getQuotedName(columns[i]) : columns[i]);
            }
        } else {
            colExpBuilder.append('*');
        }

        String conditionExp = "";
        if (condition != null && !condition.isEmpty()) {
            conditionExp = " WHERE " + condition;
        }

        return String.format("SELECT %s FROM %s %s", colExpBuilder.toString(), tableName, conditionExp);
    }

    public static String getDeleteTableSQL(String tableName, String condition) {
        String conditionExp = "";
        if (condition != null && !condition.isEmpty()) {
            conditionExp = " WHERE " + condition;
        }

        return String.format("DELETE FROM %s %s", tableName, conditionExp);
    }

    public static String getInsertPreparedStatmentSQL(String tableName, String[] columns) {
        StringBuilder colExpBuilder = new StringBuilder();
        StringBuilder valuesExpBuilder = new StringBuilder();

        for(int i = 0; i < columns.length; ++i) {
            if (i > 0) {
                colExpBuilder.append(", ");
                valuesExpBuilder.append(", ");
            }

            colExpBuilder.append(getQuotedName(columns[i]));
            valuesExpBuilder.append('?');
        }

        return String.format("INSERT INTO %s (%s) VALUES (%s)", tableName, colExpBuilder.toString(), valuesExpBuilder.toString());
    }

    public static String getInsertPreparedStatmentSQLWithTaskID(String tableName, String[] columns, String taskID) {
        StringBuilder colExpBuilder = new StringBuilder();
        StringBuilder valuesExpBuilder = new StringBuilder();

        for(int i = 0; i < columns.length; ++i) {
            if (i > 0) {
                colExpBuilder.append(", ");
                valuesExpBuilder.append(", ");
            }

            colExpBuilder.append(getQuotedName(columns[i]));
            if (columns[i].equals(TeradataBatchInsertProcessor.taskIDColumnName)) {
                valuesExpBuilder.append("'" + taskID + "'");
            } else {
                valuesExpBuilder.append('?');
            }
        }

        return String.format("INSERT INTO %s (%s) VALUES (%s)", tableName, colExpBuilder.toString(), valuesExpBuilder.toString());
    }

    public static String getInsertSelectSQL(String targetTableName, String[] targetColumns, String sourceTableName, String[] sourceColumns) {
        StringBuilder targetColExpBuilder = new StringBuilder();
        StringBuilder sourceColExpBuilder = new StringBuilder();

        int i;
        for(i = 0; i < targetColumns.length; ++i) {
            if (i > 0) {
                targetColExpBuilder.append(", ");
            }

            targetColExpBuilder.append(getQuotedName(targetColumns[i]));
        }

        for(i = 0; i < sourceColumns.length; ++i) {
            if (i > 0) {
                sourceColExpBuilder.append(", ");
            }

            sourceColExpBuilder.append(getQuotedName(sourceColumns[i]));
        }

        return String.format("INSERT INTO %s (%s) SELECT %s FROM %s", targetTableName, targetColExpBuilder.toString(), sourceColExpBuilder.toString(), sourceTableName);
    }

    public static String getInsertSelectSQL(String targetTableName, String targetQuotedColumns, String sourceTableName, String sourceQuotedColumns) {
        return String.format("INSERT INTO %s (%s) SELECT %s FROM %s", targetTableName, targetQuotedColumns, sourceQuotedColumns, sourceTableName);
    }

    public static String getDropTableSQL(String tableName) {
        return String.format("DROP TABLE %s", tableName);
    }

    public static String getCreateTableSQL(TeradataTableDesc tableDesc) {
        String tableType = tableDesc.isMultiset() ? "MULTISET" : "SET";
        StringBuilder colExpBuilder = new StringBuilder();
        StringBuilder parColExpBuilder = new StringBuilder();
        StringBuilder piColExpBuilder = new StringBuilder();
        TeradataColumnDesc[] columns = tableDesc.getColumns();

        String piColExp;
        for(int i = 0; i < columns.length; ++i) {
            if (i > 0) {
                colExpBuilder.append(", ");
            }

            TeradataColumnDesc column = columns[i];
            piColExp = getQuotedName(column.getName());
            colExpBuilder.append(piColExp).append(' ').append(column.getTypeString());
        }

        Iterator piitr = tableDesc.getPartitionColumnNames().iterator();

        while(piitr.hasNext()) {
            parColExpBuilder.append(getQuotedName(((String)piitr.next()).toString()));
            if (piitr.hasNext()) {
                parColExpBuilder.append(", ");
            }
        }

        piitr = tableDesc.getPrimaryIndices().iterator();

        while(piitr.hasNext()) {
            piColExpBuilder.append(getQuotedName(((String)piitr.next()).toString()));
            if (piitr.hasNext()) {
                piColExpBuilder.append(", ");
            }
        }

        String blockExp = tableDesc.getBlockSize() > 0 ? "DATABLOCKSIZE = " + tableDesc.getBlockSize() + " BYTES," : "";
        String partitionColExp = parColExpBuilder.length() > 0 ? "PARTITION BY " + parColExpBuilder : " ";
        piColExp = "";
        if (!tableDesc.hasPrimaryIndex()) {
            piColExp = "NO PRIMARY INDEX";
        } else if (piColExpBuilder.length() > 0) {
            piColExp = "PRIMARY INDEX (" + piColExpBuilder.toString() + ")";
        }

        return String.format("CREATE %s TABLE %s, %s NO FALLBACK, NO BEFORE JOURNAL, NO AFTER JOURNAL, CHECKSUM = DEFAULT (%s) %s %s", tableType, tableDesc.getQualifiedName(), blockExp, colExpBuilder.toString(), piColExp, partitionColExp);
    }

    public static String getCreateViewSQL(TeradataViewDesc viewDesc) {
        Boolean accessLock = viewDesc.getAccessLock();
        return accessLock ? String.format("CREATE VIEW %s (%s) AS %s", viewDesc.getQualifiedName(), viewDesc.getColumnsString(), addAccessLockToSql(viewDesc.getQuery())) : String.format("CREATE VIEW %s (%s) AS %s", viewDesc.getQualifiedName(), viewDesc.getColumnsString(), viewDesc.getQuery());
    }

    public static String getDropViewSQL(String viewName) {
        return String.format("DROP VIEW %s", viewName);
    }

    public static String getUsingSQL(String tableName, String[] columns, String[] types4Using, String charset) {
        StringBuilder targetColExpBuilder = new StringBuilder();
        StringBuilder usingColExpBuilder = new StringBuilder();
        StringBuilder usingValExpBuilder = new StringBuilder();

        for(int i = 0; i < columns.length; ++i) {
            if (i > 0) {
                targetColExpBuilder.append(", ");
                usingColExpBuilder.append(", ");
                usingValExpBuilder.append(", ");
            }

            String quotedUsingCol = getQuotedName(columns[i]);
            String quotedCol = getQuotedName(columns[i]);
            targetColExpBuilder.append(quotedCol);
            usingColExpBuilder.append(quotedUsingCol).append(" (").append(types4Using[i]).append(')');
            usingValExpBuilder.append(':').append(quotedUsingCol);
        }

        return String.format("USING %s INSERT INTO %s ( %s ) VALUES ( %s )", usingColExpBuilder.toString(), tableName, targetColExpBuilder.toString(), usingValExpBuilder.toString());
    }

    public static String getDatabaseSQL() {
        return "SELECT DATABASE";
    }

    public static String getCurTimestampSQL() {
        return "SELECT CURRENT_TIMESTAMP";
    }

    public static String getListDatabasesSQL(boolean useXView) {
        return useXView ? "SELECT TRIM(TRAILING FROM DBS.DATABASENAME) AS DATABASENAME FROM DBC.DATABASESVX DBS" : "SELECT TRIM(TRAILING FROM DBS.DATABASENAME) AS DATABASENAME FROM DBC.DATABASESV DBS";
    }

    public static String getPrimaryKeySQL(String tableName, boolean useXView) {
        String dbExp;
        String tblExp;
        if (tableName != null && !tableName.isEmpty()) {
            dbExp = getDatabaseName(tableName);
            if (dbExp.isEmpty()) {
                dbExp = "(" + getDatabaseSQL() + ")";
            } else {
                dbExp = getQuotedValue(dbExp);
            }

            tblExp = getQuotedValue(getObjectName(tableName));
        } else {
            tblExp = "''";
            dbExp = "''";
        }

        return useXView ? String.format("SELECT TRIM(TRAILING FROM RST.COLUMNNAME) AS COLUMNNAME FROM (   SELECT 1 AS INDEXORDER, I.INDEXNUMBER, I.COLUMNNAME, I.COLUMNPOSITION FROM DBC.INDICESVX I   WHERE I.INDEXTYPE = 'K' AND I.UNIQUEFLAG = 'Y' AND I.DATABASENAME = %s AND I.TABLENAME = %s UNION ALL   SELECT 2 AS INDEXORDER, I.INDEXNUMBER, I.COLUMNNAME, I.COLUMNPOSITION FROM DBC.INDICESVX I   WHERE I.INDEXTYPE = 'P' AND I.UNIQUEFLAG = 'Y' AND I.DATABASENAME = %s AND I.TABLENAME = %s UNION ALL   SELECT 3 AS INDEXORDER, I.INDEXNUMBER, I.COLUMNNAME, I.COLUMNPOSITION FROM DBC.INDICESVX I   WHERE I.INDEXTYPE = 'S' AND I.UNIQUEFLAG = 'Y' AND I.DATABASENAME = %s AND I.TABLENAME = %s ) RST QUALIFY RANK(RST.INDEXORDER ASC, RST.INDEXNUMBER ASC) = 1 ORDER BY RST.COLUMNPOSITION ASC", dbExp, tblExp, dbExp, tblExp, dbExp, tblExp) : String.format("SELECT TRIM(TRAILING FROM RST.COLUMNNAME) AS COLUMNNAME FROM (   SELECT 1 AS INDEXORDER, I.INDEXNUMBER, I.COLUMNNAME, I.COLUMNPOSITION FROM DBC.INDICESV I   WHERE I.INDEXTYPE = 'K' AND I.UNIQUEFLAG = 'Y' AND I.DATABASENAME = %s AND I.TABLENAME = %s UNION ALL   SELECT 2 AS INDEXORDER, I.INDEXNUMBER, I.COLUMNNAME, I.COLUMNPOSITION FROM DBC.INDICESV I   WHERE I.INDEXTYPE = 'P' AND I.UNIQUEFLAG = 'Y' AND I.DATABASENAME = %s AND I.TABLENAME = %s UNION ALL   SELECT 3 AS INDEXORDER, I.INDEXNUMBER, I.COLUMNNAME, I.COLUMNPOSITION FROM DBC.INDICESV I   WHERE I.INDEXTYPE = 'S' AND I.UNIQUEFLAG = 'Y' AND I.DATABASENAME = %s AND I.TABLENAME = %s ) RST QUALIFY RANK(RST.INDEXORDER ASC, RST.INDEXNUMBER ASC) = 1 ORDER BY RST.COLUMNPOSITION ASC", dbExp, tblExp, dbExp, tblExp, dbExp, tblExp);
    }

    public static String getPrimaryIndexSQL(String tableName, boolean useXView) {
        String dbExp;
        String tblExp;
        if (tableName != null && !tableName.isEmpty()) {
            dbExp = getDatabaseName(tableName);
            if (dbExp.isEmpty()) {
                dbExp = "(" + getDatabaseSQL() + ")";
            } else {
                dbExp = getQuotedValue(dbExp);
            }

            tblExp = getQuotedValue(getObjectName(tableName));
        } else {
            tblExp = "''";
            dbExp = "''";
        }

        return useXView ? String.format("SELECT TRIM(TRAILING FROM I.COLUMNNAME) AS COLUMNNAME FROM DBC.INDICESVX I  WHERE I.DATABASENAME = %s AND I.TABLENAME = %s AND (I.INDEXTYPE IN ('P', 'Q'))", dbExp, tblExp) : String.format("SELECT TRIM(TRAILING FROM I.COLUMNNAME) AS COLUMNNAME FROM DBC.INDICESV I  WHERE I.DATABASENAME = %s AND I.TABLENAME = %s AND (I.INDEXTYPE IN ('P', 'Q'))", dbExp, tblExp);
    }

    public static String getPartitionedPrimaryIndexSQL(String tableName, boolean useXView) {
        String dbExp;
        String tblExp;
        if (tableName != null && !tableName.isEmpty()) {
            dbExp = getDatabaseName(tableName);
            if (dbExp.isEmpty()) {
                dbExp = "(" + getDatabaseSQL() + ")";
            } else {
                dbExp = getQuotedValue(dbExp);
            }

            tblExp = getQuotedValue(getObjectName(tableName));
        } else {
            tblExp = "''";
            dbExp = "''";
        }

        return useXView ? String.format("SELECT TRIM(TRAILING FROM I.COLUMNNAME) AS COLUMNNAME FROM DBC.INDICESVX I  WHERE UPPER(I.DATABASENAME) = UPPER(%s) AND I.TABLENAME = %s AND (I.INDEXTYPE = 'Q')", dbExp, tblExp) : String.format("SELECT TRIM(TRAILING FROM I.COLUMNNAME) AS COLUMNNAME FROM DBC.INDICESV I  WHERE UPPER(I.DATABASENAME) = UPPER(%s) AND I.TABLENAME = %s AND (I.INDEXTYPE = 'Q')", dbExp, tblExp);
    }

    public static String getListTablesSQL(String databaseName, boolean useXView) {
        String dbExp;
        if (databaseName != null && !databaseName.isEmpty()) {
            dbExp = getQuotedValue(databaseName);
        } else {
            dbExp = "(" + getDatabaseSQL() + ")";
        }

        return useXView ? String.format("SELECT TRIM(TRAILING FROM T.TABLENAME) AS TABLENAME FROM DBC.TABLESVX T WHERE T.DATABASENAME = %s and (T.TABLEKIND IN ('O', 'T'))", dbExp) : String.format("SELECT TRIM(TRAILING FROM T.TABLENAME) AS TABLENAME FROM DBC.TABLESV T WHERE T.DATABASENAME = %s and (T.TABLEKIND IN ('O', 'T'))", dbExp);
    }

    public static String getTableKindSQL(String tableName, boolean useXView) {
        String dbExp;
        String tblExp;
        if (tableName != null && !tableName.isEmpty()) {
            dbExp = getDatabaseName(tableName);
            if (dbExp.isEmpty()) {
                dbExp = "(" + getDatabaseSQL() + ")";
            } else {
                dbExp = getQuotedValue(dbExp);
            }

            tblExp = getQuotedValue(getObjectName(tableName));
        } else {
            tblExp = "''";
            dbExp = "''";
        }

        return useXView ? String.format("SELECT TRIM(T.TABLEKIND) AS TABLEKIND FROM DBC.TABLESVX T WHERE T.DATABASENAME = %s AND T.TABLENAME = %s", dbExp, tblExp) : String.format("SELECT TRIM(T.TABLEKIND) AS TABLEKIND FROM DBC.TABLESV T WHERE T.DATABASENAME = %s AND T.TABLENAME = %s", dbExp, tblExp);
    }

    public static String getListColumnsSQL(String tableName, boolean useXView) {
        String dbExp;
        String tblExp;
        if (tableName != null && !tableName.isEmpty()) {
            dbExp = getDatabaseName(tableName);
            if (dbExp.isEmpty()) {
                dbExp = "(" + getDatabaseSQL() + ")";
            } else {
                dbExp = getQuotedValue(dbExp);
            }

            tblExp = getQuotedValue(getObjectName(tableName));
        } else {
            tblExp = "''";
            dbExp = "''";
        }

        return useXView ? String.format("SELECT TRIM(TRAILING FROM C.COLUMNNAME) AS COLUMNNAME FROM DBC.COLUMNSVX C  WHERE C.DATABASENAME = %s AND C.TABLENAME = %s ORDER BY COLUMNID", dbExp, tblExp) : String.format("SELECT TRIM(TRAILING FROM C.COLUMNNAME) AS COLUMNNAME FROM DBC.COLUMNSV C  WHERE C.DATABASENAME = %s AND C.TABLENAME = %s ORDER BY COLUMNID", dbExp, tblExp);
    }

    public static String getListColumnInfosSQL(String tableName, boolean useXView) {
        String dbExp;
        String tblExp;
        if (tableName != null && !tableName.isEmpty()) {
            dbExp = getDatabaseName(tableName);
            if (dbExp.isEmpty()) {
                dbExp = "(" + getDatabaseSQL() + ")";
            } else {
                dbExp = getQuotedValue(dbExp);
            }

            tblExp = getQuotedValue(getObjectName(tableName));
        } else {
            tblExp = "''";
            dbExp = "''";
        }

        return useXView ? String.format("SELECT TRIM(TRAILING FROM C.COLUMNNAME) AS COLUMNNAME, CHARTYPE FROM DBC.COLUMNSVX C  WHERE C.DATABASENAME = %s AND C.TABLENAME = %s ORDER BY COLUMNID", dbExp, tblExp) : String.format("SELECT TRIM(TRAILING FROM C.COLUMNNAME) AS COLUMNNAME, CHARTYPE FROM DBC.COLUMNSV C  WHERE C.DATABASENAME = %s AND C.TABLENAME = %s ORDER BY COLUMNID", dbExp, tblExp);
    }

    public static String getDatabaseName(String dbObject) {
        ConnectorSchemaParser parser = new ConnectorSchemaParser();
        parser.setDelimChar('.');
        List<String> tokens = parser.tokenize(dbObject, 2, false);
        return tokens.size() == 2 ? (String)tokens.get(0) : "";
    }

    public static String getObjectName(String dbObject) {
        ConnectorSchemaParser parser = new ConnectorSchemaParser();
        parser.setDelimChar('.');
        List<String> tokens = parser.tokenize(dbObject, 2, false);
        switch(tokens.size()) {
            case 0:
                return "";
            case 1:
                return (String)tokens.get(0);
            default:
                return (String)tokens.get(1);
        }
    }

    public static String getQuotedName(String name) {
        return TeradataSchemaUtils.quoteFieldNameForSql(name);
    }

    public static String getQuotedValue(String value) {
        return TeradataSchemaUtils.quoteFieldValueForSql(value);
    }

    public static String getQuotedColumnNames(String columnName) {
        return TeradataSchemaUtils.quoteFieldNamesForSql(columnName);
    }

    public static String getEscapedName(String name) {
        return name == null ? "" : name.replace("\"", "\"\"");
    }

    public static String getEscapedValue(String value) {
        return value == null ? "" : value.replace("'", "''");
    }

    public static String getQuotedEscapedName(String... nameparts) {
        StringBuilder builder = new StringBuilder();
        String[] var2 = nameparts;
        int var3 = nameparts.length;

        for(int var4 = 0; var4 < var3; ++var4) {
            String namepart = var2[var4];
            if (!ConnectorStringUtils.isEmpty(namepart)) {
                builder.append('"').append(getEscapedName(namepart)).append("\".");
            }
        }

        if (builder.length() > 0) {
            builder.setLength(builder.length() - 1);
        }

        return builder.toString();
    }

    public static String getQuotedEscapedValue(String value) {
        return "'" + getEscapedValue(value) + "'";
    }

    public static String addAccessLockToSql(String selectQuery) {
        return "LOCK ROW FOR ACCESS " + selectQuery;
    }

    public static void setQueryBandProperty(Connection specifiedConnection, String queryBandProperty) throws SQLException {
        String queryBandSql = String.format("SET QUERY_BAND = '%s' For Session", queryBandProperty);
        Statement statement = specifiedConnection.createStatement();
        statement.execute(queryBandSql);
        statement.close();
    }

    public static void enableUnicodePassthrough(Connection specifiedConnection) throws SQLException {
        Statement statement = specifiedConnection.createStatement();
        statement.execute("SET SESSION CHARACTER SET UNICODE PASS THROUGH ON");
        statement.close();
    }

    protected static String getUsingSQL(String tableName, TeradataColumnDesc[] columnDescs, int columnCount, String charset) {
        int strCharOrVarcharMultiplier = 1;
        int strTimeOrIntervalMultiplier = 1;
        if ("UTF16".equalsIgnoreCase(charset)) {
            strCharOrVarcharMultiplier = 2;
            strTimeOrIntervalMultiplier = 2;
        } else if ("UTF8".equalsIgnoreCase(charset)) {
            strCharOrVarcharMultiplier = 3;
        }

        StringBuilder usingColExpBuilder = new StringBuilder();
        StringBuilder usingValExpBuilder = new StringBuilder();
        StringBuilder targetColExpBuilder = new StringBuilder();
        int length = columnDescs.length;

        for(int i = 0; i < Math.min(length, columnCount); ++i) {
            if (i > 0) {
                usingColExpBuilder.append(", ");
                usingValExpBuilder.append(", ");
                targetColExpBuilder.append(", ");
            }

            TeradataColumnDesc column = columnDescs[i];
            String quotedColName = getQuotedName(column.getName());
            targetColExpBuilder.append(quotedColName);
            usingValExpBuilder.append(':').append(quotedColName);
            int type = column.getType();
            int scale = column.getScale();
            int tsnanoLength;
            long mlen;
            switch(type) {
                case 0:
                    usingColExpBuilder.append(quotedColName).append(" (").append(column.getTypeStringWithoutNullability()).append(")");
                    break;
                case 1:
                case 12:
                    mlen = strCharOrVarcharMultiplier * column.getLength();
                    usingColExpBuilder.append(quotedColName).append(" (VARCHAR(").append(mlen).append("))");
                    break;
                case 3:
                    usingColExpBuilder.append(quotedColName).append(" (DECIMAL (38, ").append(scale).append("))");
                    break;
                case 92:
                    tsnanoLength = scale > 0 ? 8 + scale + 1 : 8;
                    mlen = strTimeOrIntervalMultiplier * tsnanoLength;
                    usingColExpBuilder.append(quotedColName).append(" (CHAR(").append(mlen).append("))");
                    break;
                case 93:
                    tsnanoLength = scale > 0 ? 19 + scale + 1 : 19;
                    mlen = strTimeOrIntervalMultiplier * tsnanoLength;
                    usingColExpBuilder.append(quotedColName).append(" (CHAR(").append(mlen).append("))");
                    break;
                default:
                    usingColExpBuilder.append(quotedColName).append(" (").append(column.getTypeStringWithoutNullability()).append(")");
            }
        }

        return String.format("USING %s INSERT INTO %s ( %s ) VALUES ( %s )", tableName, usingColExpBuilder.toString(), targetColExpBuilder.toString(), usingValExpBuilder.toString());
    }

    protected int getColumnCount(String sql) throws SQLException {
        PreparedStatement prepareStatement = this.connection.prepareStatement(sql);
        ResultSetMetaData metadata = prepareStatement.getMetaData();
        int columnCount = metadata.getColumnCount();
        prepareStatement.close();
        return columnCount;
    }

    protected String[] getColumnNames(ResultSetMetaData metadata) throws SQLException {
        int columnCount = metadata.getColumnCount();
        String[] columnNames = new String[columnCount];

        for(int i = 1; i <= columnCount; ++i) {
            columnNames[i - 1] = metadata.getColumnName(i);
        }

        return columnNames;
    }

    protected TeradataColumnDesc[] getColumnDescs(ResultSetMetaData metadata) throws SQLException {
        JDK6_SQL_ResultSetMetaData tdMetadata = (JDK6_SQL_ResultSetMetaData)metadata;
        int columnCount = metadata.getColumnCount();
        TeradataColumnDesc[] columns = new TeradataColumnDesc[columnCount];

        for(int i = 1; i <= columnCount; ++i) {
            ColumnProperties columnProperties = tdMetadata.getColumnProperties(i);
            TeradataColumnDesc column = new TeradataColumnDesc();
            String columnTypeName = metadata.getColumnTypeName(i).toUpperCase();
            column.setName(metadata.getColumnName(i));
            int columnType = metadata.getColumnType(i);
            if (6 == columnType) {
                columnType = 8;
            }

            if (1111 == columnType && columnTypeName.equals("JSON")) {
                columnType = 2005;
            }

            column.setType(columnType);
            column.setTypeName(metadata.getColumnTypeName(i));
            column.setClassName(metadata.getColumnClassName(i));
            column.setFormat(columnProperties.getColumnFormat());
            column.setNullable(metadata.isNullable(i) > 0);
            column.setLength((long)metadata.getColumnDisplaySize(i));
            column.setCaseSensitive(metadata.isCaseSensitive(i));
            column.setPrecision(metadata.getPrecision(i));
            column.setScale(metadata.getScale(i));
            columns[i - 1] = column;
        }

        return columns;
    }

    public static URLParameters getJDBCURLParameters(String url) throws SQLException {
        String prefix = "jdbc:teradata://";
        String remain = url.substring(prefix.length());
        int slashPosition = remain.indexOf("/");
        String paramString = "";
        if (slashPosition >= 0) {
            paramString = remain.substring(slashPosition + 1);
        } else {
            paramString = "";
        }

        return new URLParameters(paramString);
    }
}
