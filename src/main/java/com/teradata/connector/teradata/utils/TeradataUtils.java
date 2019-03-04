package com.teradata.connector.teradata.utils;

import com.teradata.connector.common.exception.ConnectorException;
import com.teradata.connector.common.exception.ConnectorException.ErrorCode;
import com.teradata.connector.common.utils.ConnectorConfiguration;
import com.teradata.connector.common.utils.ConnectorConfiguration.direction;
import com.teradata.connector.teradata.db.TeradataConnection;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.HashSet;
import java.util.Set;
import java.util.jar.Attributes;
import java.util.jar.JarInputStream;
import java.util.jar.Manifest;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.JobContext;

public class TeradataUtils {
    private static Log logger;
    protected static final String TDCH_VERSION = "Implementation-Version";

    public static TeradataConnection openInputConnection(final JobContext context) throws ConnectorException {
        TeradataConnection connection = null;
        try {
            final Configuration configuration = context.getConfiguration();
            final String jdbcClassName = TeradataPlugInConfiguration.getInputJdbcDriverClass(configuration);
            final String jdbcUrl = TeradataPlugInConfiguration.getInputJdbcUrl(configuration);
            final Boolean useXview = TeradataPlugInConfiguration.getInputDataDictionaryUseXView(configuration);
            if (jdbcUrl != null && jdbcUrl.length() != 0) {
                connection = new TeradataConnection(jdbcClassName, jdbcUrl, "", "", useXview);
                connection.connect(TeradataPlugInConfiguration.getInputTeradataUserName(context), TeradataPlugInConfiguration.getInputTeradataPassword(context));
                final String queryBandProperty = TeradataPlugInConfiguration.getInputQueryBand(configuration);
                if (queryBandProperty != null && !queryBandProperty.isEmpty()) {
                    try {
                        connection.setQueryBandProperty(queryBandProperty);
                    } catch (SQLException e) {
                        throw new ConnectorException(e.getMessage(), e);
                    }
                }
                final boolean enableUnicodePassthrough = TeradataPlugInConfiguration.getUnicodePassthrough(configuration);
                if (enableUnicodePassthrough) {
                    try {
                        connection.enableUnicodePassthrough();
                    } catch (SQLException e2) {
                        throw new ConnectorException(e2.getMessage(), e2);
                    }
                }
                connection.getDatabaseProperty();
            }
            return connection;
        } catch (SQLException e3) {
            connection = null;
            throw new ConnectorException(e3.getMessage(), e3);
        } catch (ClassNotFoundException e4) {
            throw new ConnectorException(e4.getMessage(), e4);
        }
    }

    public static TeradataConnection openOutputConnection(final JobContext context) throws ConnectorException {
        TeradataConnection connection = null;
        try {
            final Configuration configuration = context.getConfiguration();
            final String jdbcClassName = TeradataPlugInConfiguration.getOutputJdbcDriverClass(configuration);
            final String jdbcUrl = TeradataPlugInConfiguration.getOutputJdbcUrl(configuration);
            final Boolean useXview = TeradataPlugInConfiguration.getOutputDataDictionaryUseXView(configuration);
            if (jdbcUrl != null && jdbcUrl.length() != 0) {
                connection = new TeradataConnection(jdbcClassName, jdbcUrl, "", "", useXview);
                connection.connect(TeradataPlugInConfiguration.getOutputTeradataUserName(context), TeradataPlugInConfiguration.getOutputTeradataPassword(context));
                final String queryBandProperty = TeradataPlugInConfiguration.getOutputQueryBand(configuration);
                if (queryBandProperty != null && !queryBandProperty.isEmpty()) {
                    try {
                        connection.setQueryBandProperty(queryBandProperty);
                    } catch (SQLException e) {
                        throw new ConnectorException(e.getMessage(), e);
                    }
                }
                final boolean enableUnicodePassthrough = TeradataPlugInConfiguration.getUnicodePassthrough(configuration);
                if (enableUnicodePassthrough) {
                    try {
                        connection.enableUnicodePassthrough();
                    } catch (SQLException e2) {
                        throw new ConnectorException(e2.getMessage(), e2);
                    }
                }
                connection.getDatabaseProperty();
            }
            return connection;
        } catch (SQLException e3) {
            connection = null;
            throw new ConnectorException(e3.getMessage(), e3);
        } catch (ClassNotFoundException e4) {
            throw new ConnectorException(e4.getMessage(), e4);
        }
    }

    public static void closeConnection(TeradataConnection connection) throws ConnectorException {
        if (connection != null) {
            connection.close();
            connection = null;
        }
    }

    public static void closeConnection(Connection connection) throws ConnectorException {
        try {
            if (connection != null && !connection.isClosed()) {
                connection.close();
            }
        } catch (SQLException e) {
            throw new ConnectorException(e.getMessage(), e);
        } finally {
            connection = null;
        }
    }

    public static void validateQueryBand(final String queryBandProperty) throws ConnectorException {
        if (!queryBandProperty.isEmpty()) {
            if (queryBandProperty.length() > 2046) {
                throw new ConnectorException(21001);
            }
            int equalNumber = 0;
            int semicolonNumber = 0;
            int queryBandNamePostition = 0;
            int queryBandValuePosition = 0;
            final Set<String> queryKeys = new HashSet<String>();
            for (int i = 0, length = queryBandProperty.length(); i < length; ++i) {
                if (queryBandProperty.charAt(i) == '=') {
                    ++equalNumber;
                    queryBandValuePosition = i + 1;
                    final String queryBandName = queryBandProperty.substring(queryBandNamePostition, i).trim();
                    if (queryBandName.length() > 128) {
                        throw new ConnectorException(21003);
                    }
                    if (queryKeys.contains(queryBandName)) {
                        throw new ConnectorException(21002);
                    }
                    queryKeys.add(queryBandName);
                } else if (queryBandProperty.charAt(i) == ';') {
                    ++semicolonNumber;
                    queryBandNamePostition = i + 1;
                    final String queryBandValue = queryBandProperty.substring(queryBandValuePosition, i);
                    if (queryBandValue.length() > 256) {
                        throw new ConnectorException(21004);
                    }
                }
            }
            if (equalNumber == 0 || equalNumber != semicolonNumber) {
                throw new ConnectorException(21005);
            }
            queryKeys.clear();
        }
    }

    public static void validateDatabase(final TeradataConnection connection, final ConnectorConfiguration.direction direction) throws ConnectorException {
        try {
            if (connection != null) {
                connection.getDatabaseProperty();
            }
        } catch (SQLException e) {
            throw new ConnectorException(e.getMessage(), e);
        }
        if (direction == ConnectorConfiguration.direction.output) {
            TeradataUtils.logger.info((Object) ("the output database product is " + connection.getDatabaseProductName()));
            TeradataUtils.logger.info((Object) ("the output database version is " + connection.getDatabaseMajorVersion() + "." + connection.getDatabaseMinorVersion()));
        } else {
            TeradataUtils.logger.info((Object) ("the input database product is " + connection.getDatabaseProductName()));
            TeradataUtils.logger.info((Object) ("the input database version is " + connection.getDatabaseMajorVersion() + "." + connection.getDatabaseMinorVersion()));
        }
        TeradataUtils.logger.info((Object) ("the jdbc driver version is " + connection.getJDBCMajorVersion() + "." + connection.getJDBCMinorVersion()));
        if (!connection.isSupportedDatabase()) {
            throw new ConnectorException(200010);
        }
        if (!connection.isSupportedJDBC()) {
            throw new ConnectorException(20001);
        }
        try {
            if (direction == ConnectorConfiguration.direction.output) {
                TeradataUtils.logger.debug((Object) ("the amp count of output database is: " + connection.getAMPCount()));
            } else {
                TeradataUtils.logger.debug((Object) ("the amp count of input database is: " + connection.getAMPCount()));
            }
        } catch (SQLException e) {
            throw new ConnectorException(e.getMessage(), e);
        }
    }

    public static void validateConnectivity(final Configuration configuration, final TeradataConnection connection, final ConnectorConfiguration.direction direction) throws ConnectorException {
        try {
            connection.getDatabaseProperty();
            if (!connection.isSupportedDatabase()) {
                throw new ConnectorException(200010);
            }
            if (!connection.isSupportedJDBC()) {
                throw new ConnectorException(20001);
            }
            final int numAmps = connection.getAMPCount();
            if (direction == ConnectorConfiguration.direction.input) {
                TeradataPlugInConfiguration.setInputNumAmps(configuration, numAmps);
            } else {
                TeradataPlugInConfiguration.setOutputNumAmps(configuration, numAmps);
            }
        } catch (SQLException e) {
            throw new ConnectorException(e.getMessage(), e);
        }
    }

    public static String getTdchVersionNumber() {
        String version = "";
        Manifest manifest = null;
        try {
            final File path = new File(TeradataUtils.class.getProtectionDomain().getCodeSource().getLocation().getPath());
            if (path.isDirectory()) {
                final File mfpath = new File(TeradataUtils.class.getProtectionDomain().getCodeSource().getLocation().getPath() + File.separator + "META-INF/MANIFEST.MF");
                if (mfpath.exists()) {
                    manifest = new Manifest(new FileInputStream(mfpath));
                }
            } else {
                final JarInputStream jarStream = new JarInputStream(new FileInputStream(path));
                manifest = jarStream.getManifest();
                jarStream.close();
            }
            if (manifest != null) {
                final Attributes mainAttribs = manifest.getMainAttributes();
                version = mainAttribs.getValue("Implementation-Version");
            }
        } catch (IOException e) {
            TeradataUtils.logger.warn((Object) "Failed to get the Teradata connector for hadoop version number.");
        }
        return version;
    }

    public static void validateInputTeradataProperties(final Configuration configuration, final TeradataConnection connection) throws ConnectorException {
        final String inputQuery = TeradataPlugInConfiguration.getInputQuery(configuration);
        final String inputDatabase = TeradataPlugInConfiguration.getInputDatabase(configuration);
        String inputTableName = TeradataPlugInConfiguration.getInputTable(configuration);
        String[] inputTableFieldNames = TeradataPlugInConfiguration.getInputFieldNamesArray(configuration);
        final String inputCondition = TeradataPlugInConfiguration.getInputConditions(configuration);
        final String objectName = inputTableName;
        inputTableName = TeradataConnection.getQuotedEscapedName(inputDatabase, inputTableName);
        if (inputQuery.isEmpty()) {
            if (objectName.isEmpty()) {
                throw new ConnectorException(12007);
            }
            if (inputTableFieldNames == null || inputTableFieldNames.length == 0) {
                inputTableFieldNames = new String[]{"*"};
            }
        } else {
            if (!objectName.isEmpty()) {
                throw new ConnectorException(12008);
            }
            final int maxLength = connection.getMaxTableNameLength();
            if (objectName.length() > maxLength) {
                throw new ConnectorException(12012, new Object[]{maxLength});
            }
        }
        if (!inputCondition.isEmpty() && inputCondition.toLowerCase().contains("where")) {
            throw new ConnectorException("input condition can't include where keyword");
        }
        validateQueryBand(TeradataPlugInConfiguration.getInputQueryBand(configuration));
        validateDatabase(connection, ConnectorConfiguration.direction.input);
        validateConnectivity(configuration, connection, ConnectorConfiguration.direction.input);
        try {
            String[] sourceFieldNames;
            if (!inputTableName.isEmpty()) {
                sourceFieldNames = connection.getColumnNamesForTable(inputTableName);
            } else {
                sourceFieldNames = connection.getColumnNamesForSQL(inputQuery);
            }
            if (inputTableFieldNames.length != 0 && (inputTableFieldNames.length != 1 || !inputTableFieldNames[0].equals("*"))) {
                for (int i = 0, length = inputTableFieldNames.length; i < length; ++i) {
                    int j;
                    int inputFieldsCount;
                    for (j = 0, inputFieldsCount = sourceFieldNames.length; j < inputFieldsCount && !inputTableFieldNames[i].equalsIgnoreCase(sourceFieldNames[j]); ++j) {
                    }
                    if (j == inputFieldsCount) {
                        throw new ConnectorException(14005);
                    }
                }
            }
        } catch (SQLException e) {
            throw new ConnectorException(e.getMessage(), e);
        }
    }

    public static void validateOutputTeradataProperties(final Configuration configuration, final TeradataConnection connection) throws ConnectorException {
        final String outputDatabase = TeradataPlugInConfiguration.getOutputDatabase(configuration);
        final String objectName;
        String outputTableName = objectName = TeradataPlugInConfiguration.getOutputTable(configuration);
        outputTableName = TeradataConnection.getQuotedEscapedName(outputDatabase, outputTableName);
        validateQueryBand(TeradataPlugInConfiguration.getOutputQueryBand(configuration));
        validateDatabase(connection, ConnectorConfiguration.direction.output);
        validateConnectivity(configuration, connection, ConnectorConfiguration.direction.output);
        if (objectName.isEmpty()) {
            throw new ConnectorException(13007);
        }
        final int maxLength = connection.getMaxTableNameLength();
        if (objectName.length() > maxLength) {
            throw new ConnectorException(13012, new Object[]{maxLength});
        }
        try {
            if (!connection.isTable(outputTableName)) {
                throw new ConnectorException(20008);
            }
        } catch (SQLException e) {
            throw new ConnectorException(e.getMessage(), e);
        }
    }

    static {
        TeradataUtils.logger = LogFactory.getLog((Class) TeradataUtils.class);
    }
}
