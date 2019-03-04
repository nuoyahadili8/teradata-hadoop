package com.teradata.connector.sample.plugin.utils;

import com.teradata.connector.common.utils.ConnectorStringUtils;

import java.sql.Connection;
import java.sql.SQLException;


/**
 * @author Administrator
 */
public class CommonDBUtils {
    public static String getQuotedEscapedName(final String... nameparts) {
        final StringBuilder builder = new StringBuilder();
        for (final String namepart : nameparts) {
            if (!ConnectorStringUtils.isEmpty(namepart)) {
                builder.append('\"').append(getEscapedName(namepart)).append("\".");
            }
        }
        if (builder.length() > 0) {
            builder.setLength(builder.length() - 1);
        }
        return builder.toString();
    }

    public static String getEscapedName(final String name) {
        if (name == null) {
            return "";
        }
        return name.replace("\"", "\"\"");
    }

    public static void CloseConnection(final Connection sqlConnection) {
        try {
            if (sqlConnection != null && !sqlConnection.isClosed()) {
                sqlConnection.close();
            }
        } catch (SQLException e1) {
            e1.printStackTrace();
        }
    }
}
