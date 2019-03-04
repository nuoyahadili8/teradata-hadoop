package com.teradata.connector.teradata.db;

import com.teradata.jdbc.*;
import java.sql.*;
import org.junit.Assert;
import org.junit.Test;

public class TeradataConnectionTest {
    @Test
    public void testUrlParamterParser() throws SQLException, ClassNotFoundException {
        URLParameters params = TeradataConnection.getJDBCURLParameters("jdbc:teradata://10.25.32.113/database=xsun,govern=off");
        Assert.assertFalse(params.isGoverned());
        params = TeradataConnection.getJDBCURLParameters("jdbc:teradata://10.25.32.113/database=xsun,govern=on");
        Assert.assertTrue(params.isGoverned());
        params = TeradataConnection.getJDBCURLParameters("jdbc:teradata://10.25.32.113/database=xsun");
        Assert.assertTrue(params.isGoverned());
        params = TeradataConnection.getJDBCURLParameters("jdbc:teradata://10.25.32.113/");
        Assert.assertTrue(params.isGoverned());
        params = TeradataConnection.getJDBCURLParameters("jdbc:teradata://10.25.32.113");
        Assert.assertTrue(params.isGoverned());
        params = TeradataConnection.getJDBCURLParameters("jdbc:teradata://");
        Assert.assertTrue(params.isGoverned());
    }
}
