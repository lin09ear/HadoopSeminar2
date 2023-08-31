package com.bigdata.hadoopseminar2;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
public class DbConnector {

    public static Connection getMysqlConnection(String sourceJdbcUrl, String sourceDbUser, String sourceDbPwd) throws SQLException {
        return DriverManager.getConnection(sourceJdbcUrl, sourceDbUser, sourceDbPwd);
    }

    public static Connection getImpalaConnection(String jdbcUrl, String dbUser, String dbPwd) throws SQLException {
        return DriverManager.getConnection(jdbcUrl, dbUser, dbPwd);
    }
}
