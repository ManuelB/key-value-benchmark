package de.apaxo.benchmark.keyvalue;

import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.Map;

import com.mchange.v2.c3p0.ComboPooledDataSource;

public class MySQLKeyValueLookup implements KeyValueLookup {

    private static final String          MYSQL_SERVER                    = "localhost";
    private static final String          MYSQL_PORT                      = "3306";
    private static final String          MYSQL_USER                      = "root";
    private static final String          MYSQL_PASSWORD                  = "";
    private static final String          MYSQL_DATABASE                  = "apaxo_benchmark";
    private static final String          MYSQL_TABLE                     = "apaxo_cache_benchmark";

    private Connection                   connection                      = null;
    private PreparedStatement            preparedLookupStatement         = null;
    private PreparedStatement            preparedDeleteStatement         = null;
    private PreparedStatement            preparedDeleteAllStatement      = null;
    private PreparedStatement            preparedInsertStatement         = null;
    private PreparedStatement            preparedCreateTableStatement    = null;
    private PreparedStatement            preparedCreateDatabaseStatement = null;

    private static ComboPooledDataSource datasource;

    private ResultSet                    resultSet                       = null;
    
    private Map<String,Boolean> debuggingMap = new HashMap<String, Boolean>();

    public void init(Map<String, String> configuration) {
        try {
            Class.forName("com.mysql.jdbc.Driver");
            String connectURI = "jdbc:mysql://" + MYSQL_SERVER + ":"
                    + MYSQL_PORT + "/" + MYSQL_DATABASE + "?"
              + "user=" + MYSQL_USER + "&password=" + MYSQL_PASSWORD;

            datasource = new ComboPooledDataSource();
            datasource.setJdbcUrl(connectURI);
            datasource.setMaxPoolSize(16);
            datasource.setMinPoolSize(4);
            datasource.setInitialPoolSize(4);
            datasource.setNumHelperThreads(6);
            datasource.setTestConnectionOnCheckin(true);
            datasource.setTestConnectionOnCheckout(true);

            connection = datasource.getConnection();

            preparedLookupStatement = connection
                    .prepareStatement("SELECT value FROM " + MYSQL_TABLE
                            + " WHERE `key` = ?");
            preparedDeleteStatement = connection
                    .prepareStatement("DELETE FROM " + MYSQL_TABLE
                            + " WHERE `key` = ?");
            preparedDeleteAllStatement = connection
                    .prepareStatement("DELETE FROM " + MYSQL_TABLE);
            preparedInsertStatement = connection
                    .prepareStatement("INSERT INTO " + MYSQL_TABLE
                            + " (`key`, value) VALUES (?, ?)");
            preparedCreateTableStatement = connection
                    .prepareStatement("CREATE TABLE "
                            + MYSQL_TABLE
                            + " (`key` VARCHAR(255) NOT NULL, value BLOB NOT NULL, PRIMARY KEY (`key`))"
                            + "ENGINE=InnoDB DEFAULT CHARSET=utf8 COLLATE=utf8_unicode_ci");

            preparedCreateDatabaseStatement = connection
                    .prepareStatement("CREATE DATABASE IF NOT EXISTS "
                            + MYSQL_DATABASE);

            // Check if database exist, on fail, create it
            //
            preparedCreateDatabaseStatement.executeUpdate();

            // Check if table exist, on fail, create it
            //

            DatabaseMetaData dbm = connection.getMetaData();

            resultSet = dbm.getTables(null, null, MYSQL_TABLE, null);
            if (!resultSet.next()) {
                preparedCreateTableStatement.executeUpdate();
            }

        } catch (Exception e) {
            throw new RuntimeException(e);
        }

    }

    public void cleanup() {

        try {
            preparedDeleteAllStatement.executeUpdate();
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }

        try {
            if (connection != null) {
                connection.close();
            }
            if (datasource != null) {
                datasource.close();
            }
            if (resultSet != null) {
                resultSet.close();
            }
            if (preparedCreateTableStatement != null) {
                preparedCreateTableStatement.close();
            }
            if (preparedCreateDatabaseStatement != null) {
                preparedCreateDatabaseStatement.close();
            }
            if (preparedInsertStatement != null) {
                preparedInsertStatement.close();
            }
            if (preparedLookupStatement != null) {
                preparedLookupStatement.close();
            }
            if (preparedDeleteStatement != null) {
                preparedDeleteStatement.close();
            }
            if (preparedDeleteAllStatement != null) {
                preparedDeleteAllStatement.close();
            }

        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }

    public Object lookup(String key) {
        Object result = null;
        try {
            preparedLookupStatement.setString(1, key);
            resultSet = preparedLookupStatement.executeQuery();
            result = resultSet.next() ? resultSet.getString(1) : null;
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }

        return result;
    }

    public void remove(String key) {
        try {
            preparedDeleteStatement.setString(1, key);
            preparedDeleteStatement.executeUpdate();
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }

    public void insert(String key, Object o) {
        try {
            synchronized(this) {
                preparedInsertStatement.setString(1, key);
                preparedInsertStatement.setString(2, o.toString());
                preparedInsertStatement.executeUpdate();
            }
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }

    public String toString() {
        return "MySQLKeyValueLookup";
    }

}
