package org.ekbana.bigdata.dbmanagement;

import java.io.IOException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;

public class PostgresSQLDBConnection {

    private GetFromProperty gfp;
    final String JDBC_DRIVER = "org.hsqldb.jdbc.JDBCDriver";
    private String DB_URL = "";
    private String USER = "";
    private String PASS = "";

    public PostgresSQLDBConnection() throws IOException {
        gfp = new GetFromProperty();
        this.DB_URL = "jdbc:postgresql://" +
                gfp.getH2Host() +
                "/"+gfp.getHSQLDB();
        this.USER = gfp.getHSQLDBUser();
        this.PASS = gfp.getHSQLDBPass();
    }

    public Connection get_or_create_connection(){
        Connection conn=null;
        Statement stmt=null;

        try {
            // STEP 1: Register JDBC driver
            Class.forName(JDBC_DRIVER);

            // STEP 2: Open a connection
            System.out.println("Connecting to a selected database...");
            conn = DriverManager.getConnection(DB_URL, USER, PASS);

            if (conn != null) {
                System.out.println("Connection created successfully");
                stmt = conn.createStatement();
                stmt.execute("select * from table_alias limit 1");
            } else {
                System.out.println("Problem with creating connection");
            }
        } catch (SQLException e) {
            if (Integer.parseInt(e.getSQLState()) == 42501) {
                String sql = "CREATE TABLE TABLE_ALIAS " +
                        "(table_id INTEGER IDENTITY PRIMARY KEY, name VARCHAR(50), alias  VARCHAR(50))";
                try {
                    stmt.executeUpdate(sql);
                    stmt.close();
                } catch (SQLException e1) {
                    e1.printStackTrace();
                }
                System.out.println("Created table in given database...");
            } else {
                System.out.println(e.getMessage());
            }
        } catch (ClassNotFoundException e) {
            System.out.println(e.getMessage());
        }
        return conn;
    }
}
