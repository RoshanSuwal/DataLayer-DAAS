package org.ekbana.bigdata.dbmanagement;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.ApplicationArguments;

import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.List;
import java.util.Properties;


/**
 * GetFromPropery class loads property file and defined methods to provide property to
 * whole system.
 * @author      Saurab Dahal
 */
public class GetFromProperty {
    Properties prop;
    Connection hsqldb_connection;
    Connection postgresSQLDB_connection;

    /**
     * Constructor for this class.It uses @see java.io.InputStream to read property file and @see java.util.Properties#load()
     * method to load.
     *
     * @throws IOException
     */
    public GetFromProperty() throws IOException {
        this.prop = new Properties();
        this.hsqldb_connection = Application.hsqldb_connection;
        this.postgresSQLDB_connection=Application.postgresSQLDB_connection;

        InputStream ins = new FileInputStream("/etc/ApplicationLayer/properties/caching_service.properties");
        this.prop.load(ins);
    }

    /**
     * Loads value for property <code>alias</code>
     *
     * @param alias It is the alias for table in SQLOperation/NOSQL query
     * @return String      value defined for alias
     */
    public String getAlias(String alias) {

        Statement stmt;
        String table_name = "";

        try {

            stmt=postgresSQLDB_connection.createStatement();
            String sql="SELECT name FROM TABLE_ALIAS WHERE alias='"+alias.toUpperCase()+"'";
            ResultSet rs=stmt.executeQuery(sql);

            while (rs.next()){
                table_name=rs.getString("name");
            }

        } catch (SQLException e) {
            System.out.println("Error while getting table alias: " + e.getMessage());
            e.printStackTrace();
        }
        return table_name;
    }

    public String getH2Host(){
        return this.prop.getProperty("hsqldb_host");
    }

    public String getHSQLDB(){
        return this.prop.getProperty("hsqldb_db");
    }

    public String getHSQLDBUser(){
        return this.prop.getProperty("hsqldb_user");
    }

    public String getHSQLDBPass(){
        return this.prop.getProperty("hsqldb_pass");
    }

    /**
     * @return String      value defined for property "'memcacheServer1"
     */
    String getMemcacheServer1() {
        return this.prop.getProperty("memcacheServer1");
    }

    /**
     * @return String      value defined for property "'memcacheServer2"
     */
    String getMemcacheServer2() {
        return this.prop.getProperty("memcacheServer2");
    }

    /**
     * @return String      value defined for property "'memcacheServer1_port"
     */
    String getMemcacheServer1Port() {
        return this.prop.getProperty("memcacheServer1_port");
    }

    /**
     * @return String      value defined for property "'memcacheServer2_port"
     */
    String getMemcacheServer2Port() {
        return this.prop.getProperty("memcacheServer2_port");
    }

    /**
     * @return String      value defined for property "'socket_client_ip"
     */
    String getSocketClientIP() {
        return this.prop.getProperty("socket_client_ip");
    }

    String getENC_KEY() {
        return this.prop.getProperty("enc_key");
    }

    /**
     * @return String      value defined for property "'socket_client_port"
     */
    String getSocketClientPort() {   return this.prop.getProperty("socket_client_port"); }

    /**
     * @return       String      value defined for property "'token"
     */
    String getToken() {
        return this.prop.getProperty("token");
    }


}


