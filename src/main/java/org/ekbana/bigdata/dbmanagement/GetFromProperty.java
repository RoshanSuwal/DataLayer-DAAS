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
 *
 * @author Saurab Dahal
 */
public class GetFromProperty {
    Properties prop;

    /**
     * Constructor for this class.It uses @see java.io.InputStream to read property file and @see java.util.Properties#load()
     * method to load.
     *
     * @throws IOException
     */
    public GetFromProperty() throws IOException {
        this.prop = new Properties();

        InputStream ins = new FileInputStream("/etc/ApplicationLayer/properties/caching_service.properties");
      //  InputStream ins=new FileInputStream("/home/roshan/workspace/ekbana/bigdata/rest_cache_api/src/main/resources/application.properties");
        this.prop.load(ins);
    }

    /**
     * Loads value for property <code>alias</code>
     *
     * @param alias It is the alias for table in SQLOperation/NOSQL query
     * @return String      value defined for alias
     */
    public String getAlias(String alias) throws SQLException {

        Statement stmt;
        String table_name = "";

        try {
            if (Application.postgresSQLDB_connection==null){
                Application.restartConnection();
            }
            stmt = Application.postgresSQLDB_connection.createStatement();
            String sql = "SELECT name FROM TABLE_ALIAS WHERE alias='" + alias.toUpperCase() + "'";
            ResultSet rs = stmt.executeQuery(sql);

            while (rs.next()) {
                table_name = rs.getString("name");
            }

        } catch (SQLException | IOException e) {
            try {
                Application.restartConnection();
                return getAlias(alias);
            } catch (IOException ioException) {
                System.out.println(ioException);
            }
            System.out.println("Error while getting table alias: " + e.getMessage());
            e.printStackTrace();
        }catch (NullPointerException f){
            throw new SQLException();
        }
        return table_name.toUpperCase();
    }

    public String getPostgresHost() {
        return this.prop.getProperty("postgresdb_host");
    }

    public String getPostgresDB() {
        return this.prop.getProperty("postgresdb_db");
    }

    public String getPostgresDBUser() {
        return this.prop.getProperty("postgresdb_user");
    }

    public String getPostgresDBPass() {
        return this.prop.getProperty("postgresdb_pass");
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
    String getSocketClientPort() {
        return this.prop.getProperty("socket_client_port");
    }

    /**
     * @return String      value defined for property "'token"
     */
    String getToken() {
        return this.prop.getProperty("token");
    }


    /***Error messages**/
    public String getDatabaseConnectionErrorMsg(){return this.prop.getProperty("postgres_connection_error","internal database error");}
    public String getApplayerConnectionErrorMsg(){return this.prop.getProperty("applayer_connection_error","socket connection error");}
    public String getInvalidTableNameErrorMsg(){return this.prop.getProperty("table_name_error","invalid table name");}
    public String getMalformedQueryErrorMsg(){return this.prop.getProperty("query_error","invalid query");}
    public String getEmptyQueryErrorMsg(){return this.prop.getProperty("empty_query_error","empty query");}
    public String getNullValueInsertionError(){return this.prop.getProperty("null_value_insertion_error","no values to insert");}
}


