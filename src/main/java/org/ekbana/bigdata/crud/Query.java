package org.ekbana.bigdata.crud;

import java.io.IOException;
import java.sql.SQLException;
import java.util.ArrayList;

/**
 * This class contains methods that performs search and replace operation on
 * a given string. The string is set by setters.
 * Currently methods on this class deals with two types of DBMS dialects <b>SQLOperation</b>
 * and <b>NOSQL</b>.  SQLOperation represents mysql,postgres and oracle and NOSQL represents
 * mongodb.
 *
 * * @author      Saurab Dahal
 */
public abstract class Query {
    private String str = "";


    /**
     * @return query string with alias being replaced by table
     */
    abstract String getFinalQuery() throws SQLException;


    /**
     * This function extracts replaces alias with table name in the original query. Alias and table name
     * are stored as properties in properties file which is read by @see GetFromProperty#getAlias()
     * Difference between this method and @see #replaceMongo() is that this method works explicitly on
     * AQL query.
     *
     * @return String      The alias in original string is replaced by table name.
     */
    abstract String extractAndReplaceSqlTable() throws SQLException;

    /**
     * Checks the query ending arguments like
     * SELECT : limit,allow filtering
     * INSERT: If not exists
     * UPDATE : if exists
     * DELETE : if exists
     * and add it if not present
     * @return String
     */

    abstract String checkQueryEnd();
}
