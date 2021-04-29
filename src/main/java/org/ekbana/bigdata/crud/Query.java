package org.ekbana.bigdata.crud;

import java.io.IOException;
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

    public Query(String query) throws IOException {
    }

    public Query() throws IOException {
    }

    /**
     * @return query string with alias being replaced by table
     */
    abstract String getFinalQuery();

    /**
     * This is function splits query by <tt>white space</tt>
     * and compares each token with the words <code>from</code> and <code>join</code>
     * and adds <tt>token index plus one </tt> key, which in sql is always table, to <code>ArrayList</code>
     *
     * @return <code>ArrayList</code>   Containing list of tables
     */
    abstract ArrayList extractSqlTables();

    /**
     * This is function splits query by <tt>dot ( . )</tt>
     * and compares each token with the words <code>db</code>
     * and adds <tt>token index plus one </tt> key, which in mongo helper is always collection, to <code>ArrayList</code>
     *
     * @return <code>ArrayList</code>   Containing list of collections
     */
    abstract ArrayList extractMongoTables();

    /**
     * This function replaces alias with table name in the original query. Alias and table name
     * are stored as properties in properties file which is read by @see GetFromProperty#getAlias().
     * Difference between this method and @see #replaceSql() is that this method works explicitly on
     * Mongo helper query.
     *
     * @return String      The alias in original string is replaced by table name.
     */
    abstract String replaceMongo();

    /**
     * This function replaces alias with table name in the original query. Alias and table name
     * are stored as properties in properties file which is read by @see GetFromProperty#getAlias()
     * Difference between this method and @see #replaceMongo() is that this method works explicitly on
     * AQL query.
     *
     * @return String      The alias in original string is replaced by table name.
     */
    abstract String replaceSql();

    /**
     * Replaces limit value greater than 10000 ( Ten Thousand ) to 10000
     *
     * @param str1 Query string in which alias name is replaced by table name
     * @return String which has limit value above 10000 is changed.
     */
    abstract String replaceLimit(String str1);

    abstract String appendLimit(String str1);
}
