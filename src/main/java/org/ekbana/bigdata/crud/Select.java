package org.ekbana.bigdata.crud;

import org.apache.log4j.Logger;
import org.ekbana.bigdata.Sanitizer.CheckSql;
import org.ekbana.bigdata.dbmanagement.GetFromProperty;
import org.ekbana.bigdata.sqlparser.QueryBuilder;

import java.io.IOException;
import java.sql.SQLException;
import java.util.Arrays;
import java.util.Locale;

@SuppressWarnings("Duplicates")
public class Select extends Query implements IQuery {

    public Boolean isValid;
    private static final Logger logger = Logger.getLogger(Select.class);
    private String str = "";
    private String query_type = "";
    private String table = "";

    private String keyspace = "";
    GetFromProperty gfp = new GetFromProperty();
    QueryBuilder queryBuilder;

    public Select(String query, String dbms, String values) throws IOException {
        this.str = query;
        logger.info("[SELECT] operation");
        // this.queryBuilder = new QueryBuilder(query, values);
        if (dbms.equals("cassandra")) {
            this.query_type = "sql";
            isValid = new CheckSql(query, "select").isValidSql();// && this.queryBuilder.isIsvalid();
        } else {
            logger.error("requested for dbms:" + dbms);
        }

    }

    /**
     * @return query string with alias being replaced by table
     */

    @Override
    public String getFinalQuery() throws SQLException {
        String replacedQuery = "";

        //System.out.println(isValid);

        replacedQuery = extractAndReplaceSqlTable();
        logger.info("[replaced query] " + replacedQuery);
        return replacedQuery;
    }

    @Override
    public String getFinalQuerY() throws SQLException {
        return getFinalQuery();
    }

    @Override
    public String extractAndReplaceSqlTable() throws SQLException {

        //pattern= SELECT [] FROM [KEYSPACE].[TABLE_NAME]

        String[] tokens = this.str.split(" ");//this.queryBuilder.getTokenizedQuery();

        //System.out.println(Arrays.toString(tokens));

        for (int h = 0; h < tokens.length; h++) {
            if (tokens[h].equals("SELECT") || tokens[h].equals("select")) {
                for (int i = h + 1; i < tokens.length; i++) {
                    if (tokens[i].equals("FROM") || tokens[i].equals("from")) {
                        for (int j = i + 1; j < tokens.length; j++) {
                            if (!tokens[j].isEmpty()) {
                                tokens[j] = replaceTableName(tokens[j]);
                            }
                            break;
                        }
                        break;
                    }
                }
            }
        }

        if (this.str.toLowerCase().contains("allow filtering") ){
            return String.join(" ",tokens);
        }else {
            return String.join(" ", tokens)+" ALLOW FILTERING";
        }
    }

    @Override
    public String checkQueryEnd() {
        return null;
    }


    void setTable(String table) {
        this.table = table;
    }

    public String getTable() {
        return this.table;
    }

    @Override
    public boolean isValid() {
        return isValid;
    }

    public String getKeySpace() {
        return keyspace;
    }

    public void setKeyspace(String keyspace) {
        this.keyspace = keyspace;
    }

    /**
     * Replaces limit value greater than 10000 ( Ten Thousand ) to 10000
     *
     * @param str1 Query string in which alias name is replaced by table name
     * @return String which has limit value above 10000 is changed.
     */

    String replaceOrAppendLimit(String str1) {
        String str = "";
        String[] strArr = str1.split(" ");

        //getting last four words separated by space -> [...limit [value] ALLOW FILTERING]

        int count = 4;
        int size;
        String recent = "";

        for (int i = strArr.length - 1; i >= 0; i--) {

            if (!strArr[i].isBlank()) {
                count--;
                if (strArr[i].equals("limit") || strArr[i].equals("LIMIT")) {
                    String[] string = Arrays.copyOfRange(strArr, 0, i);

                    str = String.join(" ", string);

                    try {
                        if (count < 3) {
                            size = Integer.parseInt(recent);
                            //System.out.println(strArr[i + 1] + " : " + size);
                            str = str + " LIMIT " + Math.min(size, 10000) + " ALLOW FILTERING";
                        } else {
                            str = str + " LIMIT " + 10000 + " ALLOW FILTERING";
                        }
                    } catch (NumberFormatException e) {
                        str = str1;
                        logger.error(" user sent non-numeric character in query");
                    }
                    break;
                }
                if (count < 1) {
                    str = str1 + " LIMIT " + 10000 + " ALLOW FILTERING";
                    break;
                }
                recent = strArr[i];
            }
        }
        System.out.println("replaced limit = " + str);
        return str;
    }

    /**
     * This function replaces table with valid table_name
     * extracts the keyspace and alias splitting with (.)
     * replaces alias with table_name obtained from properties @See GetFromProperty for more
     * sets the table_name and keyspace
     *
     * @param table String is the table obtained from query
     **/

    private String replaceTableName(String table) throws SQLException {
        String alias = "";
        alias = gfp.getAlias(table);

        if (alias.isEmpty()) {
            return table;
        } else {
            String[] tokens = alias.split("\\.");
            this.setTable(tokens[1]);
            this.setKeyspace(tokens[0]);
            return alias;
        }
    }
}
