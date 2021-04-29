package org.ekbana.bigdata.crud;

import org.apache.log4j.Logger;
import org.ekbana.bigdata.Sanitizer.CheckNoSql;
import org.ekbana.bigdata.Sanitizer.CheckSql;
import org.ekbana.bigdata.dbmanagement.GetFromProperty;

import java.io.IOException;
import java.lang.reflect.Array;
import java.util.ArrayList;
import java.util.Arrays;

@SuppressWarnings("Duplicates")
public class Update extends Query {

    public Boolean isValid;
    private String str = "";
    public String table;
    private String query_type = "";
    GetFromProperty gfp = new GetFromProperty();
    private static final Logger logger = Logger.getLogger(Select.class);

    public Update(String query, String dbms) throws IOException {
        this.str = query;
        switch (dbms) {
            case "mysql":
            case "postgres":
                this.query_type = "postgres";
                isValid = new CheckSql(query, "update").isValidSql();
                break;
            case "oracle":
            case "phoenix":
                this.query_type = "sql";
                isValid = new CheckSql(query, "update").isValidSql();
                break;
            case "mongo":
                this.query_type = "mongo";
                isValid = new CheckNoSql(query).isValidMongoSql();
                break;
            default:
                logger.error("requested for dbms:" + dbms);
                break;
        }
    }

    /**
     * @return query string with alias being replaced by table
     */
    @Override
    public String getFinalQuery() {
        String replacedQuery = "";
        switch (this.query_type) {
            case "sql":
                replacedQuery = replaceSql();
                break;
            case "mongo":
                replacedQuery = replaceMongo();
                break;
            case "postgres":
                replacedQuery = replaceSql();
                break;
            default:
                logger.error("illegal value " + this.query_type + " in get final query");
        }
        return replacedQuery;
    }

    String extractAndReplaceSqlTables() {
        logger.info("extracting tables from queries and replacing in SQL tables");
        String string = this.str;
        String str = "";
        try {
            String[] strArr = string.split(" ");
            for (int q = 0; q < strArr.length; q++) {
                String currToken = strArr[q];
                if (currToken.equals("update")) {
                    str = this.str.replace("update " + strArr[q + 1], "update " + gfp.getAlias(strArr[q + 1].toUpperCase()));
                    this.setTable(gfp.getAlias(strArr[q + 1].toUpperCase()));
                } else if (currToken.equals("UPDATE")) {
                    str = this.str.replace("UPDATE " + strArr[q + 1], "UPDATE " + gfp.getAlias(strArr[q + 1].toUpperCase()));
                    this.setTable(gfp.getAlias(strArr[q + 1].toUpperCase()));
                }

            }
        } catch (Exception e) {
            logger.error(e.getLocalizedMessage());
        }

        System.out.println(str);
        return str;
    }

    /**
     * This is function splits query by <tt>white space</tt>
     * and compares each token with the words <code>from</code> and <code>join</code>
     * and adds <tt>token index plus one </tt> key, which in sql is always table, to <code>ArrayList</code>
     *
     * @return <code>ArrayList</code>   Containing list of tables
     */
    @Override
    ArrayList extractSqlTables() {
        logger.info("extracting tables from queries");
        ArrayList<String> tables = new ArrayList<>();
        String string = this.str;
        try {
            String[] strArr = string.split(" ");
            for (int q = 0; q < strArr.length; q++) {
                String curToken = strArr[q];
                if (curToken.equals("update") || curToken.equals("UPDATE")) {
                    tables.add(strArr[q + 1].replace(")", ""));
                } else if (curToken.equals("join")) {
                    tables.add(strArr[q + 1].replace(")", ""));
                } else if (curToken.equals("from") || curToken.equals("FROM")) {
                    tables.add(strArr[q + 1].replace(")", ""));
                }
            }
        } catch (Exception e) {
            logger.error(e.getLocalizedMessage());
        }
        return tables;
    }

    /**
     * This is function splits query by <tt>dot ( . )</tt>
     * and compares each token with the words <code>db</code>
     * and adds <tt>token index plus one </tt> key, which in mongo helper is always collection, to <code>ArrayList</code>
     *
     * @return <code>ArrayList</code>   Containing list of collections
     */
    @Override
    ArrayList extractMongoTables() {
        logger.info("extracting tables from queries");
        ArrayList<String> tables = new ArrayList<>();
        String string = this.str;
        try {
            String[] strArr = string.split("\\.");
            for (int q = 0; q < strArr.length; q++) {
                String curToken = strArr[q];
                if (curToken.equals("db")) {
                    tables.add(strArr[q + 1].replace(")", ""));
                }
            }
        } catch (Exception e) {
            logger.error(e.getLocalizedMessage());
        }
        return tables;
    }

    /**
     * This function replaces alias with table name in the original query. Alias and table name
     * are stored as properties in properties file which is read by @see GetFromProperty#getAlias().
     * Difference between this method and @see #replaceSql() is that this method works explicitly on
     * Mongo helper query.
     *
     * @return String      The alias in original string is replaced by table name.
     */
    @Override
    String replaceMongo() {
        return null;
    }

    /**
     * This function replaces alias with table name in the original query. Alias and table name
     * are stored as properties in properties file which is read by @see GetFromProperty#getAlias()
     * Difference between this method and @see #replaceMongo() is that this method works explicitly on
     * AQL query.
     *
     * @return String      The alias in original string is replaced by table name.
     */
    @Override
    String replaceSql() {
        logger.info("replacing alias with table name");
        ArrayList arrayList = extractSqlTables();

        String str = this.str;
        for (Object o : arrayList) {
            if (this.str.contains("update")) {
                str = str.replace("update " + o.toString(), "update " + gfp.getAlias(o.toString()));
            } else if (this.str.contains("UPDATE")) {
                str = str.replace("UPDATE " + o.toString(), "UPDATE " + gfp.getAlias(o.toString()));
            }

            if (this.str.contains("from")){
                str=str.replace("from "+o.toString(),"from "+gfp.getAlias(o.toString()));
            }else if (this.str.contains("FROM")){
                str=str.replace("FROM "+o.toString(),"FROM "+gfp.getAlias(o.toString()));
            }

            setTable(gfp.getAlias(o.toString()));
        }

        System.out.println(str);
        return str;
    }

    /**
     * Replaces limit value greater than 10000 ( Ten Thousand ) to 10000
     *
     * @param str1 Query string in which alias name is replaced by table name
     * @return String which has limit value above 10000 is changed.
     */
    @Override
    String replaceLimit(String str1) {
        return null;
    }

    @Override
    String appendLimit(String str1) {
        return null;
    }

    void setTable(String table) {
        this.table = table;
    }

    public String getTable() {
        return this.table;
    }
}
