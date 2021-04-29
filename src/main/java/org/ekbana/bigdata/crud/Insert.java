package org.ekbana.bigdata.crud;

import org.apache.log4j.Logger;
import org.ekbana.bigdata.Sanitizer.CheckNoSql;
import org.ekbana.bigdata.Sanitizer.CheckSql;
import org.ekbana.bigdata.dbmanagement.GetFromProperty;

import java.io.IOException;
import java.util.ArrayList;

@SuppressWarnings("Duplicates")
public class Insert extends Query {
    public Boolean isValid;
    public String table;
    private String str = "";
    private String query_type = "";
    GetFromProperty gfp = new GetFromProperty();
    private static final Logger logger = Logger.getLogger(Select.class);

    public Insert(String query, String dbms) throws IOException {
        this.str = query.toLowerCase();
        switch (dbms) {
            case "mysql":
            case "postgres":
                this.query_type="postgres";
                isValid=new CheckSql(query,"insert").isValidSql();
                break;
            case "oracle":
            case "cassandra":
            case "phoenix":
                this.query_type = "sql";
                isValid = new CheckSql(query, "insert").isValidSql();
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
                replacedQuery = extractAndReplaceSqlTables();
                break;
            case "mongo":
                replacedQuery = replaceMongo();
                break;
            case "postgres":
                replacedQuery=extractAndReplaceSqlTables();
                break;
            default:
                logger.error("illegal value " + this.query_type + " in get final query");
        }
        return replacedQuery;
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
                if (curToken.equals("into")) {
                    tables.add(strArr[q + 1].replace(")", ""));
                } else if (curToken.equals("join")) {
                    tables.add(strArr[q + 1].replace(")", ""));
                }
            }
        } catch (Exception e) {
            logger.error(e.getLocalizedMessage());
        }
        return tables;
    }

    /**
     * This is function splits query by <tt>white space</tt>
     * and compares each token with the words <code>from</code> and <code>join</code>
     * and adds <tt>token index plus one </tt> key, which in sql is always table, to <code>ArrayList</code>
     *
     * @return null
     */
    String extractAndReplaceSqlTables() {
        logger.info("extracting tables from queries "+this.str);
        String string = this.str;
        String str = this.str;
        try {
            String[] strArr = string.split(" ");
            for (int q = 0; q < strArr.length; q++) {
                String curToken = strArr[q];
                if (curToken.equals("into")) {

                    String[] abs=strArr[q+1].split("\\(");
                    if (abs.length>1){
                        /**If
                         * INSERT INTO TABLE_NAME(parameters) ...
                         * split TABLE_NAME(parameters) using regex="\\("
                         * and the take the 1st element as table_name_alias
                         * */
                        str=this.str.replace("into "+abs[0],"into "+gfp.getAlias(abs[0].toUpperCase()));
                        this.setTable(gfp.getAlias(abs[0].toUpperCase()));
                    }else{
                        str = this.str.replace("into " + strArr[q + 1], "into " + gfp.getAlias(strArr[q + 1].toUpperCase()));
                        this.setTable(gfp.getAlias(strArr[q + 1].toUpperCase()));
                    }
                }
            }
        } catch (Exception e) {
            logger.error(e.getLocalizedMessage());
        }
        return str;
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
        logger.info("replacing alias with table name");
        ArrayList arrayList = extractMongoTables();
        String str = this.str;
        for (Object o : arrayList) {
            str = this.str.replace("db." + o.toString(), "db." + gfp.getAlias(o.toString()));
        }
        return str;
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
        System.out.println(arrayList.toArray());

        String str = this.str;
        for (Object o : arrayList) {
            str = this.str.replace("into " + o.toString(), "into " + gfp.getAlias(o.toString().toUpperCase()));
        }
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
