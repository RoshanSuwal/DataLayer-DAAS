package org.ekbana.bigdata.crud;

import org.apache.log4j.Logger;
import org.ekbana.bigdata.Sanitizer.CheckNoSql;
import org.ekbana.bigdata.Sanitizer.CheckSql;
import org.ekbana.bigdata.dbmanagement.GetFromProperty;
import org.ekbana.bigdata.sqlparser.TableNameChanger;
import org.ekbana.bigdata.sqlparser.TableNameParser;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;

@SuppressWarnings("Duplicates")
public class Select extends Query {

    public Boolean isValid;
    private static final Logger logger = Logger.getLogger(Select.class);
    private String str = "";
    private String query_type = "";
    private String table = "";
    GetFromProperty gfp = new GetFromProperty();

    TableNameParser tableNameParser;
    TableNameChanger tableNameChanger=new TableNameChanger();


    public Select(String query, String dbms) throws IOException {
//        this.str = query.toLowerCase();
        this.str = query;
        switch (dbms) {
            case "mysql":
            case "postgres":
                this.query_type="postgres";
                isValid=new CheckSql(query,"select").isValidSql();
                break;
            case "oracle":
            case "phoenix":
            case "cassandra":
                this.query_type = "sql";
                isValid = new CheckSql(query, "select").isValidSql();
                break;
            case "mongo":
                this.query_type = "mongo";
                isValid = new CheckNoSql(query).isValidMongoSql();
                break;
            default:
                logger.error("requested for dbms:" + dbms);
                break;
        }


        this.tableNameParser=new TableNameParser(query);
    }

    /**
     * @return query string with alias being replaced by table
     */
    @Override
    public String getFinalQuery() {
        String replacedQuery = "";
        switch (this.query_type) {
            case "sql":
                if (this.str.contains("limit ") || this.str.contains("LIMIT")) {
                    System.out.println("replaced");
                    replacedQuery = replaceSql();
                } else {
                    System.out.println("append");
                    replacedQuery = replaceSql();
                }
                break;
            case "mongo":
                replacedQuery = replaceMongo();
                break;

            case "postgres":
                replacedQuery=replaceSql();
            default:
                logger.error("illegal value " + this.query_type + " in get final query");
        }
        logger.info("replaced query is " + replacedQuery);
        System.out.println("replaced = " + replacedQuery);
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
                if (curToken.equals("from") || curToken.equals("FROM")) {
                    tables.add(strArr[q + 1].replace(")", ""));
                    this.setTable(gfp.getAlias(strArr[q + 1].toUpperCase()));
                } else if (curToken.equals("join") || curToken.equals("JOIN")) {
                    tables.add(strArr[q + 1].replace(")", ""));
                }
            }
        } catch (Exception e) {
            logger.error(e.getLocalizedMessage());
        }
        System.out.println("TABLES from SQL :" + tables);
        return tables;
    }

    void setTable(String table) {
        this.table = table;
    }

    public String getTable() {
        return this.table;
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
            str = this.str.replace("db." + o.toString(), "db." + gfp.getAlias(o.toString().toUpperCase()));
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

        /**
        ArrayList arrayList = extractSqlTables();

        System.out.println(Arrays.toString(new ArrayList[]{arrayList}));
        String str = this.str;
        for (Object o : arrayList) {
            String tableName = gfp.getAlias(o.toString().toUpperCase());
            if (this.str.contains("from") || this.str.contains("join")) {
                str = str.replace("from " + o.toString(), "from " + tableName)
                        .replace("join " + o.toString(), "join " + tableName);
            }
            if (this.str.contains("FROM") || this.str.contains("JOIN")){
                str = str.replace("FROM " + o.toString(), "FROM " + tableName)
                        .replace("JOIN " + o.toString(), "JOIN " + tableName);
            }

            System.out.println(o+":"+str+":"+tableName);
        }

         **/


        HashMap<String, String> map = tableNameChanger.mapTableNameToTableAliasName(this.tableNameParser.tables());
        this.setTable((this.tableNameParser.tables().toString()));
        return tableNameChanger.replaceTableNameWithAliasName(tableNameParser.getfilteredQueryAsTokens(),map);

    }

    /**
     * Replaces limit value greater than 10000 ( Ten Thousand ) to 10000
     *
     * @param str1 Query string in which alias name is replaced by table name
     * @return String which has limit value above 10000 is changed.
     */
    @Override
    String replaceLimit(String str1) {
        String str = "";
        String[] strArr = str1.split(" ");
        int size;
        for (int i = 0; i < strArr.length; i++) {
            String currToken = strArr[i];
            if (currToken.equals("limit") || currToken.equals("LIMIT")) {
                try {
                    if (i + 1 <= strArr.length) {
                        size = Integer.parseInt(strArr[i + 1]);
                        if (size > 10000) {
                            str = str1.replace("limit " + strArr[i + 1], "limit " + String.valueOf(10000));
                            str = str1.replace("LIMIT " + strArr[i + 1], "LIMIT " + String.valueOf(10000));
                        } else {
                            str = str1;
                        }
                    }
                } catch (NumberFormatException e) {
                    logger.error(" user sent non-numeric character in query");
                }
            }
        }
        System.out.println("replaced limit = " + str);
        return str;
    }

    @Override
    String appendLimit(String str1) {
        String s = "";
        s = str1 + " limit 10000";
        s = str1 + " LIMIT 10000";
        return s;
    }
}
