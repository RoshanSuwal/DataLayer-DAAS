package org.ekbana.bigdata.crud;

import org.apache.log4j.Logger;
import org.ekbana.bigdata.Sanitizer.CheckSql;
import org.ekbana.bigdata.dbmanagement.GetFromProperty;
import org.ekbana.bigdata.sqlparser.QueryBuilder;
import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;

import java.io.IOException;
import java.sql.SQLException;
import java.util.Arrays;

@SuppressWarnings("Duplicates")
public class Insert extends Query implements IQuery{
    public Boolean isValid;
    public String table;
    private String keySpace ;
    private String str = "";
    GetFromProperty gfp = new GetFromProperty();
    private static final Logger logger = Logger.getLogger(Insert.class);

    QueryBuilder queryBuilder;

    public Insert(String query,String dbms,String values) throws IOException {
        this.str = query.toLowerCase();
        //queryBuilder=new QueryBuilder(query,"");
        logger.info("[INSERT] operation");
        switch (dbms) {
            case "cassandra":
                isValid = new CheckSql(query, "insert").isValidSql(); //&& IsValuesAJsonString(values);
                break;
            default:
                logger.error("requested for dbms:" + dbms);
                break;
        }
    }

    public boolean IsValuesAJsonString(String values){
        try {
            new JSONObject(values);
            return true;
        }catch (JSONException e) {
            try {
                new JSONArray(values);
                return true;
            } catch (JSONException f) {
                logger.error(f.getMessage());
                return false;
            }
        }
    }

    @Override
    public String getKeySpace() {
        return keySpace;
    }

    public void setKeySpace(String keySpace) {
        this.keySpace = keySpace;
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

    @Override
    public String getFinalQuerY() throws SQLException {
        return getFinalQuery();
    }

    /**
     * @return query string with alias being replaced by table
     */
    @Override
    public String getFinalQuery() throws SQLException {
        String replacedQuery = "";
        replacedQuery = extractAndReplaceSqlTable();
        logger.info("[replaced query] "+replacedQuery);
        return replacedQuery;
    }

    @Override
    String extractAndReplaceSqlTable() throws SQLException {
        //        INSERT INTO [keyspace_name].[table_name] (column_list)
//        VALUES (column_values)[IF NOT EXISTS]
//        [USING TTL seconds | TIMESTAMP epoch_in_microseconds]

        String[] tokens = this.str.split(" ");//this.queryBuilder.getTokenizedQuery();
       // System.out.println(Arrays.toString(tokens));

        if (tokens[0].equals("INSERT") || tokens[0].equals("insert")) {
            //System.out.println("insert detected");
            for (int i = 1; i < tokens.length; i++) {
                if (!tokens[i].isEmpty()) {
                    if (tokens[i].equals("INTO") || tokens[i].equals("into")) {
                        // System.out.println("INTO detected");
                        for (int j = i + 1; j < tokens.length; j++) {
                            if (!tokens[j].isEmpty()) {
                                if (tokens[j].contains("(")) {
                                    String[] tok = tokens[j].split("\\(");
                                  //  System.out.println("table_name found: " + tok[0]);
                                    tok[0] = replaceTableName(tok[0]);
                                    tokens[j] = String.join("(", tok);
                                } else {
                                   // System.out.println("table_name found: " + tokens[j]);
                                    tokens[j] = replaceTableName(tokens[j]);
                                }
                                break;
                            }
                        }
                        break;
                    } else {
                       // System.out.println("INSERT : invalid syntax " + tokens[i]);
                        logger.error("[INSERT] invalid syntax : expecting 'into' but found "+tokens[i]);
                        return this.str;
                    }
                }
            }
        }

        return String.join(" ", tokens);
    }

    @Override
    String checkQueryEnd() {
        return null;
    }

    private String replaceTableName(String table) throws SQLException {
        String alias = gfp.getAlias(table);

        if (alias.isEmpty()) {
            return table;
        } else {
            String[] tokens = alias.split("\\.");
            this.setTable(tokens[1]);
            this.setKeySpace(tokens[0]);
            return alias;
        }
    }
}
