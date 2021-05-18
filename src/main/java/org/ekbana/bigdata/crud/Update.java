package org.ekbana.bigdata.crud;

import org.apache.log4j.Logger;
import org.ekbana.bigdata.Sanitizer.CheckSql;
import org.ekbana.bigdata.dbmanagement.GetFromProperty;
import org.ekbana.bigdata.sqlparser.QueryBuilder;

import java.io.IOException;

@SuppressWarnings("Duplicates")
public class Update extends Query {

    public Boolean isValid;
    private String str = "";
    public String table;
    public String keyspace;
    private String query_type = "";
    GetFromProperty gfp = new GetFromProperty();
    private static final Logger logger = Logger.getLogger(Update.class);

    QueryBuilder queryBuilder;

    public Update(String query, String dbms,String values) throws IOException {
        this.str = query;
       // queryBuilder=new QueryBuilder(query,values);
        logger.info("[UPDATE] operation");
        switch (dbms) {
            case "cassandra":
                this.query_type = "cassandra";
                isValid = new CheckSql(query, "update").isValidSql() ;//&& queryBuilder.isIsvalid();
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
        replacedQuery=extractAndReplaceSqlTable();
        logger.info("[replaced query] "+replacedQuery);
        return replacedQuery;
    }

    @Override
    String extractAndReplaceSqlTable() {
        //pattern UPDATE [KEYSPACE].[TABLE-NAME]
        String[] tokens=this.str.split(" ");//queryBuilder.getTokenizedQuery();

        if (tokens[0].equals("UPDATE") || tokens[0].equals("update")){
            for (int i=1;i< tokens.length;i++){
                if (!tokens[i].isEmpty()){
                   // System.out.println("UPDATE: found  table-name : "+tokens[i]);
                    tokens[i]=replaceTableName(tokens[i]);
                    break;
                }
            }
        }
        return String.join(" ",tokens);
    }

    @Override
    String checkQueryEnd() {
        return null;
    }

    void setTable(String table) {
        this.table = table;
    }

    public String getTable() {
        return this.table;
    }

    public String getKeyspace() {
        return keyspace;
    }

    public void setKeyspace(String keyspace) {
        this.keyspace = keyspace;
    }


    private String replaceTableName(String table) {
        String alias = gfp.getAlias(table);

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
