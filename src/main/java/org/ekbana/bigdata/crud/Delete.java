package org.ekbana.bigdata.crud;

import org.apache.log4j.Logger;
import org.ekbana.bigdata.Sanitizer.CheckSql;
import org.ekbana.bigdata.dbmanagement.GetFromProperty;
import org.ekbana.bigdata.sqlparser.QueryBuilder;

import java.io.IOException;
import java.sql.SQLException;

@SuppressWarnings("Duplicates")
public class Delete extends Query implements IQuery {

    public Boolean isValid;
    private String str = "";
    private String table="";
    private String keyspace="";
    GetFromProperty gfp = new GetFromProperty();
    private static final Logger logger = Logger.getLogger(Delete.class);

    QueryBuilder queryBuilder;


    public Delete(String query, String dbms,String values) throws IOException {
        this.str = query;
        //this.queryBuilder=new QueryBuilder(query,values);
        logger.info("[DELETE] operation");
        switch (dbms) {
            case "cassandra":
                isValid=new CheckSql(this.str,"delete").isValidSql(); //&& queryBuilder.isIsvalid();
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
    public String getFinalQuery() throws SQLException {
        String replacedQuery = "";

        replacedQuery=extractAndReplaceSqlTable();
        logger.info("[replaced query] "+replacedQuery);
        return replacedQuery;
    }

    @Override
    String extractAndReplaceSqlTable() throws SQLException {
//        DELETE [column_name (term)][, ...]
//        FROM [keyspace_name.] table_name
//                [USING TIMESTAMP timestamp_value]
//        WHERE PK_column_conditions
//        [IF EXISTS | IF static_column_conditions]

        String[] tokens=this.str.split(" ");//queryBuilder.getTokenizedQuery();

        if (tokens[0].toUpperCase().equals("DELETE")){
            for (int i=1;i<tokens.length;i++){
                if ((tokens[i].toUpperCase()).equals("FROM")){
                    for (int j=i+1;j< tokens.length;j++){
                        if (!tokens[j].isEmpty()){
                            tokens[j]=replaceTableName(tokens[j]);
                            break;
                        }
                    }
                    break;
                }
            }
        }else {
            logger.error("Invalid query");
        }
        return String.join(" ",tokens);
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
            this.setKeyspace(tokens[0]);
            return alias;
        }
    }


    public String getTable() {
        return table;
    }

    @Override
    public boolean isValid() {
        return isValid;
    }

    @Override
    public String getFinalQuerY() throws SQLException {
        return getFinalQuery();
    }

    public void setTable(String table) {
        this.table = table;
    }

    @Override
    public String getKeySpace() {
        return keyspace;
    }

    public void setKeyspace(String keyspace) {
        this.keyspace = keyspace;
    }

}
