package org.ekbana.bigdata.crud;

import org.apache.log4j.Logger;
import org.ekbana.bigdata.Sanitizer.CheckSql;
import org.ekbana.bigdata.dbmanagement.GetFromProperty;
import org.ekbana.bigdata.sqlparser.QueryParser;

import java.io.IOException;

@SuppressWarnings("Duplicates")
public class Delete extends Query {

    public Boolean isValid;
    private String str = "";
    private String table="";
    private String keyspace="";
    GetFromProperty gfp = new GetFromProperty();
    private static final Logger logger = Logger.getLogger(Select.class);


    public Delete(String query, String dbms) throws IOException {
        this.str = query;
        switch (dbms) {
            case "cassandra":
                isValid=new CheckSql(this.str,"delete").isValidSql();
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

        logger.info("replaced query is " + replacedQuery);
        System.out.println("replaced = " + replacedQuery);
        return replacedQuery;
    }

    @Override
    String extractAndReplaceSqlTable() {
//        DELETE [column_name (term)][, ...]
//        FROM [keyspace_name.] table_name
//                [USING TIMESTAMP timestamp_value]
//        WHERE PK_column_conditions
//        [IF EXISTS | IF static_column_conditions]

        String[] tokens=new QueryParser().getTokenizedQuery(this.str);

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

    private String replaceTableName(String table) {
        String alias;
        if (table.contains(".")) {
            String[] tokens = table.split("\\.");
            alias = gfp.getAlias(tokens[1]);
            table = table.replace("." + tokens[1], "." + alias);
            this.setTable(alias);
            this.setKeyspace(tokens[0]);
        } else {
            table = gfp.getAlias(table);
            this.setTable(table);
        }
        return table;
    }


    public String getTable() {
        return table;
    }

    public void setTable(String table) {
        this.table = table;
    }

    public String getKeyspace() {
        return keyspace;
    }

    public void setKeyspace(String keyspace) {
        this.keyspace = keyspace;
    }

}
