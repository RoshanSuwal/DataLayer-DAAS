package org.ekbana.bigdata.sqlparser;

import org.ekbana.bigdata.dbmanagement.GetFromProperty;

import java.io.IOException;
import java.util.Collection;
import java.util.HashMap;

public class TableNameChanger {

    GetFromProperty gfp=new GetFromProperty();

    public TableNameChanger() throws IOException {
    }

    /**
     * @return HashMap<String,String> where key=table and values=table_alias
     * @param tables is list of table preset in query
     * */
    public HashMap<String, String> mapTableNameToTableAliasName(Collection<String> tables){
        HashMap<String,String> map=new HashMap<>();
        String[] dotSplitting;
        for(String table:tables){
            /***/
            if (table.contains(".")){
                dotSplitting=table.split("\\.");
                map.put(table,dotSplitting[0]+"."+gfp.getAlias(dotSplitting[1]));
            }else {
                map.put(table, gfp.getAlias(table));
            }
        }

        return map;
    }
    /**
     * returns String   String contains query
     * replaces the table_name with table_alias
     * replaces table_name = table_alias as table_name if 'as' not present after table_name
     *
     * @param query actual query separated by SPACE
     * @param map   contains table_name as keys and table_alias as values
     **/
    public String replaceTableNameWithAliasName(String[] query, HashMap<String, String> map) {

        String newQuery = "";

        for (int i = 0; i < query.length; i++) {
            if (map.containsKey(query[i])) {

                newQuery = newQuery + " " + map.get(query[i]);

//                if (i < query.length - 1) {
//                    if (!query[i + 1].equals("as") && !query[i + 1].equals("AS")) {
//                        newQuery = newQuery + " as " + (query[i]);
//                    }
//                } else {
//                    newQuery = newQuery + " as " + getTableNameOnlyFromToken(query[i]);
//                }
            } else if (query[i].contains(".")){
                String[] str=query[i].split("\\.");
                if (map.containsKey(str[0])){
                    newQuery=newQuery+" "+map.get(str[0])+"."+str[1];
                }else{
                    newQuery=newQuery+" "+query[i];
                }
            }
            else{
                newQuery = newQuery + " " + query[i];
            }
        }

        return newQuery;
    }

    private String getTableNameOnlyFromToken(String str){
        if (str.contains(".")){
            return str.split("\\.")[1];
        }
        return str;
    }

    private String[] splitByDot(String str){
        return str.split("\\.");
    }
}

// SELECT DISTINCT  x.Student_Name FROM Course_Taken AS x WHERE NOT EXISTS(SELECT * FROM Course_Required AS y WHERE NOT EXISTS(SELECT * FROM Course_Taken AS z WHERE z.Student_name = x.Student_name AND z.Course = y.Course ))
//SELECT column-name-list FROM table-name1, table-name2 on table-name1.column-name = table-name2.column-name(+);

//SELECT column-name-list FROM table-name1 RIGHT OUTER JOIN table-name2 ON table-name1.column-name = table-name2.column-name;
