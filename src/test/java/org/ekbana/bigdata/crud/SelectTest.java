package org.ekbana.bigdata.crud;

import org.ekbana.bigdata.Sanitizer.CheckSql;

import java.io.IOException;
import java.util.Arrays;


class SelectTest {

    CheckSql checkSql = null;
    Select select = null;
    Insert insert = null;

    public void testSelect() throws IOException {
        String inject="'b contains 'drop table''";
        String query = "select * table_alias where 1=apple";
        System.out.println(new CheckSql(query,"select").isValidSql());
        System.out.println(query);
        System.out.println(Arrays.toString(query.split(" ")));
        //assertEquals(false, new CheckSql(query, "select").isValidSql());
    }

    public void prepareQuery(){
        String query="select * from emp where name in $var1,a=$var3";
        String values= "{\"var1\":\"('apple','banana','orange')\", \"var2\":\"12345\"}";
        //String finalQuery=new QueryBuilder().prepareSelectQuery(query,values);

        System.out.println(query);

       // assertEquals(1,1);
    }

}