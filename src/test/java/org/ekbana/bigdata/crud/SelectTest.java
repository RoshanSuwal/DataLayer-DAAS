package org.ekbana.bigdata.crud;

import org.ekbana.bigdata.Sanitizer.CheckSql;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.util.Arrays;

import static org.junit.jupiter.api.Assertions.assertEquals;

class SelectTest {

    CheckSql checkSql = null;
    Select select = null;
    Insert insert = null;

    @Test
    public void testSelect() throws IOException {
        String inject="'b contains 'drop table''";
        String query = "select * table_alias where a='apple;select * from emp where 1=10'";
        System.out.println(query);
        System.out.println(Arrays.toString(query.split(" ")));
        assertEquals(false, new CheckSql(query, "select").isValidSql());
    }
}