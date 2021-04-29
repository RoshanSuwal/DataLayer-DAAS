package org.ekbana.bigdata.crud;

import org.ekbana.bigdata.Sanitizer.CheckSql;
import org.junit.jupiter.api.Test;

import java.io.IOException;

import static org.junit.jupiter.api.Assertions.*;

class SelectTest {

    CheckSql checkSql=null;
    Select select=null;
    Insert insert=null;

    @Test
    public void testSelect(){
        String query="SELECT * FROM class FULL OUTER JOIN class_info ON (class.id = class_info.id)";
        checkSql=new CheckSql(query,"select");

        boolean check=checkSql.isValidSql();

        assertEquals(check,true);
    }

    @Test
    public void testSelectQueryWithJoins(){
        String query="SELECT * FROM class FULL OUTER JOIN class_info ON (class.id = class_info.id)";
        try {
            select =new Select(query,"postgres");

            if (select.isValid){
                System.out.println(select.getFinalQuery());
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    @Test
    public void insertTest(){
        String query="INSERT INTO postgres VALUES(1,' ')";
        try {
            insert=new Insert(query,"postgres");
        } catch (IOException e) {
            e.printStackTrace();
        }

        boolean isValid=insert.isValid;

        assertEquals(isValid,true);

    }

    @Test
    public void testSqlTableNames(){
        String sql="select * from table1";
    }

}