package org.ekbana.bigdata.crud;

import java.sql.SQLException;

public interface IQuery {
    public String getKeySpace();
    public String getTable();
    public boolean isValid();
    public String getFinalQuerY() throws SQLException;
}
