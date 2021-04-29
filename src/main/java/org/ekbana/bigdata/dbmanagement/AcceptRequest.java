package org.ekbana.bigdata.dbmanagement;


import org.codehaus.commons.nullanalysis.NotNull;

/**
 * The type Accept request.
 */
public class AcceptRequest {
    private String query;
    private String dbms;
    private String db;
    private String values;

    @NotNull
    private String username;
    @NotNull
    private String password;
    private String offset_key;
    private String session_id;
    private int offset;

    /**
     * Gets db.
     *
     * @return Value of db.
     */
    public String getDb() {
        return db;
    }

    /**
     * Sets new dbms.
     *
     * @param dbms New value of dbms.
     */
    public void setDbms(String dbms) {
        this.dbms = dbms;
    }

    /**
     * Gets values.
     *
     * @return Value of values.
     */
    public String getValues() {
        return values;
    }

    /**
     * Sets new db.
     *
     * @param db New value of db.
     */
    public void setDb(String db) {
        this.db = db;
    }

    /**
     * Gets query.
     *
     * @return Value of query.
     */
    public String getQuery() {
        return query;
    }

    /**
     * Gets dbms.
     *
     * @return Value of dbms.
     */
    public String getDbms() {
        return dbms;
    }

    /**
     * Gets username.
     *
     * @return Value of username.
     */
    public String getUsername() {
        return username;
    }

    /**
     * Sets new values.
     *
     * @param values New value of values.
     */
    public void setValues(String values) {
        this.values = values;
    }

    /**
     * Sets new query.
     *
     * @param query New value of query.
     */
    public void setQuery(String query) {
        this.query = query;
    }

    /**
     * Sets new password.
     *
     * @param password New value of password.
     */
    public void setPassword(String password) {
        this.password = password;
    }

    /**
     * Gets password.
     *
     * @return Value of password.
     */
    public String getPassword() {
        return password;
    }

    /**
     * Sets new username.
     *
     * @param username New value of username.
     */
    public void setUsername(String username) {
        this.username = username;
    }

    /**
     * Sets new offset_key.
     *
     * @param offset_key New value of offset_key.
     */
    public void setOffset_key(String offset_key) {
        this.offset_key = offset_key;
    }

    /**
     * Gets offset_key.
     *
     * @return Value of offset_key.
     */
    public String getOffset_key() {
        return offset_key;
    }


    /**
     * Gets offset.
     *
     * @return Value of offset.
     */
    public int getOffset() {
        return offset;
    }

    /**
     * Sets new offset.
     *
     * @param offset New value of offset.
     */
    public void setOffset(int offset) {
        this.offset = offset;
    }

    public String getSession_id() {
        return session_id;
    }

    public void setSession_id(String session_id) {
        this.session_id = session_id;
    }

    @Override
    public String toString() {
        return "AcceptRequest{" +
                "query='" + query + '\'' +
                ", dbms='" + dbms + '\'' +
                ", db='" + db + '\'' +
                ", values='" + values + '\'' +
                ", username='" + username + '\'' +
                ", password='" + password + '\'' +
                ", offset_key='" + offset_key + '\'' +
                ", session_id='" + session_id + '\'' +
                ", offset=" + offset +
                '}';
    }
}
