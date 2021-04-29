package org.ekbana.bigdata.dbmanagement;

import org.apache.log4j.Logger;
import org.json.JSONObject;
import org.springframework.util.StopWatch;

import java.io.*;
import java.net.Socket;
import java.util.concurrent.Callable;
import java.util.regex.Pattern;

/**
 * This Class opens a socket connection to server using asynchronous
 * call method.
 */
class Client implements Callable {
    String sql = "";
    DataOutputStream outToServer;
    private JSONObject sqlObj = new JSONObject();
    private GetFromProperty gfp = new GetFromProperty();
    static final Logger logger = Logger.getLogger(Client.class);
    private Socket clientSocket;


    Client(String session_id, String dbms, String db, String sql, String user, String offset_key, int offset, String pass, String table,
           String values, int rt) throws IOException {
        String[] t = table.split(Pattern.quote("."));
        this.sqlObj
                .put("session_id", session_id)
                .put("dbms", dbms)
                .put("db", db)
                .put("query", sql)
                .put("username", user)
                .put("password", pass)
                .put("keyspace", t[0])
                .put("table", t[1])
                .put("values", values)
                .put("offset_key", offset_key)
                .put("offset", offset)
                .put("request_type", rt)
                .put("token", gfp.getToken());
        this.clientSocket = new Socket(gfp.getSocketClientIP(), Integer.parseInt(gfp.getSocketClientPort()));
    }

    /**
     * Returns query output from socket server.It opens a connection
     * //     * @param sql   SQLOperation/NOSQL query from user
     *
     * @return
     */
    String processmessage() {
        String s = "";
        try {
            logger.info("initiating new socket connection on " + gfp.getSocketClientIP() + ":" + gfp.getSocketClientPort());
            PrintWriter writer = new PrintWriter(this.clientSocket.getOutputStream(), true);
            writer.println(this.sqlObj.toString());
            writer.flush();
                BufferedReader inFromServer = new BufferedReader(new InputStreamReader(clientSocket.getInputStream()));
                s = inFromServer.readLine();
            clientSocket.close();
        } catch (Exception e) {
            logger.error(e.getLocalizedMessage());
        }
        return s;
    }

    @Override
    public String call() {
        StopWatch st = new StopWatch();
        st.start();
        String s ="";
        s = processmessage();
        return s;
    }
}