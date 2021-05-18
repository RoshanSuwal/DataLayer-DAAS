package org.ekbana.bigdata.dbmanagement;

import org.apache.log4j.Logger;
import org.ekbana.bigdata.sqlparser.QueryBuilder;
import org.json.JSONException;
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


    Client(String session_id, String offset_key, int request_type,String sql,String keyspace, String username,String password) throws IOException {
        this.sqlObj
                .put("session_id", session_id)
                .put("query", sql)
                .put("username", username)
                .put("password", password)
                .put("keyspace", keyspace)
                //.put("values", values)
                .put("offset_key", offset_key)
                .put("request_type", request_type)
                .put("token", gfp.getToken());
        this.clientSocket = new Socket(gfp.getSocketClientIP(), Integer.parseInt(gfp.getSocketClientPort()));
    }

    Client(String session_id, String offset_key, int request_type,String sql,String keyspace,String table,String values, String username,String password) throws IOException {
        this.sqlObj
                .put("session_id", session_id)
                .put("query", sql)
                .put("username", username)
                .put("password", password)
                .put("keyspace", keyspace)
                .put("table",table)
                .put("values", values)
                .put("offset_key", offset_key)
                .put("request_type", request_type)
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