package org.ekbana.bigdata.dbmanagement;

import net.rubyeye.xmemcached.exception.MemcachedException;
import netscape.javascript.JSException;
import org.apache.log4j.Logger;
import org.ekbana.bigdata.constants.Status;
import org.ekbana.bigdata.crud.*;
import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RestController;

import java.io.IOException;
import java.net.ConnectException;
import java.security.NoSuchAlgorithmException;
import java.sql.SQLException;
import java.util.concurrent.*;

@RestController
public class AcceptRequestController {

    private static final Logger logger = Logger.getLogger(Application.class);
    String finalQuery = "";
    private ExecutorService executor = Executors.newFixedThreadPool(10);
    private Future f;
    private MemCacheClient mcc = new MemCacheClient();
    private GetFromProperty gfp = new GetFromProperty();

    public AcceptRequestController() throws IOException {
    }

    /**
     * Returns output from query string.It uses <code>ThreadPool</code> for sending request to socket server.
     *
     * @param query String SQLOperation/NOSQL for querying database
     * @return output of query string
     */
    @PostMapping("/select")
    public byte[] acceptSelectRequest(@RequestBody AcceptRequest query) throws IOException, SQLException {

        String queryStatus = "";
        if (query.getQuery().isEmpty()) {
            return getReturnMsg(gfp.getEmptyQueryErrorMsg(), Status.EMPTY_REQUEST).getBytes();
        }
        Select select = new Select(query.getQuery(), query.getDbms(), query.getValues());

        return getResult(select, 1, query).getBytes();
//        try {
//            if (select.isValid) {
//                queryStatus = sendRequest(select.getFinalQuery(), select.getKeyspace(), select.getTable(), 1, query);
//            } else {
//                logger.error("malformed query detected : " + query.getQuery());
//                queryStatus = getReturnMsg("malformed query", Status.MALFORMED_QUERY);
//            }
//        }catch (SQLException e) {
//            //this exception is triggered when Postgres Server in Down
//            queryStatus=getReturnMsg("Internal Database Error, Inform to Bigdata Team",400);
//        }catch (NullPointerException e) {
//            logger.error(e.getMessage());
//            queryStatus = getReturnMsg("Invalid table name", Status.NULLPOINTER);
//        } catch (ConnectException e) {
//            // This Exception is triggered when Applayer-Cassandra Server is down.
//            queryStatus = getReturnMsg("SocketLayer Error, Inform to BigData Team", Status.CONNECTION_REFUSED);
//        } catch (Exception e) {
//            e.printStackTrace();
//            queryStatus = getReturnMsg("malformed query", Status.MALFORMED_QUERY);
//            logger.error(e.getLocalizedMessage());
//        }
//        return queryStatus.getBytes();
    }

    /**
     * Insert data into database.It uses <code>ThreadPool</code> for sending request to socket server.
     *
     * @param query String SQLOperation/NOSQL for querying database
     * @return output of query string
     */

    @PostMapping(path = "/insert")
    public String acceptInsertRequest(@RequestBody AcceptRequest query) throws InterruptedException, IOException, ExecutionException, TimeoutException, MemcachedException, NoSuchAlgorithmException {

        String queryStatus = null;

        if (query.getQuery().isEmpty()) {
            return getReturnMsg(gfp.getEmptyQueryErrorMsg(), Status.EMPTY_REQUEST);
        } else if (query.getValues().isEmpty()) {
            return getReturnMsg(gfp.getNullValueInsertionError(), Status.EMPTY_REQUEST);
        }

        try {
            new JSONObject(query.getValues());
        } catch (JSONException e) {
            try {
                new JSONArray(query.getValues());
            } catch (JSONException f) {
                return getReturnMsg("values must be of jsonString format",Status.EMPTY_REQUEST);
            }
        }


        Insert insert = new Insert(query.getQuery(), query.getDbms(), query.getValues());

        return getResult(insert, 2, query);

//        try {
//            if (insert.isValid) {
//                queryStatus = sendRequest(insert.getFinalQuery(), insert.getKeySpace(), insert.getTable(), 2, query);
//            } else {
//                logger.error("malformed query detected : " + query.getQuery());
//                queryStatus = getReturnMsg("malformed query", Status.MALFORMED_QUERY);
//            }
//        } catch (NullPointerException e) {
//            queryStatus = getReturnMsg("malformed query", Status.MALFORMED_QUERY);
//            logger.error(e.getMessage());
//        } catch (Exception e) {
//            queryStatus = getReturnMsg("malformed query", Status.MALFORMED_QUERY);
//            logger.error(e.getLocalizedMessage());
//        }
//        return queryStatus;
    }

    /**
     * Update the value in database.It uses <code>ThreadPool</code> for sending request to socket server.
     *
     * @param query String SQLOperation/NOSQL for querying database
     * @return output of query string
     */
    @PostMapping(path = "/update")
    public String acceptUpdateRequest(@RequestBody AcceptRequest query) throws InterruptedException, IOException, ExecutionException, TimeoutException, MemcachedException, NoSuchAlgorithmException {
        String queryStatus = null;
        if (query.getQuery().isEmpty()) {
            return getReturnMsg(gfp.getEmptyQueryErrorMsg(), Status.EMPTY_REQUEST);
        }
        Update update = new Update(query.getQuery(), query.getDbms(), query.getValues());
        return getResult(update, 3, query);

//        try {
//            if (update.isValid) {
//                queryStatus = sendRequest(update.getFinalQuery(), update.getKeySpace(), update.getTable(), 3, query);
//            } else {
//                logger.error("malformed query detected : " + query.getQuery());
//                queryStatus = getReturnMsg("malformed query", Status.MALFORMED_QUERY);
//            }
//        }catch (SQLException e){
//            queryStatus = getReturnMsg("database error", Status.MALFORMED_QUERY);
//            logger.error(e.getMessage());
//        }catch (NullPointerException e) {
//            queryStatus = getReturnMsg(e.getMessage(), Status.MALFORMED_QUERY);
//            logger.error(e.getMessage());
//        } catch (Exception e) {
//            queryStatus = getReturnMsg("malformed query", Status.MALFORMED_QUERY);
//            logger.error(e.getLocalizedMessage());
//        }
//        return queryStatus;
    }

    /**
     * Deletes the documents in database.It uses <code>ThreadPool</code> for sending request to socket server.
     *
     * @param query String SQLOperation/NOSQL for querying database
     * @return output of query string
     */
    @PostMapping(path = "/delete/rm")
    public String acceptDeleteRequest(@RequestBody AcceptRequest query) throws InterruptedException, IOException, ExecutionException, TimeoutException, MemcachedException, NoSuchAlgorithmException {
        String queryStatus = null;
        if (query.getQuery().isEmpty()) {
            return getReturnMsg(gfp.getEmptyQueryErrorMsg(), Status.EMPTY_REQUEST);
        }

        Delete delete = new Delete(query.getQuery(), query.getDbms(), query.getValues());

        return getResult(delete, 4, query);
//        try {
//            if (delete.isValid) {
//                queryStatus = sendRequest(delete.getFinalQuery(), delete.getKeySpace(), delete.getTable(), 4, query);
//            } else {
//                logger.error("malformed query detected : " + query.getQuery());
//                queryStatus = getReturnMsg("malformed query", Status.MALFORMED_QUERY);
//            }
//        } catch (NullPointerException e) {
//            queryStatus = getReturnMsg("malformed query", Status.MALFORMED_QUERY);
//            logger.error(e.getMessage());
//        } catch (Exception e) {
//            queryStatus = getReturnMsg("malformed query", Status.MALFORMED_QUERY);
//            logger.error(e.getLocalizedMessage());
//        }
//        return queryStatus;
    }


    /**
     * Sends user requests to server and response back to user.This function uses thread pool of five threads
     * Number of threads is subjected to change
     *
     * @param query Query sent by User
     * @return Response returned from database
     * @throws ExecutionException
     * @throws InterruptedException
     */
    private String sendRequest(String qry, String keyspace, String table, int rt, AcceptRequest query) throws ExecutionException, InterruptedException, IOException {
        if (keyspace.isEmpty()) {
            throw new NullPointerException("keyspace name is empty");
        } else if (table.isEmpty()) {
            throw new NullPointerException("table name is empty");
        }

        Client c=new Client(query.getSession_id(),query.getOffset_key(),rt,qry,keyspace,table,query.getValues(),query.getUsername(),query.getPassword());
        logger.info("submitting new connection to executor");
        f = executor.submit(c);
        return f.get().toString();

//        return qry;

        //mcc.add(query.getDbms(), query.getDb(), query.getQuery(), 0, result);
    }

    /**
     * @param msg
     * @param status_code
     * @return String
     */
    private String getReturnMsg(String msg, int status_code) {
        JSONObject obj = new JSONObject();
        obj.put("status_code", status_code);
        obj.put("msg", msg);
        return obj.toString();
    }

    public String getResult(IQuery query, int rt, AcceptRequest acceptRequest) {
        String queryStatus = "";
        try {
            if (query.isValid()) {
                queryStatus = sendRequest(query.getFinalQuerY(), query.getKeySpace(), query.getTable(), rt, acceptRequest);
            } else {
                queryStatus = getReturnMsg(gfp.getMalformedQueryErrorMsg(), Status.MALFORMED_QUERY);
                logger.error("[QUERY] malformed query");
            }
        } catch (SQLException e) {//triggered when postgres server is down
            queryStatus = getReturnMsg(gfp.getDatabaseConnectionErrorMsg(), Status.DBMS_NOT_CONFIGURED);
            logger.error("[POSTGRES] Failed to connect to postgres database");
        } catch (ConnectException e) {//triggered when failed to connect to Applayer-cassandra server
            logger.error("[TCP SOCKET] " + e.getMessage());
            queryStatus = getReturnMsg(gfp.getApplayerConnectionErrorMsg(), Status.CONNECTION_REFUSED);
        } catch (NullPointerException e) { //triggered when table name in invalid ie not found in postgres database;
            logger.error("[DATABASE] " + e.getMessage());
            queryStatus = getReturnMsg(gfp.getInvalidTableNameErrorMsg(), Status.NULL_POINTER);
        } catch (Exception e) {
            queryStatus = getReturnMsg(e.getMessage(), Status.MALFORMED_QUERY);
        }

        return queryStatus;

    }

}
