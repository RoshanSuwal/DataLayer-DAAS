package org.ekbana.bigdata.dbmanagement;

import net.rubyeye.xmemcached.exception.MemcachedException;
import org.apache.log4j.Logger;
import org.ekbana.bigdata.constants.Status;
import org.ekbana.bigdata.crud.Delete;
import org.ekbana.bigdata.crud.Insert;
import org.ekbana.bigdata.crud.Select;
import org.ekbana.bigdata.crud.Update;
import org.json.JSONObject;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RestController;

import java.io.IOException;
import java.net.ConnectException;
import java.security.NoSuchAlgorithmException;
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
    public byte[] acceptSelectRequest(@RequestBody AcceptRequest query) throws IOException {

        String queryStatus = "";
        if (query.toString().isEmpty()) {
            return getReturnMsg("query is empty", Status.EMPTY_REQUEST).getBytes();
        }
        Select select = new Select(query.getQuery(), query.getDbms(), query.getValues());

        try {
            if (select.isValid) {
                queryStatus = sendRequest(select.getFinalQuery(), select.getKeyspace(), select.getTable(), 1, query);
            } else {
                logger.error("malformed query detected : " + query.getQuery());
                queryStatus = getReturnMsg("malformed query", Status.MALFORMED_QUERY);
            }
        } catch (NullPointerException e) {
            logger.error(e.getMessage());
            queryStatus = getReturnMsg("Nullpointer exception", Status.NULLPOINTER);
        } catch (ConnectException e) {
            queryStatus = getReturnMsg("Couldnot connect to socket layer", Status.CONNECTION_REFUSED);
        } catch (Exception e) {
            e.printStackTrace();
            queryStatus = getReturnMsg("malformed query", Status.MALFORMED_QUERY);
            logger.error(e.getLocalizedMessage());
        }
        return queryStatus.getBytes();
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

        if (query.toString().isEmpty()) {
            return getReturnMsg("query is empty", Status.EMPTY_REQUEST);
        }

        Insert insert = new Insert(query.getQuery(), query.getDbms(), query.getValues());

        try {
            if (insert.isValid) {
                queryStatus = sendRequest(insert.getFinalQuery(), insert.getKeySpace(), insert.getTable(), 2, query);
            } else {
                logger.error("malformed query detected : " + query.getQuery());
                queryStatus = getReturnMsg("malformed query", Status.MALFORMED_QUERY);
            }
        } catch (NullPointerException e) {
            queryStatus = getReturnMsg("malformed query", Status.MALFORMED_QUERY);
            logger.error(e.getMessage());
        } catch (Exception e) {
            queryStatus = getReturnMsg("malformed query", Status.MALFORMED_QUERY);
            logger.error(e.getLocalizedMessage());
        }
        return queryStatus;
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
        if (query.toString().isEmpty()) {
            return getReturnMsg("query is empty", Status.EMPTY_REQUEST);
        }
        Update update = new Update(query.getQuery(), query.getDbms(), query.getValues());

        try {
            if (update.isValid) {
                queryStatus = sendRequest(update.getFinalQuery(), update.getKeyspace(), update.getTable(), 3, query);
            } else {
                logger.error("malformed query detected : " + query.getQuery());
                queryStatus = getReturnMsg("malformed query", Status.MALFORMED_QUERY);
            }
        } catch (NullPointerException e) {
            queryStatus = getReturnMsg("malformed query", Status.MALFORMED_QUERY);
            logger.error(e.getMessage());
        } catch (Exception e) {
            queryStatus = getReturnMsg("malformed query", Status.MALFORMED_QUERY);
            logger.error(e.getLocalizedMessage());
        }
        return queryStatus;
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
        if (query.toString().isEmpty()) {
            return getReturnMsg("query is empty", Status.EMPTY_REQUEST);
        }

        Delete delete = new Delete(query.getQuery(), query.getDbms(), query.getValues());
        try {
            if (delete.isValid) {
                queryStatus = sendRequest(delete.getFinalQuery(), delete.getKeyspace(), delete.getTable(), 4, query);
            } else {
                logger.error("malformed query detected : " + query.getQuery());
                queryStatus = getReturnMsg("malformed query", Status.MALFORMED_QUERY);
            }
        } catch (NullPointerException e) {
            queryStatus = getReturnMsg("malformed query", Status.MALFORMED_QUERY);
            logger.error(e.getMessage());
        } catch (Exception e) {
            queryStatus = getReturnMsg("malformed query", Status.MALFORMED_QUERY);
            logger.error(e.getLocalizedMessage());
        }
        return queryStatus;
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

//        Client c=new Client(query.getSession_id(),query.getOffset_key(),rt,qry,keyspace,table,query.getValues(),query.getUsername(),query.getPassword());
//        logger.info("submitting new connection to executor");
//        f = executor.submit(c);
//        return f.get().toString();

        return qry;

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

}
