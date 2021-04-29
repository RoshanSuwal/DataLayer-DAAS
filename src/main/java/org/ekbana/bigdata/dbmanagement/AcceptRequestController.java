package org.ekbana.bigdata.dbmanagement;

import net.rubyeye.xmemcached.exception.MemcachedException;
import org.apache.log4j.Logger;
import org.ekbana.bigdata.constants.Status;
import org.ekbana.bigdata.crud.Delete;
import org.ekbana.bigdata.crud.Insert;
import org.ekbana.bigdata.crud.Select;
import org.ekbana.bigdata.crud.Update;
import org.json.JSONObject;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RestController;

import java.io.IOException;
import java.net.ConnectException;
import java.security.NoSuchAlgorithmException;
import java.util.concurrent.*;

@RestController
public class AcceptRequestController {

    private ExecutorService executor = Executors.newFixedThreadPool(10);
    private Future f;
    String finalQuery = "";
    private MemCacheClient mcc = new MemCacheClient();
    private static final Logger logger = Logger.getLogger(Application.class);
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
        System.out.println(query.toString());

        String queryStatus = "";
        if(query.toString().isEmpty()){
            return getReturnMsg("query is empty", Status.EMPTY_REQUEST).getBytes();
        }

        Select select =new Select(query.getQuery(),query.getDbms());

        try {
            if (select.isValid) {
                queryStatus = sendRequest(select.getFinalQuery(),select.getTable(),1,query);
            } else {
                logger.error("malformed query detected : " + query.getQuery());
                queryStatus = getReturnMsg("malformed query", Status.MALFORMED_QUERY);
            }
        } catch (NullPointerException e) {
            e.printStackTrace();
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

        System.out.println(query.toString());

        String queryStatus = null;

        System.out.println(query.getValues()+":"+query.getDbms());

        Insert insert = new Insert(query.getQuery(),query.getDbms());
        System.out.println("insert ");
        try {
            if (insert.isValid) {
                queryStatus = sendRequest(insert.getFinalQuery(),insert.getTable(),2,query);
            } else {
                logger.error("malformed query detected : " + query.getQuery());
                queryStatus = getReturnMsg("malformed query", Status.MALFORMED_QUERY);
            }
        } catch (NullPointerException e) {
            queryStatus = getReturnMsg("malformed query", Status.MALFORMED_QUERY);
            System.out.println(e.getLocalizedMessage());
        } catch (Exception e) {
            queryStatus = getReturnMsg("malformed query", Status.MALFORMED_QUERY);
            System.out.println(e.getLocalizedMessage());
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
        Update update = new Update(query.getQuery(),query.getDbms());

        try {
            if (update.isValid) {
                queryStatus = sendRequest(update.getFinalQuery(),update.getTable(),3,query);
            } else {
                logger.error("malformed query detected : " + query.getQuery());
                queryStatus = getReturnMsg("malformed query", Status.MALFORMED_QUERY);
            }
        } catch (NullPointerException e) {
            queryStatus = getReturnMsg("malformed query", Status.MALFORMED_QUERY);
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
    @PostMapping(path = "/delete")
    public String acceptDeleteRequest(@RequestBody AcceptRequest query) throws InterruptedException, IOException, ExecutionException, TimeoutException, MemcachedException, NoSuchAlgorithmException {
        String queryStatus = null;
        System.out.println(query.toString());
        Delete delete = new Delete(query.getQuery(),query.getDbms());
        try {
            if (delete.isValid) {
                System.out.println(delete.isValid+":"+delete.getFinalQuery());
                queryStatus = sendRequest(delete.getFinalQuery(),delete.getTable(),4,query);
            } else {
                logger.error("malformed query detected : " + query.getQuery());
                queryStatus = getReturnMsg("malformed query", Status.MALFORMED_QUERY);
            }
        } catch (NullPointerException e) {
            queryStatus = getReturnMsg("malformed query", Status.MALFORMED_QUERY);
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
    private String sendRequest(String qry, String table,int rt,AcceptRequest query) throws ExecutionException, InterruptedException, IOException {
        if(table.isEmpty()){
            throw new NullPointerException("table name is empty");
        }

//        Client c = new Client(query.getSession_id(), query.getDbms(), query.getDb(), qry, query.getUsername(), query.getOffset_key(), query.getOffset(),
//                query.getPassword(),table,query.getValues(),rt);
//        logger.info("submitting new connection to executor");
//        f = executor.submit(c);
//        return f.get().toString();
//                mcc.add(query.getDbms(), query.getDb(), query.getQuery(), 0, result);

        return qry;
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
