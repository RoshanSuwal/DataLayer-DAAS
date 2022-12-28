package org.ekbana.bigdata.poc;

import com.fasterxml.jackson.databind.deser.impl.CreatorCollector;
import org.apache.http.HttpResponse;
import org.apache.http.client.HttpClient;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.entity.ContentType;
import org.apache.http.entity.StringEntity;
import org.apache.http.impl.client.HttpClientBuilder;
import org.ekbana.bigdata.security.AES;
import org.json.JSONArray;
import org.json.JSONObject;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;

public class ClientConnection {

    public String preparePayload(String query,
                                 String values,
                                 String username,
                                 String password,
                                 String offset_key,
                                 String session_id
    ){
        JSONObject jsonObject=new JSONObject();
        jsonObject.put("query",query);
        jsonObject.put("values",values);
        jsonObject.put("username",username);
        jsonObject.put("password",password);
        jsonObject.put("offset_key",offset_key);
        jsonObject.put("session_id",session_id);

        return jsonObject.toString();
    }

    public String sendRequest(String payload, String queryType) throws IOException {
        String database_url="https://casservice.ekbana.net/";
        StringEntity entity=new StringEntity(payload, ContentType.APPLICATION_JSON);

        HttpClient httpClient= HttpClientBuilder.create().build();

        HttpPost httpPost=new HttpPost(database_url+queryType);

        httpPost.setEntity(entity);

        HttpResponse response=httpClient.execute(httpPost);
        InputStream inputStream=response.getEntity().getContent();

        BufferedReader inFromServer=new BufferedReader(new InputStreamReader(inputStream));

        String final_result=inFromServer.readLine();

        inFromServer.close();

        return final_result;
    }

    public static void select() throws IOException {
        ClientConnection clientConnection=new ClientConnection();

        String query="select count(*) from v_sale ";

        int count=0;
        String status = "";

        while (!status.equals("done")){
            count++;

            String payload=clientConnection.preparePayload(
                    query,
                    "",
                    "cassandra",
                    "cassandra",
                    "",
                    ""
            );

            String responseString= clientConnection.sendRequest(payload,"select");

            JSONObject final_result=new JSONObject(responseString);
            int statusCode=final_result.getInt("status_code");

            System.out.println(statusCode);

            if (statusCode==200){
                String rs=final_result.getString("result");
                System.out.println(rs);


//                final String decrypted = AES.decrypt(rs, "Tz9f7W8hBg2RuRNhPuUXxF7F6uxRsvmj");
//                final String decrypted = AES.decrypt(rs, "Tz9f7W8hBg2RuRNhPuUXxF7F6uxRsvmj");
                final String decrypted = AES.decrypt(rs, "9uMs6PFC9aQ8ztEhG63rApu6pTZRBRmk");
                final JSONArray objects = new JSONArray(decrypted);
//                final JSONObject x = objects.getJSONObject(0);
                System.out.println(objects);
            }
            status="done";
        }
    }

    public static void main(String[] args) throws IOException {
        select();
    }

    /*
     * V_SALE : last bill date : 2021-12-8
     * V_COLLECTION : last bill date : 2021-12-28
     *  */
}
