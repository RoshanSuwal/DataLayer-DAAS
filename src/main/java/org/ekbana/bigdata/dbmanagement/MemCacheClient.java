package org.ekbana.bigdata.dbmanagement;

import net.rubyeye.xmemcached.MemcachedClient;
import net.rubyeye.xmemcached.MemcachedClientBuilder;
import net.rubyeye.xmemcached.XMemcachedClientBuilder;
import net.rubyeye.xmemcached.exception.MemcachedException;
import org.apache.log4j.Logger;
import org.springframework.context.annotation.Configuration;

import javax.xml.bind.DatatypeConverter;
import java.io.IOException;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.concurrent.TimeoutException;

/**
 * This class defines various methods to configure memecache and
 * add hashed key with it's value in memcache server
 */
@Configuration
public class MemCacheClient {
    private static final Logger logger = Logger.getLogger(MemCacheClient.class);

    private MemcachedClient memcachedClient;
    private MemcachedClient memcachedClient1;

    private GetFromProperty gfp = new GetFromProperty();


    /**
     * Creates a static variable <b>memcachedClient</b> to configure
     * set values for memcache server like defining servers and setting
     * expiration time for key
     * @throws IOException
     */
    public MemCacheClient() throws IOException{
        try {
            MemcachedClientBuilder mcb = new XMemcachedClientBuilder();
            MemcachedClientBuilder mcb1 = new XMemcachedClientBuilder();

            memcachedClient = mcb.build();
            memcachedClient1 = mcb1.build();

//            memcachedClient.addServer(gfp.getMemcacheServer1(), Integer.parseInt(gfp.getMemcacheServer1Port()));
//            memcachedClient.setSanitizeKeys(false);
        }catch (IOException e){
            e.printStackTrace();
            logger.error(e.getLocalizedMessage());
        }

        }


    /**
     * Inserts key value pair in memcache server
     * @param key   this key is used to store and later retrieve value
     *              from memcache server
     * @param exp   key-value will expire in <b>exp</b> time
     * @param value     <b>value to be stored</b>
     */
 void add(String dbms,String db,String query,int exp,String value){
    try {
        memcachedClient.add(hash(dbms+db+query),exp,value);
        memcachedClient1.add(hash(dbms+db+query),exp,value);

        System.out.println((String) memcachedClient.get(hash(dbms+db+query)));
    } catch (TimeoutException | InterruptedException | MemcachedException e) {
        e.printStackTrace();
    }
}

    /**
     * @param key   <b>key</b> that is to be hashed
     * @return  String      hashed string
     */
private static String hash(String key){
    MessageDigest md = null;
    try {
        md = MessageDigest.getInstance("MD5");
    } catch (NoSuchAlgorithmException e) {
        e.printStackTrace();
    }
    if (md != null) {
        md.update(key.getBytes());
    }
    byte[] digest = md != null ? md.digest() : new byte[0];
    System.out.println(DatatypeConverter.printHexBinary(digest).toLowerCase());
    return DatatypeConverter.printHexBinary(digest).toLowerCase();
}

}

