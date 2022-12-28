package org.ekbana.bigdata.security;

import javax.crypto.Cipher;
import javax.crypto.spec.IvParameterSpec;
import javax.crypto.spec.SecretKeySpec;
import java.io.UnsupportedEncodingException;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.Arrays;
import java.util.Base64;

public class AES {

    public static SecretKeySpec setKey(String secret)
    {
        MessageDigest sha = null;
        byte[] key;
        SecretKeySpec secretKey = null;
        try {
            key = secret.getBytes("UTF-8");
            sha = MessageDigest.getInstance("SHA-1");
            key = sha.digest(key);
            key = Arrays.copyOf(key, 16);
            secretKey = new SecretKeySpec(key, "AES");
        }
        catch (NoSuchAlgorithmException | UnsupportedEncodingException e) {
            e.printStackTrace();
        }
        return secretKey;
    }

    public static String encrypt(String strToEncrypt, String encrypt_key)
    {
        try
        {
            SecretKeySpec secret_key = setKey(encrypt_key);
            Cipher cipher = Cipher.getInstance("AES/CBC/PKCS5PADDING");
            cipher.init(Cipher.ENCRYPT_MODE, secret_key);
            return Base64.getEncoder().encodeToString(cipher.doFinal(strToEncrypt.getBytes("UTF-8")));
        }
        catch (Exception e)
        {
            System.out.println("Error while encrypting: " + e.toString());
        }
        return null;
    }

    public static String decrypt(String ciphertext,String key){
        try {
            byte[] cipherbytes = Base64.getDecoder().decode(ciphertext);

            byte[] initVector = Arrays.copyOfRange(cipherbytes,0,16);

            byte[] messagebytes = Arrays.copyOfRange(cipherbytes,16,cipherbytes.length);

            IvParameterSpec iv = new IvParameterSpec(initVector);
            SecretKeySpec skeySpec = new SecretKeySpec(key.getBytes("UTF-8"), "AES");

            Cipher cipher = Cipher.getInstance("AES/CBC/PKCS5PADDING");
            cipher.init(Cipher.DECRYPT_MODE, skeySpec, iv);

            // Convert the ciphertext Base64-encoded String back to bytes, and
            // then decrypt
            byte[] byte_array = cipher.doFinal(messagebytes);

            // Return plaintext as String
            return new String(byte_array, StandardCharsets.UTF_8);
        } catch (Exception ex) {
            ex.printStackTrace();
        }

        return null;

    }
//    public static String decrypt(String strToDecrypt, String decrypt_key)
//    {
//        try
//        {
//            SecretKeySpec secret_key = setKey(decrypt_key);
//            Cipher cipher = Cipher.getInstance("AES/CBC/PKCS5PADDING");
//            cipher.init(Cipher.DECRYPT_MODE, secret_key);
////            System.out.println(strToDecrypt);
////            System.out.println(decrypt_key);
//            return new String(cipher.doFinal(Base64.getDecoder().decode(strToDecrypt)));
//        }
//        catch (Exception e)
//        {
//            e.printStackTrace();
//            System.out.println("Error while decrypting: " + e.toString());
//        }
//        return null;
//    }
}