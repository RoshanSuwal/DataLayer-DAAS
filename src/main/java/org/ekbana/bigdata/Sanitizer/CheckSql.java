package org.ekbana.bigdata.Sanitizer;

import org.apache.log4j.Logger;
import org.ekbana.bigdata.constants.Endpoints;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Locale;
import java.util.Objects;

public class CheckSql {

    private static final String[] CASSANDRA_COMMENTS={"/*","*/"};
    private static final String TOKEN_ORACLE_HINT_START = "/*+";
    private static final String TOKEN_ORACLE_HINT_END = "*/";
    private static final String[] ILLEGAL_CHARACTERS = {"--", "#", ";", "'",TOKEN_ORACLE_HINT_START, TOKEN_ORACLE_HINT_END};
    private static final String[] STOP_WORDS = {"drop table", "create table", "drop database", "create database","truncate"};

    private static final Logger logger = Logger.getLogger(CheckSql.class);

    private String sanitizedQuery = "";
    String endpoint = "";

    public CheckSql(String query,String endpoint) {
        this.endpoint = endpoint;
        setSanitizedQuery(query);
    }

    /**
     * Checks if query statement contains words define in
     * <b>STOP_WORDS_MONGO</b> or <b>STOP_WORDS</b>.
     *
     * @return Boolean      returns true if query contains stop words else returns false
     */

    private Boolean containsStopWords() {
        logger.info("checking if query contains any stop words");
        if (Arrays.stream(STOP_WORDS).parallel().anyMatch(getSanitizedQuery()::contains)){
            System.out.println("[CheckSqk] contains stop words");
            return true;
        }
        return false;
    }

    /**
     * checks if query statement is clean
     *
     * @return the boolean     returns true if every method defined here returned true else returns false
     */

    private Boolean isMalFormed() {
        logger.info("checking for any illegal string");
        Boolean hello =true;
        try{
            switch (this.endpoint){
                case "select":
                    hello = (fishyWhere() ||  containsIllegalCharacter() || containsStopWords() || hasConsecutiveQuotes()  || firstWordMisMatched());
                    break;
                case "insert":
                    hello=(firstWordMisMatched() || containsIllegalCharacter() || containsStopWords());
                    break;
                case "update":
                    hello=(firstWordMisMatched() || hasConsecutiveQuotes() || fishyWhere() || containsStopWords() || containsIllegalCharacter());
                    break;
                case "delete":
                    hello=(firstWordMisMatched() || containsStopWords() || containsIllegalCharacter() || fishyWhere() || hasConsecutiveQuotes());
                    break;
                default:
                    break;
            }

        }catch (Exception e){
            e.printStackTrace();
        }
        return hello;
    }

    /**
     * First words defines the sql operation for endpoint defined on {@link Endpoints}.
     * for example query for 'select' endpoint must start from "select" word
     *
     * @return Boolean      returns true if first word matches endpoints else returns false
     */

    private Boolean firstWordMisMatched() {
        return !getSanitizedQuery().split(" ")[0].equals(this.endpoint);
    }

    /**
     * checks if where clause contains any fishy expressions like '1==1' or '1<2' that always returns true.
     * It is accompained by other methods like 'hasConsecutiveQuotes','malformedKeyValue'
     *
     * @return Boolean     returns true if first words matches endpoint else returns false
     */

    private Boolean fishyWhere() {
        logger.info("checking for any fishy where");
        return malformedKeyValue();
    }

    /**
     * checks if query statement contains any consecutive quotes. It is generally a sign of
     * comment and should not be allowed
     *
     * @return Boolean      returns true if query statement has consecutive quotes else returns false
     */

    private Boolean hasConsecutiveQuotes() {
        logger.info("checking for any consecutive quotes");
        return (getSanitizedQuery().contains("\"\"") || getSanitizedQuery().contains("''"));
    }

    /**
     * this method is accompained by other methods like keyEqualsValue and  keyIsString
     *
     * @return Boolean      returns true if key is malformed else returns false
     */

    private Boolean malformedKeyValue() {
        Boolean malformedKeyValue = false;
        String query = getSanitizedQuery();
        ArrayList<String> fc = new ArrayList<>();
        if (query.contains("where")) {
            String a[] = query.split("where");
            String[] afterWhere = a[1].split(" and ");
            for (String d : afterWhere) {
                if (d.contains(" or ")) {
                    String[] p = d.split(" or ");
                    fc.addAll(Arrays.asList(p));
                } else {
                    fc.add(d);
                }
            }
        }

        System.out.println(Arrays.toString(new ArrayList[]{fc}));
        String[] operators={"!=",">=","<=","==",">","<","="};
        for (String aFc : fc) {
            for(String operator: operators){
                if (!keyIsString(aFc,operator)){
                    System.out.println("[CheckSql]"+ aFc);
                    return true;
                }
            }
        }

        return malformedKeyValue;
    }

    /**
     * checks if key and value in where clause of query statement are equal.
     *
     * @param pair     key-value pair
     * @param operator could be any of '==', '>' ,'<', '!=', '<=', '>=', '='
     * @return Boolean      returns true if key and value is same else returns false
     */

    private Boolean keyEqualsValue(String pair, String operator) {
        Boolean keyEqualsValue = false;
        if (pair.contains(operator)) {
            String[] kve = pair.split(operator);
            if (kve.length > 0) {
                String key = trimQuotes(kve[0]);
                String value = trimQuotes(kve[1]);
                if (Objects.deepEquals(key, value)) {
                    keyEqualsValue = true;
                    System.out.println("[CheckSql] "+pair);
                }
            }
        }
        return keyEqualsValue;
    }

    /**
     * checks if key in where clause is string
     *
     * @param pair     key-value pair
     * @param operator could be any of '==', '>' ,'<', '!=', '<=', '>=', '='
     * @return Boolean      returns true if key is string else returns false
     */

    private Boolean keyIsString(String pair, String operator) {

        Boolean keyIsString = true;
        if (pair.contains(operator)) {
            String[] kve = pair.split(operator);
            if (kve.length > 0) {
                String key = trimQuotes(kve[0]);
                if (isInt(key) || isFloat(key) || key.charAt(0)=='$'){
                    logger.error("[CheckSql] Fishy where detected :"+pair);
                    keyIsString = false;}
            }
        }
        //System.out.println(pair+" : "+operator+" : "+keyIsString);
        return keyIsString;
    }

    /**
     * checks if key is  int .
     *
     * @param key the key to be checked
     * @return returns true if key is int else returns false
     */

    private Boolean isInt(String key) {
        try {
            Integer.parseInt(key);
            return true;
        } catch (NumberFormatException e) {
            return false;
        }

    }

    /**
     * checks if key is  float .
     *
     * @param key the key to be checked
     * @return returns true if key is float else returns false
     */

    private Boolean isFloat(String key) {
        try {
            Float.parseFloat(key);
            return true;
        } catch (NumberFormatException e) {
            return false;
        }
    }

    /**
     * Checks if query statement contains words define in
     * <b>ILLEGAL_CHARACTERS</b>
     *
     * @return Boolean      returns true if query contains illegal characters else returns false
     */

    private Boolean containsIllegalCharacter() {
        logger.info("checking for any illegal character");
        if(endpoint.equals("select")){
            if (Arrays.stream(ILLEGAL_CHARACTERS).parallel().anyMatch(getSanitizedQuery()::contains) ||
                    Arrays.stream(CASSANDRA_COMMENTS).parallel().anyMatch(getSanitizedQuery()::contains)){
                System.out.println("[CheckSql] contains illegal characters");
                return true;
            }else{
                return false;
            }
        }

        if(Arrays.stream(ILLEGAL_CHARACTERS).parallel().anyMatch(getSanitizedQuery()::contains)){
            System.out.println("[CheckSql] contains illegal characters");
            return true;
        }else {
            return false;
        }

    }

    /**
     * Trim quotes on token
     *
     * @param token the token
     * @return String       trimmed token
     */

    private String trimQuotes(String token) {
        String token1 = token.trim();
        return token1.replace("'", "").replace("\"", "");
    }

    /**
     * checks if the query statement provided is a valid sql statement.
     *
     * @return Boolean returns true id it a valid sql else returns false
     */

    public Boolean isValidSql() {
        logger.info("validating sql");
        Boolean isValidSql = true;
        if (isMalFormed())
            isValidSql = false;
        return isValidSql;
    }

    /**
     * Gets sanitized query.
     *
     * @return the sanitized query
     */

    public String getSanitizedQuery() {
        return this.sanitizedQuery;
    }

    /**
     * Sets sanitized query.
     *
     * @param sanitizedQuery the sanitized query
     */

    private void setSanitizedQuery(String sanitizedQuery) {
        this.sanitizedQuery = sanitizedQuery.toLowerCase();
    }
}
