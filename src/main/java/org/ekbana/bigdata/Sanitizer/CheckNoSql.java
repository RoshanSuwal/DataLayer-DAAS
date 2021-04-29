package org.ekbana.bigdata.Sanitizer;

import org.apache.log4j.Logger;

import java.util.Arrays;

/**
 * The type Check no sql.
 */
public class CheckNoSql {

    private static final String[] STOP_WORDS_MONGO = {"db.dropDatabase", "db.repairDatabase","db.shutdownServer",
            "db.createCollection","db.collection.dropIndex","db.adminCommand","db.createUser","db.dropUser","db.dropAllUser",
            "db.removeUser","db.updateUser"};

    private static final Logger logger = Logger.getLogger(CheckNoSql.class);

    private String sanitizedQuery = "";

    public CheckNoSql(String query) {
        setSanitizedQuery(query);
    }

    /*
      * Checks if the mongo sql provided by user is a valid mongo sql
     * @return      Boolean
     */
    public Boolean isValidMongoSql(){
        logger.info("validating mongo sql");
        Boolean isValidSql = true;
        if (isMalFormedMongoSql())
            isValidSql = false;
        return isValidSql;
    }

    /**
     * @return      Boolean
     */
    private Boolean containsMongoStopWords(){
        return Arrays.stream(STOP_WORDS_MONGO).parallel().anyMatch(getSanitizedQuery()::contains);
    }

    /**
     * @return      Boolean
     */
    private Boolean isNotFirstWord(){
        return !getSanitizedQuery().split("\\.")[0].toLowerCase().trim().equals("db");
    }

    /**
     * @return      Boolean
     */
    private Boolean isMalFormedMongoSql(){
        return (containsMongoStopWords() || isNotFirstWord());
    }

    /**
     * @return      String
     */
    private String getSanitizedQuery() {
        return this.sanitizedQuery;
    }


    /**
     * @param sanitizedQuery
     */
    private void setSanitizedQuery(String sanitizedQuery) {
        this.sanitizedQuery = sanitizedQuery.toLowerCase();
    }

}
