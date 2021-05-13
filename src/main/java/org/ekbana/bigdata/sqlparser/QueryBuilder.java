package org.ekbana.bigdata.sqlparser;

import org.apache.log4j.Logger;
import org.ekbana.bigdata.dbmanagement.GetFromProperty;
import org.json.JSONException;
import org.json.JSONObject;

import java.io.IOException;
import java.util.*;

public class QueryBuilder {

    /**
     * @variable isvalid of type boolean
     * set true by default
     * set to false if alias name do not match and table name is empty
     * set to false if error in json string parsing
     * set to false if @variable in query do not match with key on json
     */
    boolean isvalid = true;
    String[] operators = {"<=", ">=", "!=", "<", ">", "="};

    String query;
    String values;
    String[] tokens;

    Logger LOGGER = Logger.getLogger(QueryBuilder.class);


    public QueryBuilder(String query, String values){
        this.query = query;
        this.values = values;
        this.tokens=buildQuery(query);
    }

    public boolean isIsvalid() {
        return isvalid;
    }

    public void setIsvalid(boolean isvalid) {
        this.isvalid = isvalid;
    }

    public String[] getTokenizedQuery(){
        return this.tokens;
    }


    /**
     * @method buildQuery
     * this method replaces the variables in query with corresponding json values
     * return String is final query
     * **/
    private String[] buildQuery(String query) {
        LOGGER.info("[QueryBuilder] initialization");
        String[] tokens=tokenization(query);
        if (isvalid) {
            try {
                JSONObject jsonObject = new JSONObject(this.values);

                for (int i = 0; i < tokens.length; i++) {
                    //check is it variable or not => $variable
                    if (tokens[i].charAt(0) == '$') {
                        tokens[i] = getJsonData(jsonObject, tokens[i].replace("$", ""));
                        if (tokens[i].equals("") || tokens[i].equals("''")) {
                            LOGGER.error("[QUERY Builder] [JSON] Null key-value pair");
                            this.setIsvalid(false);
                            return tokens;
                            //return this.query;
                        }
                    }
                }

            } catch (JSONException e) {
                for (String token : tokens) {
                    if (token.charAt(0) == '$') {
                        LOGGER.error("[QUERY BUILDER] [JSON] " + e.getMessage());
                        this.setIsvalid(false);
                        break;
                    }
                }
            }
        }

        return tokens;
        //return String.join(" ", tokens);
    }

    private boolean isInt(String str) {
        try {
            Integer.parseInt(str);
            return true;
        } catch (NumberFormatException e) {
            return false;
        }
    }

    private boolean isFloat(String str) {
        try {
            Float.parseFloat(str);
            return true;
        } catch (NumberFormatException e) {
            return false;
        }
    }

    /**
     * @method getJsonDate
     * @param jsonObject
     * @param key is String whose value is to be found in jsonObject
     * return String
     *
     * ***/
    private String getJsonData(JSONObject jsonObject, String key) {
        try {
            /**checks if the obtained value is number**/
            return String.valueOf(jsonObject.getNumber(key));
        } catch (JSONException e) {

            try { /** checks if the obtained value is String*/
                String value = jsonObject.getString(key);
                if (value.isEmpty()) {
                    return value;
                } else {
                    /**replaces "'" with "''" if present in @variable value , prevents from SQL injection**/
                    return "'" + value.replace("'", "''") + "'";
                }
            } catch (JSONException f) {
                try {
                    return jsonObject.getJSONObject(key).toString();
                } catch (JSONException h) {
                    try {
                        /** checks if the obtained value is list**/
                        List<Object> list = jsonObject.getJSONArray(key).toList();
                        ArrayList<String> arrayList = new ArrayList<>();
                        for (Object obj : list) {
                            if (isInt(obj.toString()) || isFloat(obj.toString())) {
                                arrayList.add(obj.toString());
                            } else {
                                arrayList.add("'" + obj.toString().replace("'", "''") + "'");
                            }
                        }
                        return String.join(",", arrayList);
                    } catch (JSONException i) {
                        LOGGER.error("[QUERY BUILDER][JSON][getJsonData]" + i.getMessage());
                        return "";
                    }

                }
            }
        }

    }

    /**
     * @method tokenization
     * @param query String is the query to be tokenized
     * this methods tokenizes the query and removes the unnecessary spaces obtained while tokenizing the query using regex space
     * returns String[]
     * ***/

    private String[] tokenization(String query) {
        query = query.replace(",", " , ")
                .replace("(", " ( ")
                .replace(")", " ) ")
                .replace("{", " { ")
                .replace("}", " } ")
                .replace("[", " [ ")
                .replace("]", " ] ")
                .replace("+", " + ")
                .replace("-", " - ");

        String[] tokens = query.split(" ");

        //remove unnecessary spaces between tokens
        ArrayList<String> normalizedToken = new ArrayList<>();

        for (String tok : tokens) {
            if (tok.length() > 0 && !tok.isEmpty()) {
                boolean hasNormalised = false;
                for (String str : operators) {
                    if (tok.contains(str)) {
                        hasNormalised = true;
                        Arrays.stream(tok.replace(str, " " + str + " ").split(" ")).forEach(
                                t -> {
                                    if (!t.isEmpty()) {
                                        normalizedToken.add(t);
                                    }
                                }
                        );
                        break;
                    }
                }
                if (!hasNormalised) {
                    normalizedToken.add(tok);
                }
            }
        }

        tokens= new String[normalizedToken.size()];

        for (int i = 0; i < normalizedToken.size(); i++) {
            tokens[i] = normalizedToken.get(i);
        }

        return tokens;
    }
}
