package org.ekbana.bigdata.sqlparser;

import java.util.Arrays;

public class QueryParser {

    String[] characters = new String[]{",", "'", "(", ")", "{", "}", ":"};

    public String parseQuery(String query) {
        for (String str : characters) {
            query = query.replace(str, " " + str + " ");
        }
        return operatorParsing(getTokenizedQuery(query));
    }

    /**
     * tokenize the query using regex space
     * remove space from the start and end of the query
     *
     * **/

    public String[] getTokenizedQuery(String query) {
        String[] tokens = query.split(" ");

        int starting_offset = 0;
        int ending_offset=tokens.length;

        for (int i = 0; i < tokens.length; i++) {
            if (!tokens[i].isBlank()) {
                starting_offset= i;
                break;
            }
        }

        for (int i=tokens.length-1;i>=0;i--){
            if (!tokens[i].isBlank()){
                ending_offset=i+1;
                break;
            }
        }
        return Arrays.copyOfRange(tokens, starting_offset, ending_offset);
    }

    private String operatorParsing(String[] tokens) {
        String str = "";

        for (String token : tokens) {
            if (token.contains("<=")) {
                str = str + " " + token.replace("<=", " <= ");
            } else if (token.contains(">=")) {
                str = str + " " + token.replace(">=", " >= ");
            } else if (token.contains("==")) {
                str = str + " " + token.replace("==", " == ");
            } else if (token.contains("<")) {
                str = str + " " + token.replace("<", " < ");
            } else if (token.contains(">")) {
                str = str + " " + token.replace(">", " > ");
            } else if (token.contains("=")) {
                str = str + " " + token.replace("=", " = ");
            } else {
                str = str + " " + token;
            }
        }
        return str;
    }

}
