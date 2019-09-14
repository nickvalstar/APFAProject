package com.damirvandic.sparker.students.group6;

import java.util.HashSet;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Created by wkuipers on 07-11-14.
 */
public class ModelWordSplitter {
    private String numReg = "(([0-9],*)+(?:(\\.\\d+)|(-\\d+/\\d))?)";
    private String alphaNumReg = "[0-9a-zA-Z]+";
    private String combinedReg = "[0-9a-zA-Z\".-]+";

    private String mwReg = "([a-zA-Z0-9]*(([0-9]+[^0-9^,^ ]+)|([^0-9^,^ ]+[0-9]+))[a-zA-Z0-9]*)";

    private boolean numeric, alphanumeric, combined;


    Pattern numRegex, alphaNumRegex, combinedRegex, mwRegex;

    public ModelWordSplitter(boolean numeric, boolean alphanumeric, boolean combined) {
        this.numeric = numeric;
        this.alphanumeric = alphanumeric;
        this.combined = combined;

        numRegex = Pattern.compile(numReg);
        alphaNumRegex = Pattern.compile(alphaNumReg);
        combinedRegex = Pattern.compile(combinedReg);

        mwRegex = Pattern.compile(mwReg);
    }

    public Set<String> getModelWords(String string) {
        Set<String> result = new HashSet<String>();
        // testing for combined
        Matcher m = mwRegex.matcher(string);

        boolean modelWordFound = true;
        String word;

        while (modelWordFound) {
            modelWordFound = false;

            if (m.find()) {
                modelWordFound = true;
                word = m.group();

                // only add words having more than
                if (word.length() > 1) {
                    result.add(m.group());
                }
            }
        }
        return result;
    }
}
