package com.damirvandic.sparker.students.group6.LSH;

/**
 * Created by wkuipers on 01-11-14.
 */
public class HashFunction {

    // concatenate all hashvalues as hash
    public static String concatenate(Signature sig, int from, int to) {
        String result = "";
        for (int i = from; i <= to; i++) {
            result = result + sig.get(i);
        }
        return result;
    }

    // for example cosine hashfunction
    public static String cosine(Signature sig, int from, int to) {
        return 0 + "";
    }
}
