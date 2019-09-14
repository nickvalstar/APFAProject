package com.damirvandic.sparker.msm;

public interface KeyMatcher {
    boolean keyMatches(String shopA, String keyA, String shopB, String keyB);

    double keySimilarity(String shopA, String keyA, String shopB, String keyB);
}
