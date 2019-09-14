package com.damirvandic.sparker.msm;

import com.damirvandic.sparker.util.StringPair;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import uk.ac.shef.wit.simmetrics.similaritymetrics.QGramsDistance;

import java.util.Locale;

public class MsmKeyMatcher implements KeyMatcher {
    public static final int MAX_CACHE_SIZE = 100000;
    private final LoadingCache<StringPair, Float> cache;
    private final QGramsDistance qGrammer = new QGramsDistance();
    private final double gamma;
    private int counter;

    public MsmKeyMatcher(double gamma) {
        this.counter = 0;
        this.gamma = gamma;
        this.cache = CacheBuilder.newBuilder()
                .maximumSize(MAX_CACHE_SIZE)
                .build(new CacheLoader<StringPair, Float>() {
                    @Override
                    public Float load(StringPair key) {
                        String keyA = key.id_a;
                        String keyB = key.id_b;
                        return qGrammer.getSimilarity(preProcess(keyA), preProcess(keyB));
                    }
                });
    }

    @Override
    public boolean keyMatches(String shopA, String keyA, String shopB, String keyB) {
        return keySimilarity(shopA, keyA, shopB, keyB) > gamma;
    }

    @Override
    public double keySimilarity(String shopA, String keyA, String shopB, String keyB) {
        float ret = cache.getUnchecked(new StringPair(keyA, keyB));
        if (++counter % 10000 == 0) {
            cache.cleanUp();
            counter = 0;
        }
        return ret;
    }

    private String preProcess(String str) {
        str = str.replaceAll("\\, |\\. |\\.$|\\(|\\)", " ");    //remove comma and period at end of word (or period at end of whole string)
        return str.toLowerCase(Locale.ENGLISH);
    }

    @Override
    public String toString() {
        return "MsmKeyMatcher";
    }
}
