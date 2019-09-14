package com.damirvandic.sparker.students.group6.UniformProductDescription;

import java.util.HashMap;
import java.util.Map;
import java.util.Set;

/**
 * Created by wkuipers on 07-10-14.
 */
public class KeyMapper {
    private KeyValues keyValues;
    private int nrOfKeys;
    private double alpha;

    private HashMap<String, Map<String, Double>> mapping;
    private double[][] keySimilarities;

    public KeyMapper(KeyValues kv, double a) {
        keyValues = kv;
        nrOfKeys = keyValues.getKeys().size();

        alpha = a;
    }

    public KeyValues getKeyValues() {
        return keyValues;
    }

    public double getAlpha() {
        return alpha;
    }

    public HashMap<String, Map<String, Double>> getMapping() {
        return mapping;
    }

    public void mapKeys() {
        Set<String> keys = keyValues.getKeys();

        HashMap<String, Map<String, Double>> mapping = new HashMap<>();
        double similarity;

        for (String s : keys) {
            mapping.put(s, new HashMap<String, Double>());
            for (String t : keys) {
                // keys are from same shop
                if (keyValues.getKeyShop().get(s).equals(keyValues.getKeyShop().get(t)))
                    continue;

                if (!s.equals(t)) {
                    similarity = Levenshtein.getSimilarity(s, t);
                    if (similarity > alpha) // keys are sufficient similar
                    {
                        mapping.get(s).put(t, similarity);
                    }
                }
            }
        }
    }
}
