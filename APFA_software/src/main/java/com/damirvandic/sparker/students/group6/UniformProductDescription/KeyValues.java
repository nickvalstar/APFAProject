package com.damirvandic.sparker.students.group6.UniformProductDescription;

import com.damirvandic.sparker.core.ProductDesc;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

/**
 * Created by wkuipers on 03-10-14.
 */
public class KeyValues {
    private HashMap<String, HashMap<String, Integer>> keyValues;
    private HashMap<String, Set<String>> keyShop;

    public KeyValues(Set<ProductDesc> products) {
        keyValues = new HashMap<>();
        keyShop = new HashMap<>();

        boolean keyIsNew, valueIsNew;
        Integer oldCount;
        HashMap<String, Integer> values;
        HashSet<String> shop;

        String key, value;

        // initialize keyValues and frequencies
        for (ProductDesc p : products) {
            for (Map.Entry<String, String> kv : p.featuresMap.entrySet()) {
                key = kv.getKey();
                value = kv.getValue();

                key = cleanKey(key);
                value = cleanValue(value);

                keyIsNew = !keyValues.containsKey(key);
                if (keyIsNew) {
                    values = new HashMap<String, Integer>();
                    values.put(value, 1);
                    keyValues.put(key, values);

                    shop = new HashSet<String>();
                    shop.add(p.shop);
                    keyShop.put(key, shop);
                } else // key exists
                {
                    // check if value exists
                    valueIsNew = !keyValues.get(key).containsKey(value);
                    keyShop.get(key).add(p.shop);

                    if (valueIsNew) {
                        keyValues.get(key).put(value, 1);
                    } else // value exists
                    {
                        oldCount = keyValues.get(key).get(value);

                        keyValues.get(key).put(value, oldCount + 1);
                    }
                }
            }
        }
    }

    public HashMap<String, Set<String>> getKeyShop() {
        return keyShop;
    }

    public int getKeyFreq(String key) {
        Integer sum = new Integer(0);

        for (Map.Entry<String, Integer> e : keyValues.get(key).entrySet()) {
            sum = sum + e.getValue();
        }
        return sum;
    }

    public int getValueFreq(String key, String value) {
        return keyValues.get(key).get(value);
    }

    public Set<String> getKeys() {
        Set<String> a = new HashSet<String>();
        for (Map.Entry<String, HashMap<String, Integer>> e : keyValues.entrySet()) {
            a.add(e.getKey());
        }
        return a;
    }

    public HashMap<String, Integer> getValues(String key) {
        return keyValues.get(key);
    }

    public HashMap<String, Integer> getKeyWordCount(Set<ProductDesc> products) {
        HashMap<String, Integer> result = new HashMap<String, Integer>();
        Set<String> keys = getKeys();
        String key;
        String[] words;
        Integer oldCount;

        for (ProductDesc p : products) {
            for (Map.Entry<String, String> kv : p.featuresMap.entrySet()) {
                key = kv.getKey();
                words = key.toLowerCase().split(" ");

                for (String w : words) {
                    if (!result.containsKey(w)) {
                        result.put(w, 1);
                    } else {
                        oldCount = result.get(w);
                        result.put(w, oldCount + 1);
                    }
                }
            }
        }
        return result;
    }

    /**
     * Remove brackets and text between, remove ':'
     */
    public static String cleanKey(String k) {
        k = k.replaceAll("\\(.*?\\)", "");
        k = k.replaceAll(":", "");
        k = k.trim().toLowerCase();

        return k;
    }


    /**
     * Remove brackets and text between
     */
    public static String cleanValue(String k) {
        k = k.replaceAll("\\(.*?\\)", "");
        k = k.trim().toLowerCase();

        return k;
    }
}
