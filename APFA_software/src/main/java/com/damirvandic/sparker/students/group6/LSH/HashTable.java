package com.damirvandic.sparker.students.group6.LSH;

import com.damirvandic.sparker.students.group6.Vector;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

/**
 * Created by wkuipers on 01-11-14.
 */
public class HashTable {
    private Map<String, Set<Vector>> buckets;

    public HashTable() {
        buckets = new HashMap<String, Set<Vector>>();
    }

    public void newBucket(String key, Vector v) {
        HashSet<Vector> set = new HashSet<>();
        set.add(v);
        buckets.put(key, set);
    }

    public void addVector(String bucketKey, Vector v) {
        buckets.get(bucketKey).add(v);
    }

    public boolean hasBucket(String key) {
        return buckets.containsKey(key);
    }

    public Map<String, Set<Vector>> getBuckets() {
        return buckets;
    }
}
