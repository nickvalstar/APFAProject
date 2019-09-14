package com.damirvandic.sparker.students.group6.LSH;


import com.damirvandic.sparker.students.group6.Vector;

import java.util.*;

/**
 * Created by wkuipers on 31-10-14.
 */
public class LocalitySensitiveHash {
    private List<Vector> vectors;

    private int vectorLength, nrOfBands, nrOfRows;
    private double threshold;
    private HashTable[] hashTables;

    public LocalitySensitiveHash(List<Vector> vectors, double t) {
        this.vectors = vectors;
        vectorLength = vectors.get(0).getSignature().getLength();
        threshold = t;

        // initialize nrOfBands and nrOfRows (per band)
        int n = vectorLength;
        double minDeviation = Double.MAX_VALUE;
        int optimalB = 1;
        for (int b = 1; b <= n; b++) {
            t = ((double) 1) / b;
            t = Math.pow(t, ((double) b) / n);

            if (Math.abs(t - threshold) < minDeviation) {
                minDeviation = Math.abs(t - threshold);
                optimalB = b;
            }
        }

        nrOfBands = optimalB;
        nrOfRows = (int) Math.floor(((double) n) / nrOfBands);

        // initialize hashTables
        hashTables = new HashTable[nrOfBands];
        for (int i = 0; i < nrOfBands; i++) {
            hashTables[i] = new HashTable();
        }
    }

    public Map<Vector, Set<Vector>> performLSH() {
        List<Signature> M = getSignatures(vectors);
        if (!M.get(0).equals(vectors.get(0).getSignature())) {
            System.out.println("error");
        }

        int min, max;
        String hashValue;

        // for all hashtables
        for (int i = 0; i < nrOfBands; i++) {
            min = i * nrOfRows;
            max = min + nrOfRows - 1;

            // for all vectors assign to buckets in hashtable i
            for (Signature sig : M) {
                // HashFunction concatenate is used to make hashValues
                hashValue = HashFunction.concatenate(sig, min, max);

                // if hashValue occurs in table i
                if (hashTables[i].hasBucket(hashValue)) {
                    hashTables[i].addVector(hashValue, sig.getVector());
                } else // hashValue is new
                {
                    hashTables[i].newBucket(hashValue, sig.getVector());
                }
            }
        }


        // retrieve nearest neigbors for every vector
        Map<Vector, Set<Vector>> result = new HashMap<>();
        Set<Vector> set;

        for (Vector v1 : vectors) {
            set = new HashSet<>();
            for (HashTable table : hashTables) {
                for (Map.Entry<String, Set<Vector>> bucket : table.getBuckets().entrySet()) {
                    // if vector is in bucket
                    if (bucket.getValue().contains(v1)) {
                        for (Vector v2 : bucket.getValue()) {
                            set.add(v2);
                        }
                    }
                }
            }
            result.put(v1, set);
        }

        return result;
    }

    private ArrayList<Signature> getSignatures(List<Vector> vectors) {
        ArrayList<Signature> sigs = new ArrayList<>();

        for (Vector v : vectors) {
            sigs.add(v.getSignature());
        }
        return sigs;
    }
}
