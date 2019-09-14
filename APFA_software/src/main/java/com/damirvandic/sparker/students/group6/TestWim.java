package com.damirvandic.sparker.students.group6;

import com.damirvandic.sparker.core.Clusters;
import com.damirvandic.sparker.core.ProductDesc;
import com.damirvandic.sparker.data.Reader;
import com.damirvandic.sparker.students.group6.LSH.LocalitySensitiveHash;
import com.damirvandic.sparker.students.group6.LSH.MinHash;
import com.google.common.collect.Sets;

import java.util.*;

/**
 * Created by wkuipers on 07-11-14.
 */
public class TestWim {
    public static void main(String[] args) {
        // parameters
        double t = 0.05;
        double h = 0.1;

        Set<ProductDesc> productDescSet = getData();
        System.out.println("number of products: " + productDescSet.size());

        long t1;
        LSHVectorsFromModelWords vectorBuilder = new LSHVectorsFromModelWords(productDescSet);
        ArrayList<Vector> vectors = vectorBuilder.retrieveVectors();
        System.out.println("number of vectors: " + vectors.size());

        int vectorLength = vectors.get(0).getDimensions();
        System.out.println("vectorLength: " + vectorLength);

        int minHashSize = (int) h * vectorLength;

        MinHash minHash = new MinHash(minHashSize, vectorLength);
        minHash.hash(vectors);

        LocalitySensitiveHash lsh = new LocalitySensitiveHash(vectors, t);
        Map<Vector, Set<Vector>> neighbors = lsh.performLSH();

        double lshPerformance = evaluateLSH(neighbors, productDescSet);

        System.out.println("lshPerformance: " + lshPerformance);
    }

    public static Set<ProductDesc> getData() {
        String dsName = "TV's";
        String dsPath = "./hdfs/TVs-all-merged.json";

        Clusters clusters = Reader.readDataSet(dsName, dsPath).clusters();

        Set<ProductDesc> data = clusters.javaDescriptions();

        return data;
    }

    public static void print(Set<String> set) {
        for (String o : set) {
            System.out.println(o);
        }
    }

    public static double evaluateLSH(Map<Vector, Set<Vector>> neighbors, Set<ProductDesc> data) {
        Map<String, Set<ProductDesc>> goldenStandard = new HashMap<>();

        Set<ProductDesc> set;
        for (ProductDesc p : data) {
            if (goldenStandard.containsKey(p.modelID)) {
                goldenStandard.get(p.modelID).add(p);
            } else {
                set = new HashSet<ProductDesc>();
                set.add(p);
                goldenStandard.put(p.modelID, set);
            }
        }

        System.out.println("unique products: " + goldenStandard.size());


        Map<ProductDesc, ProductDesc> duplicates = new HashMap<>();
        Set<Set<ProductDesc>> powerSet;
        Object[] array;
        int duplicatesSum = 0;

        for (Map.Entry<String, Set<ProductDesc>> e : goldenStandard.entrySet()) {
            // set contains duplicates if larger than 1
            if (e.getValue().size() > 1) {
                // print modelID (key) and the size of set
                System.out.println(e.getKey() + ": " + e.getValue().size());

                duplicatesSum += e.getValue().size() * (e.getValue().size() - 1) / 2;

                powerSet = Sets.powerSet(e.getValue());

                // add products to duplicates
                for (Set<ProductDesc> pair : powerSet) {
                    if (pair.size() == 2) {
                        array = pair.toArray();
                    }
                }
            }
        }
        System.out.println("duplicate count: " + duplicatesSum);
        System.out.println("duplicates size: " + duplicates.size());

        int count = 0;


        System.out.println(count + " / " + duplicatesSum);
        return ((double) count) / duplicatesSum;
    }
}
