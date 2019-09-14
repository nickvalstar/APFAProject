package com.damirvandic.sparker.students.group6.UniformProductDescription;

import com.damirvandic.sparker.core.ProductDesc;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Set;

/**
 * Created by wkuipers on 25-09-14.
 */
public class UniformDescriptions {
    private final int MINIMUMCLUSTERSIZE; // more than products in largest website
    private final int MAXVALUESINCLUSTER;


    private Set<ProductDesc> products;
    private KeyValues keyValues;
    private TypeClassifier typeClassifier;
    private double alpha, beta, gamma;

    private KeyClusters keyClusters;
    private HashMap<String, HashMap<String, Double>> keyDistances;

    public UniformDescriptions(Set<ProductDesc> p, double a, double b, double c) {
        products = p;
        alpha = a;
        beta = b;
        gamma = c;

        MINIMUMCLUSTERSIZE = (int) (products.size() * 0.0); // is arbitrary
        MAXVALUESINCLUSTER = 300; // arbitrary

        keyValues = new KeyValues(products);

        typeClassifier = new TypeClassifier(keyValues, beta);

        keyClusters = new KeyClusters(keyValues.getKeys(), this, alpha, gamma);
        keyClusters.removeInsignificantClusters(MINIMUMCLUSTERSIZE, MAXVALUESINCLUSTER);
    }


    public KeyValues getKeyValues() {
        return keyValues;
    }


    public double getKeyDistance(String key1, String key2) {
        return keyClusters.getKeyDistance(key1, key2);
    }

    public static double findMax(double[][] A) {
        double max = Double.NEGATIVE_INFINITY;

        for (int i = 0; i < A.length; i++) {
            for (int j = 0; j < A[0].length; j++) {
                if (A[i][j] > max) {
                    max = A[i][j];
                }
            }
        }
        return max;
    }

    public void printKeyClusters() {
        ArrayList<ArrayList<String>> kC = keyClusters.getKeyClusters();
        System.out.println("KEYCLUSTERS: ");

        for (ArrayList<String> cluster : kC) {
            if (totalKeysInCluster(cluster) < MINIMUMCLUSTERSIZE)
                continue;

            for (String key : cluster) {
                System.out.print(key + " | ");
            }
            System.out.print(totalKeysInCluster(cluster));
            System.out.println("\n");
        }
    }

    public int totalKeysInCluster(ArrayList<String> cluster) {
        int sum = 0;
        for (String key : cluster) {
            sum = sum + keyValues.getKeyFreq(key);
        }
        return sum;
    }

    public ArrayList<ArrayList<String>> getKeyClusters() {
        return keyClusters.getKeyClusters();
    }

    public void printTypeClassification() {
        typeClassifier.printClassification();
    }

    public TypeClassifier getTypeClassifier() {
        return typeClassifier;
    }


}