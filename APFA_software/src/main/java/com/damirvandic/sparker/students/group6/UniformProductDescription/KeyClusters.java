package com.damirvandic.sparker.students.group6.UniformProductDescription;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Set;

/**
 * Created by wkuipers on 14-10-14.
 */
public class KeyClusters {
    private ArrayList<ArrayList<String>> keyClusters;
    private double[][] keyClusterSimilarities;
    private UniformDescriptions uniform;
    private Set<String> keys;
    private HashMap<String, HashMap<String, Double>> keyDistances;
    private double gamma, alpha;

    public KeyClusters(Set<String> k, UniformDescriptions u, double a, double c) {
        keyClusters = new ArrayList<>();

        uniform = u;
        keys = k;
        alpha = a;
        gamma = c;

        for (String key : keys) {
            // eliminate universal keys in initial clusters
            if (!uniform.getTypeClassifier().getClassification(key).equals("u")) {
                keyClusters.add(0, new ArrayList<String>());
                keyClusters.get(0).add(key);
            }
        }

        keyDistances = initializeKeyDistances();
        keyClusterSimilarities = computeClusterSimilarities();

        double highestKeyClusterSimilarity = uniform.findMax(keyClusterSimilarities);

        // merge similar clusters until highest similarity is below threshold gamma
        while (highestKeyClusterSimilarity > gamma) {
            //System.out.println(highestKeyClusterSimilarity);
            mergeSimilarKeyClusters();
            highestKeyClusterSimilarity = findMax(keyClusterSimilarities);
        }
    }

    public ArrayList<ArrayList<String>> getKeyClusters() {
        return keyClusters;
    }

    public void mergeSimilarKeyClusters() {
        keyClusterSimilarities = computeClusterSimilarities();
        if (keyClusters.size() != keyClusterSimilarities.length) {
            System.out.println("error");
        }

        int n = keyClusterSimilarities.length;
        int maxI = 0, maxJ = 1;
        double max = 0;

        for (int i = 0; i < n; i++) {
            for (int j = i + 1; j < n; j++) {
                if (keyClusterSimilarities[i][j] > max) {
                    max = keyClusterSimilarities[i][j];
                    maxI = i;
                    maxJ = j;
                }
            }
        }

        if (max < gamma) {
            return;
        }

        //System.out.print(keyClusters.get(maxI));
        //System.out.println(keyClusters.get(maxJ));


        ArrayList<String> keyCluster2 = keyClusters.get(maxJ);

        for (String key : keyCluster2) {
            keyClusters.get(maxI).add(key);
        }

        keyClusters.remove(maxJ);
    }

    public int totalKeysInCluster(ArrayList<String> cluster) {
        int sum = 0;
        for (String key : cluster) {
            sum = sum + uniform.getKeyValues().getKeyFreq(key);
        }
        return sum;
    }

    private double[][] computeClusterSimilarities() {
        int n = keyClusters.size();
        double[][] result = new double[n][n];

        double minDist = 0, dist;
        ArrayList<String> cluster1, cluster2;


        for (int i = 0; i < n; i++) {
            for (int j = i + 1; j < n; j++) {
                minDist = 1;
                cluster1 = keyClusters.get(i);
                cluster2 = keyClusters.get(j);
                // determine distance between cluster i and j
                for (int c1 = 0; c1 < cluster1.size(); c1++) {
                    for (int c2 = 0; c2 < cluster2.size(); c2++) {
                        dist = getKeyDistance(cluster1.get(c1), cluster2.get(c2));
                        if (dist < minDist) {
                            minDist = dist;
                        }
                    }
                }
                result[i][j] = minDist;
                result[j][i] = minDist;
            }
        }
        return result;
    }

    private HashMap<String, HashMap<String, Double>> initializeKeyDistances() {
        HashMap<String, HashMap<String, Double>> result = new HashMap<String, HashMap<String, Double>>();

        for (String key1 : keys) {
            result.put(key1, new HashMap<String, Double>());

            for (String key2 : keys) {
                result.get(key1).put(key2, calculateKeyDistance(key1, key2));
            }
        }
        return result;
    }

    private double calculateKeyDistance(String key1, String key2) {
        String key1DataType = uniform.getTypeClassifier().getDataType(key1);
        String key2DataType = uniform.getTypeClassifier().getDataType(key2);

        for (String shop1 : uniform.getKeyValues().getKeyShop().get(key1)) {
            for (String shop2 : uniform.getKeyValues().getKeyShop().get(key2)) {
                if (shop1.equals(shop2)) {
                    return 0.0;
                }
            }
        }

        if (key1DataType.equals(key2DataType)) {
            return alpha * Levenshtein.getSimilarity(key1, key2) + (1 - alpha);

        } else {
            return alpha * Levenshtein.getSimilarity(key1, key2);

        }
    }

    public double getKeyDistance(String key1, String key2) {
        return keyDistances.get(key1).get(key2);
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

    private void print(double[][] toPrint) {
        for (int i = 0; i < toPrint.length; i++) {
            System.out.print(i + "| ");
            for (int j = 0; j < toPrint[0].length; j++) {
                System.out.printf("%5.2f", toPrint[i][j]);
            }
            System.out.println();
        }
    }

    public int shopsInCluster(ArrayList<String> keyCluster) {
        KeyValues kvs = uniform.getKeyValues();
        HashMap<String, Set<String>> keyShop = kvs.getKeyShop();

        Set<String> shops = new HashSet<>();

        for (String key : keyCluster) {
            for (String shop : keyShop.get(key)) {
                shops.add(shop);
            }
            if (shops.size() > 1) {
                // 2 indicates 2 or more.
                return 2;
            }
        }

        return shops.size();
    }

    public int valuesInCluster(ArrayList<String> keyCluster) {
        Set<String> values = new HashSet<>();

        for (String key : keyCluster) {
            for (String value : uniform.getKeyValues().getValues(key).keySet()) {
                values.add(value);
            }
        }
        return values.size();
    }

    public void removeInsignificantClusters(int MINIMUMSIZE, int MAXVALUESINCLUSTER) {
        Set<ArrayList<String>> iC = new HashSet<>();

        for (ArrayList<String> cluster : keyClusters) {
            if (totalKeysInCluster(cluster) < MINIMUMSIZE) {
                iC.add(cluster);
                continue;
            }
            if (shopsInCluster(cluster) < 2) {
                iC.add(cluster);
                continue;
            }
            if (valuesInCluster(cluster) > MAXVALUESINCLUSTER) {
                iC.add(cluster);
            }

        }
        for (ArrayList<String> cluster : iC) {
            keyClusters.remove(cluster);
        }
    }
}
