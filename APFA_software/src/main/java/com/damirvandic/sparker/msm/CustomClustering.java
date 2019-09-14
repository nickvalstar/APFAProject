package com.damirvandic.sparker.msm;

import com.damirvandic.sparker.core.ProductDesc;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

public class CustomClustering {
    private Map<Integer, ProductDesc> products;
    private double[][] ds;
    private double threshold;
    private Map<Integer, Set<ProductDesc>> prodsets = new HashMap<>();
    private Set<Set<ProductDesc>> clusters = new HashSet<>();

    // Input: 	Matrix of dissimilarities, threshold,
    //			map of integer and entities (integers 0, 1, ..., nrProducts-1 corresponding with dissimilarities matrix indices)
    public CustomClustering(Map<Integer, ProductDesc> products, double[][] dissimilarities, double threshold) {
        this.products = products;
        this.ds = dissimilarities;
        this.threshold = threshold;
    }

    // Create clusters
    public void createClusters() {
        clusters.clear();
        // First all clusters consists of one product
        for (int i = 0; i < products.size(); i++) {
            prodsets.put(i, new HashSet<ProductDesc>());
            prodsets.get(i).add(products.get(i));
        }
        int[] twoClosestClusters = getTwoClosestClusters(ds);
        double min;
        // Now the closest clusters are merged (single linkage)
        while (twoClosestClusters[0] != twoClosestClusters[1] && ds[twoClosestClusters[0]][twoClosestClusters[1]] <= threshold) {
            // Adjust dissimilarities
            for (int j = 0; j < ds.length; j++) {
                min = Math.min(ds[twoClosestClusters[0]][j], ds[twoClosestClusters[1]][j]);
                if (ds[twoClosestClusters[0]][j] + ds[twoClosestClusters[1]][j] == Double.POSITIVE_INFINITY) {
                    ds[twoClosestClusters[0]][j] = Double.POSITIVE_INFINITY;
                    ds[j][twoClosestClusters[0]] = Double.POSITIVE_INFINITY;
                } else {
                    ds[twoClosestClusters[0]][j] = min;
                    ds[j][twoClosestClusters[0]] = min;
                }
                ds[twoClosestClusters[1]][j] = Double.POSITIVE_INFINITY;
                ds[j][twoClosestClusters[1]] = Double.POSITIVE_INFINITY;
            }
            // Merge two closest clusters into one cluster
            prodsets.get(twoClosestClusters[0]).addAll(prodsets.get(twoClosestClusters[1]));
            prodsets.remove(twoClosestClusters[1]);
            twoClosestClusters = getTwoClosestClusters(ds);
        }
        clusters.addAll(prodsets.values());
    }

    public Set<Set<ProductDesc>> getClusters() {
        return clusters;
    }

    public double getThreshold() {
        return threshold;
    }

    public void setThreshold(double newThreshold) {
        threshold = newThreshold;
    }

    // Find the indices of the two closest clusters
    private int[] getTwoClosestClusters(double[][] ds) {
        double minValue = Double.POSITIVE_INFINITY;
        int[] twoClosestClusters = {0, 0};
        for (int i = 0; i < ds.length - 1; i++) {
            for (int j = i + 1; j < ds.length; j++) {
                if (ds[i][j] < minValue) {
                    minValue = ds[i][j];
                    twoClosestClusters[0] = i;
                    twoClosestClusters[1] = j;
                }
            }
        }
        return twoClosestClusters;
    }
}
