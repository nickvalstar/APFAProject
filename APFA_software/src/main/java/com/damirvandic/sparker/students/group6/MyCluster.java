package com.damirvandic.sparker.students.group6;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

/**
 * Created by Gerhard on 3-11-2014.
 */
public class MyCluster {

    private Set<Vector> vectors;
    private Map<Integer, Double> clusterDistances;

    public MyCluster() {
        vectors = new HashSet<Vector>();
        clusterDistances = new HashMap<Integer, Double>();
    }

    public void addVector(Vector toAdd) {
        vectors.add(toAdd);
    }

    public void addDistance(double distance, int clusterIndex) {
        clusterDistances.put(clusterIndex, distance);
    }

    public void removeDistance(Integer index) {
        clusterDistances.remove(index);
    }

    public Map<Integer, Double> getMapDist() {
        return clusterDistances;
    }

    public Set<Vector> getVectors() {
        return vectors;
    }


}