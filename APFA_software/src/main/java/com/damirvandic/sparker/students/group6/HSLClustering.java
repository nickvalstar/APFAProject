package com.damirvandic.sparker.students.group6;

import com.damirvandic.sparker.core.ProductDesc;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

/**
 * Created by Gerhard on 27-9-2014.
 */
public class HSLClustering {

    private static double treshold;
    private static int nrWebshops;
    private static ArrayList<Vector> allVec;
    private static Map<Vector, Set<Vector>> closestNeighbors;
    private static ArrayList<Integer> indMissing;
    private static VecDist vecDist;
    private static int linkage; //1: single 2: average 3:complete

    public HSLClustering(Map<Vector, Set<Vector>> closest, double treshold, int nrWebshops, ArrayList<Vector> allVectors, ArrayList<Integer> indicesMissingValues, int linkage) {
        this.closestNeighbors = closest;
        this.treshold = treshold;
        this.nrWebshops = nrWebshops;
        this.allVec = allVectors;
        this.indMissing = indicesMissingValues;
        this.vecDist = new VecDist(closestNeighbors);
        this.linkage = linkage;
    }

    public Set<Set<ProductDesc>> performClustering() {
        ClusterMap mapping = initializeClusters();
        while (mapping.getIndicesClustersToMerge().get(0) != -1) {
            mapping.merge(mapping.getIndicesClustersToMerge().get(0), mapping.getIndicesClustersToMerge().get(1), vecDist, indMissing, treshold, nrWebshops, linkage);
        }

        return mapping.getClusters();
    }

    public ClusterMap initializeClusters() {
        ClusterMap mapping = new ClusterMap(allVec, vecDist, indMissing, treshold);
        return mapping;
    }

    public static Set<Vector> hardcopyVectorArray(ArrayList<Vector> toCopy) {
        Set<Vector> copy = new HashSet<Vector>();
        for (int i = 0; i < toCopy.size(); i++) {
            copy.add(toCopy.get(i));
        }
        return copy;
    }


}