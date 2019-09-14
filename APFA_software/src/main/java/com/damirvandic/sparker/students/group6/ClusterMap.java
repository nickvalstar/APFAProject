package com.damirvandic.sparker.students.group6;

import com.damirvandic.sparker.core.ProductDesc;

import java.util.*;

/**
 * Created by Gerhard on 3-11-2014.
 */
public class ClusterMap {

    private static Map<Integer, MyCluster> mapping;
    private static int highestIndex;

    public ClusterMap(ArrayList<Vector> allVec, VecDist vecDist, ArrayList<Integer> missingIndices, double treshold) {
        //initialize the cluster map
        mapping = new HashMap<Integer, MyCluster>();
        for (int i = 0; i < allVec.size(); i++) {
            MyCluster toAdd = new MyCluster();
            toAdd.addVector(allVec.get(i));
            mapping.put(i, toAdd);
        }

        //add all finite cluster distances
        for (int i = 0; i < allVec.size() - 1; i++) {
            for (int j = i + 1; j < allVec.size(); j++) {
                double dist = vecDist.dist(allVec.get(i), allVec.get(j));
                if (dist != Double.POSITIVE_INFINITY && dist <= treshold && allVec.get(i).getWebshop() != allVec.get(j).getWebshop()) {
                    mapping.get(i).addDistance(dist, j);
                    mapping.get(j).addDistance(dist, i);
                }
            }
        }

        highestIndex = allVec.size() - 1;
    }

    public ArrayList<Integer> getIndicesClustersToMerge() {
        double valueMin = Double.POSITIVE_INFINITY;
        int indexMin1 = -1;
        int indexMin2 = -1;
        for (Map.Entry<Integer, MyCluster> entry : mapping.entrySet()) {
            Map<Integer, Double> dist = entry.getValue().getMapDist();
            for (Map.Entry<Integer, Double> entry2 : dist.entrySet()) {
                if (entry2.getValue() < valueMin) {
                    valueMin = entry2.getValue();
                    indexMin1 = entry.getKey();
                    indexMin2 = entry2.getKey();
                }

            }

        }
        ArrayList<Integer> toReturn = new ArrayList<Integer>();
        toReturn.add(indexMin1);
        toReturn.add(indexMin2);
        return toReturn;
    }

    public void merge(Integer a, Integer b, VecDist vecDist, ArrayList<Integer> indMissing, double treshold, int nrWebshops, int linkage) {
        MyCluster first = mapping.get(a);
        MyCluster second = mapping.get(b);
        MyCluster nieuw = new MyCluster();
        Set<Vector> vecFirst = first.getVectors();
        Set<Vector> vecSecond = second.getVectors();
        for (Vector toAdd : vecFirst) {
            nieuw.addVector(toAdd);
        }
        for (Vector toAdd : vecSecond) {
            nieuw.addVector(toAdd);
        }
        mapping.remove(a);
        mapping.remove(b);
        //remove all irrelavant distances and add new Distance
        for (Map.Entry<Integer, MyCluster> entry : mapping.entrySet()) {
            entry.getValue().removeDistance(a);
            entry.getValue().removeDistance(b);
            double thisdist = -1;
            if (linkage == 1) {
                thisdist = ClusterDist.singleLinkage(nieuw, entry.getValue(), vecDist, indMissing, treshold, nrWebshops);
            }
            if (linkage == 2) {
                thisdist = ClusterDist.averageLinkage(nieuw, entry.getValue(), vecDist, indMissing, treshold, nrWebshops);
            }
            if (linkage == 3) {
                thisdist = ClusterDist.completeLinkage(nieuw, entry.getValue(), vecDist, indMissing, treshold, nrWebshops);
            }
            if (thisdist != Double.POSITIVE_INFINITY) {
                entry.getValue().addDistance(thisdist, highestIndex + 1);
                nieuw.addDistance(thisdist, entry.getKey());
            }
        }
        mapping.put(highestIndex + 1, nieuw);

        highestIndex += 1;

    }

    public Set<Set<ProductDesc>> getClusters() {
        Set<Set<ProductDesc>> allClusters = new HashSet<Set<ProductDesc>>();
        for (Map.Entry<Integer, MyCluster> entry : mapping.entrySet()) {
            Set<Vector> allVec = entry.getValue().getVectors();
            Set<ProductDesc> toAdd = new HashSet<ProductDesc>();
            for (Vector vec : allVec) {
                toAdd.add(vec.getProduct());
            }
            allClusters.add(toAdd);
        }
        return allClusters;
    }

    public int getSize() {
        return mapping.size();
    }


}