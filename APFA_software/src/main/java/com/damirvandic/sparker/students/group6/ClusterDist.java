package com.damirvandic.sparker.students.group6;

import java.util.ArrayList;
import java.util.Set;

/**
 * Created by Gerhard on 3-11-2014.
 */
public class ClusterDist {
    public static double singleLinkage(MyCluster a, MyCluster b, VecDist vecDist, ArrayList<Integer> indMissing, double treshold, int nrWebshops) {
        double dist = Double.POSITIVE_INFINITY;
        Set<Vector> setA = a.getVectors();
        Set<Vector> setB = b.getVectors();

        boolean sameWebshop = false;
        boolean neigh = false;
        for (Vector vecA : setA) {
            for (Vector vecB : setB) {
                if (vecA.getWebshop() == vecB.getWebshop()) {
                    sameWebshop = true;
                }
                double curDist = vecDist.dist(vecA, vecB);
                if (curDist == Double.POSITIVE_INFINITY) {
                    neigh = true;
                }
                if (curDist < dist) {
                    dist = curDist;
                }
            }
        }

        if (setA.size() + setB.size() > nrWebshops || dist > treshold || sameWebshop == true || neigh == true) {
            dist = Double.POSITIVE_INFINITY;
        }

        return dist;
    }

    public static double averageLinkage(MyCluster a, MyCluster b, VecDist vecDist, ArrayList<Integer> indMissing, double treshold, int nrWebshops) {
        Set<Vector> setA = a.getVectors();
        Set<Vector> setB = b.getVectors();
        double totaldist = 0;
        boolean inf = false;
        boolean sameWebshop = false;
        for (Vector vecA : setA) {
            for (Vector vecB : setB) {
                if (vecA.getWebshop() == vecB.getWebshop()) {
                    sameWebshop = true;
                }
                double curDist = vecDist.dist(vecA, vecB);
                if (curDist == Double.POSITIVE_INFINITY) {
                    inf = true;
                } else {
                    totaldist += curDist;
                }
            }
        }
        double avDist = totaldist / (setA.size() * setB.size());

        if (setA.size() + setB.size() > nrWebshops || avDist > treshold || inf == true || sameWebshop == true) {
            avDist = Double.POSITIVE_INFINITY;
        }

        return avDist;
    }

    public static double completeLinkage(MyCluster a, MyCluster b, VecDist vecDist, ArrayList<Integer> indMissing, double treshold, int nrWebshops) {
        double dist = Double.NEGATIVE_INFINITY;
        Set<Vector> setA = a.getVectors();
        Set<Vector> setB = b.getVectors();
        boolean sameWebshop = false;
        for (Vector vecA : setA) {
            for (Vector vecB : setB) {
                if (vecA.getWebshop() == vecB.getWebshop()) {
                    sameWebshop = true;
                }
                double curDist = vecDist.dist(vecA, vecB);
                if (curDist > dist && curDist != Double.POSITIVE_INFINITY) {
                    dist = curDist;
                }
            }
        }

        if (setA.size() + setB.size() > nrWebshops || dist > treshold || dist == Double.NEGATIVE_INFINITY || sameWebshop == true) {
            dist = Double.POSITIVE_INFINITY;
        }

        return dist;
    }

}