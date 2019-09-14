package com.damirvandic.sparker.students.group6;

import com.damirvandic.sparker.core.ProductSimilarity;
import com.damirvandic.sparker.msm.MsmSimilarity;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;

/**
 * Created by Gerhard on 3-11-2014.
 */
public class VecDist {

    private Map<Vector, Set<Vector>> neighbors;
    private ProductSimilarity msm;
    private Map<Vector, HashMap<Vector, Double>> productDistance;

    public VecDist(Map<Vector, Set<Vector>> neigh) {
        Map<String, Double> c = convertConfig(1.0, 1.0, 0.6, 0.65);
        msm = new MsmSimilarity(true, c);

        this.neighbors = neigh;

        productDistance = new HashMap<Vector, HashMap<Vector, Double>>();
        double distance;
        HashMap<Vector, Double> vSet;

        // initialize distances
        for (Vector v1 : neighbors.keySet()) {
            vSet = new HashMap<Vector, Double>();

            for (Vector v2 : neighbors.get(v1)) {
                // distances already defined
                if (productDistance.keySet().contains(v2)) {
                    vSet.put(v2, productDistance.get(v2).get(v1));
                } else {
                    distance = 1 - msm.computeSim(v1.getProduct(), v2.getProduct());
                    vSet.put(v2, distance);
                }
            }
            productDistance.put(v1, vSet);
        }
    }

    private static Map<String, Double> convertConfig(double kappa, double lambda, double gamma, double mu) {
        Map<String, Double> ret = new HashMap<>();
        ret.put("kappa", kappa);
        ret.put("lambda", lambda);
        ret.put("gamma", gamma);
        ret.put("mu", mu);
        return Collections.unmodifiableMap(ret);
    }

    public double dist(Vector a, Vector b) {
        if (productDistance.get(a).containsKey(b)) {
            return productDistance.get(a).get(b);
        } else {
            return Double.POSITIVE_INFINITY;
        }
    }
}