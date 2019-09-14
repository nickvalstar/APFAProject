package com.damirvandic.sparker.msm;

import com.damirvandic.sparker.core.ClusteringProcedure;
import com.damirvandic.sparker.core.Clusters;
import com.damirvandic.sparker.core.ClustersBuilder;
import com.damirvandic.sparker.core.ProductDesc;
import com.damirvandic.sparker.util.IntPair;
import gnu.trove.map.TIntObjectMap;

import java.util.Collection;
import java.util.HashMap;
import java.util.Map;

public class MsmClusteringProcedure implements ClusteringProcedure {
    @Override
    public Clusters createClusters(Map<String, Object> conf,
                                   Map<IntPair, Double> similarities,
                                   TIntObjectMap<ProductDesc> index) {
        Collection<ProductDesc> products = index.valueCollection();
        int n = products.size();
        double epsilon = (double) conf.get("epsilon");
        double[][] dissimilarities = initDissimilarities(n);

        Map<Integer, ProductDesc> indexMap = new HashMap<>();
        Map<Integer, Integer> idToIndexMap = new HashMap<>();

        int k = 0;
        for (ProductDesc p : products) {
            indexMap.put(k, p);
            idToIndexMap.put(p.ID, k);
            ++k;
        }

        for (IntPair pair : similarities.keySet()) {
            double dist = getDistance(similarities, pair);
            int indexA = idToIndexMap.get(pair.id_a);
            int indexB = idToIndexMap.get(pair.id_b);
            dissimilarities[indexA][indexB] = dist;
            dissimilarities[indexB][indexA] = dist;
        }

        CustomClustering cc = new CustomClustering(indexMap, dissimilarities, epsilon);
        cc.createClusters();

        return new ClustersBuilder().fromSets(cc.getClusters());
    }

    @Override
    public String componentID() {
        return "msmC";
    }

    private double getDistance(Map<IntPair, Double> similarities, IntPair pair) {
        double dist;
        double sim = similarities.get(pair);
        if (Double.isInfinite(sim)) {
            dist = Double.POSITIVE_INFINITY;
        } else {
            dist = 1 - sim;
        }
        return dist;
    }

    private double[][] initDissimilarities(int n) {
        double[][] ret = new double[n][n];
        for (int i = 0; i < n; i++) {
            for (int j = i + 1; j < n; j++) {
                ret[i][j] = 1.0;
                ret[j][i] = 1.0;
            }
        }
        return ret;
    }
}
