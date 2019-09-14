package com.damirvandic.sparker.core;

import com.damirvandic.sparker.util.IntPair;
import gnu.trove.map.TIntObjectMap;
import org.apache.commons.lang3.tuple.ImmutablePair;
import org.apache.commons.lang3.tuple.Pair;

import java.util.HashSet;
import java.util.Map;
import java.util.Set;

public class PerfectClusteringProcedure implements ClusteringProcedure {
    @Override
    public Clusters createClusters(Map<String, Object> conf, Map<IntPair, Double> similarities, TIntObjectMap<ProductDesc> index) {
        Set<Pair<ProductDesc, ProductDesc>> edges = new HashSet<>();
        for (Map.Entry<IntPair, Double> entry : similarities.entrySet()) {
            if (entry.getValue() > 0) {
                IntPair pair = entry.getKey();
                ProductDesc a = index.get(pair.id_a);
                ProductDesc b = index.get(pair.id_b);
                edges.add(new ImmutablePair<>(a, b));
            }
        }
        Set<Set<ProductDesc>> clusters = new ConnectedComponentsFinder<>(edges).findClusters();
        return new ClustersBuilder().fromSets(clusters);
    }

    @Override
    public String componentID() {
        return "perfC";
    }
}
