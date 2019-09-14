package com.damirvandic.sparker.core;

import ch.usi.inf.sape.hac.agglomeration.AgglomerationMethod;
import com.damirvandic.sparker.algorithm.Clustering$;
import com.damirvandic.sparker.util.IntPair;
import gnu.trove.map.TIntObjectMap;
import scala.Tuple2;

import java.util.*;

public class HierarchicalClustering implements ClusteringProcedure {
    private final AgglomerationMethod linkage;

    public HierarchicalClustering(AgglomerationMethod linkage) {
        this.linkage = linkage;
    }

    @Override
    public Clusters createClusters(Map<String, Object> conf, Map<IntPair, Double> similarities, TIntObjectMap<ProductDesc> index) {
        double epsilon = (double) conf.get("epsilon");
        List<ProductDesc> items = new ArrayList<>(index.valueCollection());
        Map<Tuple2<ProductDesc, ProductDesc>, Double> dMap = getDissimilaritiesMap(similarities, index);
        Set<Set<ProductDesc>> clusters = Clustering$.MODULE$.findJavaClusters(items, dMap, epsilon, linkage);
        return new ClustersBuilder().fromSets(clusters);
    }

    @Override
    public String componentID() {
        return "hc_" + linkage.toString() + "C";
    }

    private Map<Tuple2<ProductDesc, ProductDesc>, Double> getDissimilaritiesMap(Map<IntPair, Double> similarities, TIntObjectMap<ProductDesc> index) {
        Map<Tuple2<ProductDesc, ProductDesc>, Double> dMap = new HashMap<>();
        for (Map.Entry<IntPair, Double> simEntry : similarities.entrySet()) {
            IntPair pairs = simEntry.getKey();
            ProductDesc a = index.get(pairs.id_a);
            ProductDesc b = index.get(pairs.id_b);
            dMap.put(new Tuple2<>(a, b), 1 - simEntry.getValue());
        }
        return dMap;
    }
}
