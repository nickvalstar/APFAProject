package com.damirvandic.sparker.core;

import com.damirvandic.sparker.util.IntPair;
import gnu.trove.map.TIntObjectMap;

import java.util.Map;

public interface ClusteringProcedure {
    /**
     * @param conf         the parameters
     * @param similarities all non-zero similarities
     */
    Clusters createClusters(Map<String, Object> conf, Map<IntPair, Double> similarities, TIntObjectMap<ProductDesc> index);

    String componentID();
}
