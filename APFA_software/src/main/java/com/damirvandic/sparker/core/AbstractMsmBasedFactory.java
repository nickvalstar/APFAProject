package com.damirvandic.sparker.core;

import com.damirvandic.sparker.algorithm.Algorithm;
import com.damirvandic.sparker.algorithm.AlgorithmFactory;
import com.damirvandic.sparker.blocking.schemes.AllPairsScheme;
import com.damirvandic.sparker.msm.MsmBasedAlgorithm;

import java.util.Map;

public abstract class AbstractMsmBasedFactory implements AlgorithmFactory {
    private final ClusteringProcedure clustering;
    private final String name;
    private final ProductSimilarity sim;

    public AbstractMsmBasedFactory(String name, ClusteringProcedure clustering) {
        this(name, clustering, null);
    }

    public AbstractMsmBasedFactory(String name, ClusteringProcedure clustering, ProductSimilarity sim) {
        this.name = name;
        this.clustering = clustering;
        this.sim = sim;
    }

    @Override
    public Algorithm build(String cachingPath, Map<String, String> hadoopConfig) {
        return new MsmBasedAlgorithm(name, clustering, cachingPath, hadoopConfig, new AllPairsScheme(), sim);
    }
}
