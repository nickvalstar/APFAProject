package com.damirvandic.sparker.algorithms;

import com.damirvandic.sparker.algorithm.Algorithm;
import com.damirvandic.sparker.algorithm.AlgorithmFactory;
import com.damirvandic.sparker.blocking.schemes.BlockingScheme;
import com.damirvandic.sparker.core.ClusteringProcedure;
import com.damirvandic.sparker.core.ProductDesc;
import com.damirvandic.sparker.msm.MsmBasedAlgorithm;

import java.util.Map;

public abstract class AbstractBlockingBasedMsmFactory implements AlgorithmFactory {
    private final BlockingScheme<ProductDesc> blockingScheme;
    private final ClusteringProcedure clustering;

    public AbstractBlockingBasedMsmFactory(BlockingScheme<ProductDesc> blockingScheme, ClusteringProcedure clustering) {
        this.blockingScheme = blockingScheme;
        this.clustering = clustering;
    }

    @Override
    public Algorithm build(String cachingPath, Map<String, String> hadoopConfig) {
        return new MsmBasedAlgorithm(blockingScheme.componentID() + "|" + clustering.componentID(), clustering, cachingPath, hadoopConfig, blockingScheme);
    }
}
