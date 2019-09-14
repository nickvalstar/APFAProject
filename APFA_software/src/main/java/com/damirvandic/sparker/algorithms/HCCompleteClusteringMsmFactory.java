package com.damirvandic.sparker.algorithms;

import ch.usi.inf.sape.hac.agglomeration.CompleteLinkage;
import com.damirvandic.sparker.core.AbstractMsmBasedFactory;
import com.damirvandic.sparker.core.HierarchicalClustering;

public class HCCompleteClusteringMsmFactory extends AbstractMsmBasedFactory {
    public HCCompleteClusteringMsmFactory() {
        super("msm.complete", new HierarchicalClustering(new CompleteLinkage()));
    }
}
