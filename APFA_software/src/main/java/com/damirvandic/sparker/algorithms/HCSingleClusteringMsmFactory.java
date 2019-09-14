package com.damirvandic.sparker.algorithms;

import ch.usi.inf.sape.hac.agglomeration.SingleLinkage;
import com.damirvandic.sparker.core.AbstractMsmBasedFactory;
import com.damirvandic.sparker.core.HierarchicalClustering;

public class HCSingleClusteringMsmFactory extends AbstractMsmBasedFactory {
    public HCSingleClusteringMsmFactory() {
        super("msm.single", new HierarchicalClustering(new SingleLinkage()));
    }
}
