package com.damirvandic.sparker.algorithms;

import ch.usi.inf.sape.hac.agglomeration.AverageLinkage;
import com.damirvandic.sparker.core.AbstractMsmBasedFactory;
import com.damirvandic.sparker.core.HierarchicalClustering;

public class HCAverageClusteringMsmFactory extends AbstractMsmBasedFactory {
    public HCAverageClusteringMsmFactory() {
        super("msm.average", new HierarchicalClustering(new AverageLinkage()));
    }
}
