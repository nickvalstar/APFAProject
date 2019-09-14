package com.damirvandic.sparker.algorithms;

import ch.usi.inf.sape.hac.agglomeration.WardLinkage;
import com.damirvandic.sparker.core.AbstractMsmBasedFactory;
import com.damirvandic.sparker.core.HierarchicalClustering;

public class HCWardClusteringMsmFactory extends AbstractMsmBasedFactory {
    public HCWardClusteringMsmFactory() {
        super("msm.ward", new HierarchicalClustering(new WardLinkage()));
    }
}
