package com.damirvandic.sparker.algorithms;

import com.damirvandic.sparker.core.AbstractMsmBasedFactory;
import com.damirvandic.sparker.msm.MsmClusteringProcedure;

public class MsmClusteringBasedMsmFactory extends AbstractMsmBasedFactory {
    public MsmClusteringBasedMsmFactory() {
        super("msm.msm", new MsmClusteringProcedure());
    }
}