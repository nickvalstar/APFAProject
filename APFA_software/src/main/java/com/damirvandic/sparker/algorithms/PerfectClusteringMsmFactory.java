package com.damirvandic.sparker.algorithms;

import com.damirvandic.sparker.core.AbstractMsmBasedFactory;
import com.damirvandic.sparker.core.PerfectClusteringProcedure;

public class PerfectClusteringMsmFactory extends AbstractMsmBasedFactory {
    public PerfectClusteringMsmFactory() {
        super("msm.perfect", new PerfectClusteringProcedure());
    }
}
