package com.damirvandic.sparker.marnix;

import com.damirvandic.sparker.core.AbstractMsmBasedFactory;
import com.damirvandic.sparker.msm.MsmClusteringProcedure;

public class MarnixMsmFactory extends AbstractMsmBasedFactory {
    public MarnixMsmFactory() {
        super("msm.fitness", new FitnessBasedClusteringProcedure());
    }
}