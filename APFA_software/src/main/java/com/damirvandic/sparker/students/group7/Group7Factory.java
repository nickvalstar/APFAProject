package com.damirvandic.sparker.students.group7;

import com.damirvandic.sparker.core.AbstractMsmBasedFactory;
import com.damirvandic.sparker.msm.MsmClusteringProcedure;

public class Group7Factory extends AbstractMsmBasedFactory {
    public Group7Factory() {
        super("msm.group7", new MsmClusteringProcedure());
    }
}