package com.damirvandic.sparker.students.nick;

import com.damirvandic.sparker.core.AbstractMsmBasedFactory;
import com.damirvandic.sparker.msm.MsmClusteringProcedure;

public class NickFactory extends AbstractMsmBasedFactory {
    public NickFactory() {
        super("msm.nick", new MsmClusteringProcedure());
    }
}