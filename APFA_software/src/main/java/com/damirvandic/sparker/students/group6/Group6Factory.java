package com.damirvandic.sparker.students.group6;

import com.damirvandic.sparker.algorithms.AbstractBlockingBasedMsmFactory;
import com.damirvandic.sparker.msm.MsmClusteringProcedure;

public class Group6Factory extends AbstractBlockingBasedMsmFactory {
    public Group6Factory() {
        super(new Group6BlockingScheme(0.3, 0.9), new MsmClusteringProcedure());
    }
}