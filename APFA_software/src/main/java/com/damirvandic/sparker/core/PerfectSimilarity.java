package com.damirvandic.sparker.core;

public class PerfectSimilarity implements ProductSimilarity {
    @Override
    public double computeSim(ProductDesc a, ProductDesc b) {
        return a.modelID.equals(b.modelID) ? 1.0 : 0.0;
    }
}
