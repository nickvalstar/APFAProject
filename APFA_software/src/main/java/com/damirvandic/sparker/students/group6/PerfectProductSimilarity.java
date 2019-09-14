package com.damirvandic.sparker.students.group6;

import com.damirvandic.sparker.core.ProductDesc;
import com.damirvandic.sparker.core.ProductSimilarity;

import java.util.Map;

public class PerfectProductSimilarity implements ProductSimilarityRepo {
    public PerfectProductSimilarity(Map<String, Object> conf) {
    }

    @Override
    public ProductSimilarity getProdSim(Map<String, Object> conf) {
        return new ProductSimilarity() {
            @Override
            public double computeSim(ProductDesc a, ProductDesc b) throws RuntimeException {
                return a.modelID.equals(b.modelID) ? 1.0 : 0.0;
            }
        };
    }
}
