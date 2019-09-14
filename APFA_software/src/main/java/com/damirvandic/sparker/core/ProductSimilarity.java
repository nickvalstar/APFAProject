package com.damirvandic.sparker.core;

public interface ProductSimilarity {
    /**
     * @return similarity between 0 and 1
     */
    double computeSim(ProductDesc a, ProductDesc b) throws RuntimeException;
}
