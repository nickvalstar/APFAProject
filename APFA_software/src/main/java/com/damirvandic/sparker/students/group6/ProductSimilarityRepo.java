package com.damirvandic.sparker.students.group6;

import com.damirvandic.sparker.core.ProductSimilarity;

import java.util.Map;

public interface ProductSimilarityRepo {
    ProductSimilarity getProdSim(Map<String, Object> conf);
}
