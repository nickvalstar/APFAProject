package com.damirvandic.sparker.msm;

import com.damirvandic.sparker.core.ProductDesc;

public interface TitleAnalyzer {
    boolean differentTitleBrands(ProductDesc a, ProductDesc b);

    double computeSim(ProductDesc prodI, ProductDesc prodJ);
}
