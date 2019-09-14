package com.damirvandic.sparker.students.group6.UniformProductDescription;

import com.damirvandic.sparker.core.ProductDesc;

import java.util.HashMap;
import java.util.Map;

/**
 * Created by wkuipers on 11-10-14.
 */
public class UniProductDesc {
    private ProductDesc prodDesc;
    private int productNr;
    private Map<String, String> features;
    private String shop;

    public UniProductDesc(ProductDesc p, int number) {
        prodDesc = p;
        productNr = number;
        shop = p.shop;
        features = new HashMap<>();
    }

    public void addFeature(String key, String value) {
        features.put(key, value);
    }

    public ProductDesc getOriginalProductDesc() {
        return prodDesc;
    }

    public Map<String, String> getFeatures() {
        return features;
    }

    public void print() {
        System.out.println("number: " + productNr + ",\tID: " + prodDesc.modelID + ", \tshop: " + shop);
        for (Map.Entry<String, String> kv : features.entrySet()) {
            System.out.println(kv.getKey() + ": " + kv.getValue());
        }
        System.out.println("\n");
    }
}
