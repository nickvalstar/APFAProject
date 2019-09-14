package com.damirvandic.sparker.students.group6;

import com.damirvandic.sparker.core.ProductDesc;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;

/**
 * Created by wkuipers on 07-11-14.
 */
public class LSHVectorsFromModelWords {
    private ArrayList<Vector> vectors;
    private Map<String, Integer> vectorDef;
    private Set<ProductDesc> products;
    private Map<String, Integer> shopNumber;

    private ModelWordSplitter splitter;

    public LSHVectorsFromModelWords(Set<ProductDesc> productDescSet) {
        products = productDescSet;
        initialize();

        shopNumber = new HashMap<>();
        shopNumber.put("bestbuy.com", 1);
        shopNumber.put("newegg.com", 2);
        shopNumber.put("amazon.com", 3);
        shopNumber.put("thenerds.net", 4);

        int vectorLength = vectorDef.size();
        Vector v;
        Set<String> modelWords;

        int counter = 1;

        for (ProductDesc product : products) {
            // set attributes of vector
            v = new Vector(vectorLength);
            v.setWebshop(shopNumber.get(product.shop));
            v.setProduct(product);
            v.setProductNumber(counter);
            counter++;

            // set values of vector
            modelWords = splitter.getModelWords(product.title);
            for (String w : modelWords) {
                v.set(vectorDef.get(w), 1);
            }

            vectors.add(v);
        }
    }

    public ArrayList<Vector> retrieveVectors() {
        return vectors;
    }

    private void initialize() {
        vectors = new ArrayList<Vector>();
        Vector p;

        vectorDef = new HashMap<String, Integer>();
        Set<String> modelWords;

        boolean numeric = true;
        boolean alphanumeric = true;
        boolean combined = true;

        splitter = new ModelWordSplitter(numeric, alphanumeric, combined);
        int index = 0;

        // initialize vector with model words;
        for (ProductDesc productDesc : products) {
            modelWords = splitter.getModelWords(productDesc.title);

            for (String word : modelWords) {
                if (!vectorDef.containsKey(word)) {
                    vectorDef.put(word, index);
                    index++;
                }
            }
        }
    }
}
