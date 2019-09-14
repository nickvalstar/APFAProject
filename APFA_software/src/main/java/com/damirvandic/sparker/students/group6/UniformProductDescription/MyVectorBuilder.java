package com.damirvandic.sparker.students.group6.UniformProductDescription;


import com.damirvandic.sparker.students.group6.Vector;

import java.util.*;

/**
 * Created by wkuipers on 12-10-14.
 */
public class MyVectorBuilder {
    private Set<UniProductDesc> uniProductDescs;
    private Set<Vector> vectors;
    private UniformDescriptions uniformDescriptions;
    private HashMap<String, Integer> shopNumber;
    private ArrayList<Integer> indicesMissingValues;
    private HashMap<String, Map<String, Integer>> vectorKeyValues;
    private UniProductDescBuilder builder;


    public MyVectorBuilder(Set<UniProductDesc> products, UniformDescriptions u, UniProductDescBuilder b) {
        uniProductDescs = products;
        uniformDescriptions = u;
        vectors = new HashSet<>();
        vectorKeyValues = new HashMap<>();
        builder = b;

        shopNumber = new HashMap<>();
        shopNumber.put("bestbuy.com", 1);
        shopNumber.put("newegg.com", 2);
        shopNumber.put("amazon.com", 3);
        shopNumber.put("thenerds.net", 4);

        indicesMissingValues = new ArrayList<>();
    }

    public void buildVectors() {
        KeyValues keyValues = uniformDescriptions.getKeyValues();
        ArrayList<ArrayList<String>> keyClusters = uniformDescriptions.getKeyClusters();

        //Map<String,Map<String,Integer>> vectorKeyValues = new HashMap<String, Map<String, Integer>>();

        int indexCount = 0;
        String val;

        for (int i = 0; i < keyClusters.size(); i++) {
            vectorKeyValues.put("key" + (i + 1), new HashMap<String, Integer>());
            vectorKeyValues.get("key" + (i + 1)).put("MISSING", indexCount);
            indicesMissingValues.add(indexCount);
            indexCount++;

            for (String key : keyClusters.get(i)) {
                for (Map.Entry<String, Integer> value : keyValues.getValues(key).entrySet()) {
                    val = builder.clean(value.getKey(), key);
                    if (!vectorKeyValues.get("key" + (i + 1)).containsKey(val)) {

                        vectorKeyValues.get("key" + (i + 1)).put(val, indexCount);
                        indexCount++;
                    }
                }
            }
        }

        int vectorLength = indexCount;

        // System.out.println("vectorLength: "+vectorLength);

        Vector v;
        int index;
        int counter = 1;
        for (UniProductDesc u : uniProductDescs) {
            v = new Vector(vectorLength);
            v.setWebshop(shopNumber.get(u.getOriginalProductDesc().shop));
            v.setProduct(u.getOriginalProductDesc());
            v.setProductNumber(counter);
            counter++;

            for (Map.Entry<String, String> kv : u.getFeatures().entrySet()) {
                // missing or empty
                if (kv.getValue().equals("MISSING") || kv.getValue().equals("")) {
                    index = (int) vectorKeyValues.get(kv.getKey()).get("MISSING");
                    v.set(index, 1);
                } else // value exists
                {
                    //System.out.println("searched key: "+kv.getKey()+"\tsearched value: "+kv.getValue());

                    if (vectorKeyValues.get(kv.getKey()) == null) {
                        System.out.println("error: key = " + kv.getKey());
                    }
                    if (vectorKeyValues.get(kv.getKey()).get(kv.getValue()) == null) {
                        System.out.println("error value = " + kv.getValue());
                    }

                    index = (int) vectorKeyValues.get(kv.getKey()).get(kv.getValue());
                    v.set(index, 1);
                }

                v.setKey(u.getOriginalProductDesc().modelID);
            }
            vectors.add(v);
        }
    }

    public Set<Vector> getVectors() {
        return vectors;
    }

    public ArrayList<Integer> getIndicesMissingValues() {
        return indicesMissingValues;
    }
}
