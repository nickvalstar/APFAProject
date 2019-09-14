package com.damirvandic.sparker.students.group6.UniformProductDescription;

import com.damirvandic.sparker.core.ProductDesc;

import java.util.*;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Created by wkuipers on 11-10-14.
 */
public class UniProductDescBuilder {
    private UniformDescriptions uniformDescriptions;
    private Set<ProductDesc> productDescs;
    private Set<UniProductDesc> uniProductDescs;
    private HashMap<String, String> valueMapping;


    private String numReg = "(([0-9],*)+(?:(\\.\\d+)|(-\\d+/\\d))?)";

    public UniProductDescBuilder(UniformDescriptions u, Set<ProductDesc> products) {
        uniformDescriptions = u;
        productDescs = products;
        uniProductDescs = new HashSet<>();
        valueMapping = new HashMap<>();
    }

    public void build() {
        UniProductDesc uniP;
        String cleanedKey, cleanedValue, cleanedValue1;
        int i = 1;

        int index = 0;

        for (ProductDesc p : productDescs) {
            uniP = new UniProductDesc(p, i);
            Map<String, String> cleanedKVS = new HashMap<String, String>();

            for (Map.Entry<String, String> e : p.featuresMap.entrySet()) {
                cleanedKey = KeyValues.cleanKey(e.getKey());
                cleanedValue = KeyValues.cleanValue(e.getValue());
                cleanedValue1 = clean(cleanedValue, cleanedKey);
                cleanedKVS.put(cleanedKey, cleanedValue1);

                valueMapping.put(cleanedValue, cleanedValue1);


            }

            int clusterIndex = 1;
            for (ArrayList<String> keyCluster : uniformDescriptions.getKeyClusters()) {
                for (String key : keyCluster) {
                    if (cleanedKVS.containsKey(key)) {
                        uniP.addFeature("key" + clusterIndex, cleanedKVS.get(key));
                    } else {
                        uniP.addFeature("key" + clusterIndex, "MISSING");
                    }
                }
                clusterIndex++;
            }

            uniProductDescs.add(uniP);
            i++;
        }
    }

    public Set<UniProductDesc> getUniProductDescs() {
        return uniProductDescs;
    }

    public String clean(String value, String key) {
        String type = uniformDescriptions.getTypeClassifier().getDataType(key);
        Pattern p;
        Matcher m;

        String v = "";

        if (type.contains("n")) {
            p = Pattern.compile(numReg);
            m = p.matcher(value);

            while (m.find()) {
                v = v + m.group();
            }
            return v;
        } else {
            return value;
        }
    }

    public HashMap<String, String> getValueMapping() {
        return valueMapping;
    }


}
