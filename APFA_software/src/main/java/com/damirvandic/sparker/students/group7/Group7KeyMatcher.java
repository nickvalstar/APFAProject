package com.damirvandic.sparker.students.group7;

import com.damirvandic.sparker.core.ProductDesc;
import com.damirvandic.sparker.msm.KeyMatcher;
import com.damirvandic.sparker.util.StringPair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

public class Group7KeyMatcher implements KeyMatcher {
    private final Logger log = LoggerFactory.getLogger(getClass());
    private final Set<StringPair> matchingKeyPairs;
    private final Map<String, Map<String, Set<String>>> matchingKeyPairsSets;
    private final KeyMatcher msmKeyMatcher;
    private final Map<StringPair, Double> pValues;


    @Override
    public String toString() {
        return "Group7KeyMatcher";
    }

    public Group7KeyMatcher(KeyMatcher msmKeyMatcher, Map<String, Object> conf, Set<ProductDesc> data) {
        this.msmKeyMatcher = msmKeyMatcher;
        this.matchingKeyPairs = new KeyMatchingAlg(conf, data).matchingKeyPairs;
        this.matchingKeyPairsSets = extractKeyPairSets(matchingKeyPairs);
        this.pValues = new KeyMatchingAlg(conf, data).pValues;
        log.info("KeyMatcher created");
    }


    private Map<String, Map<String, Set<String>>> extractKeyPairSets(Set<StringPair> matchingKeyPairs) {
        Map<String, Map<String, Set<String>>> ret = new HashMap<>();
        for (StringPair p : matchingKeyPairs) {

            Map<String, Set<String>> pairsA = getShopPairs(ret, p.id_a);
            getPairs(pairsA, extractShopName(p.id_a)).add(p.id_b);

            Map<String, Set<String>> pairsB = getShopPairs(ret, p.id_b);
            getPairs(pairsB, extractShopName(p.id_b)).add(p.id_a);

        }
        return ret;
    }

    private String extractShopName(String key) {
        return key.split("__")[0];
    }

    private Map<String, Set<String>> getShopPairs(Map<String, Map<String, Set<String>>> map, String key) {
        Map<String, Set<String>> ret;
        if (map.containsKey(key)) {
            ret = map.get(key);
        } else {
            ret = new HashMap<>();
            map.put(key, ret);
        }
        return ret;
    }

    private Set<String> getPairs(Map<String, Set<String>> map, String key) {
        Set<String> ret;
        if (map.containsKey(key)) {
            ret = map.get(key);
        } else {
            ret = new HashSet<>();
            map.put(key, ret);
        }
        return ret;
    }

    @Override
    public boolean keyMatches(String shopA, String keyA, String shopB, String keyB) {
        StringPair k = getStringPair(shopA, keyA, shopB, keyB);
        boolean msmCheck = keySimilarity(shopA, keyA, shopB, keyB) > 0.7;
        boolean ourCheck = matchingKeyPairs.contains(k);
        return msmCheck && ourCheck;
        //gamma
//        return matchingKeyPairs.contains(k);
//        double nameSim = keySimilarity(shopA, keyA, shopB, keyB);
//        double valuesSim = p_value(shopA, keyA, shopB, keyB);
//        double w = 0.5;
//        double avgSim = w * nameSim + (1 - w) * valuesSim;
//        double threshold = 0.6;
//        if (avgSim>threshold){
//            int test = 1;
//        }
//        double avgSim = keySimilarity(shopA, keyA, shopB, keyB);
//        double threshold = 0.5; // if(numeric thresholdA else thesholdB)
//        return avgSim > threshold; // TODO fix this
//        return msmKeyMatcher.keyMatches(shopA, keyA, shopB, keyB) && matchingKeyPairs.contains(getStringPair(shopA, keyA, shopB, keyB));
    }

    private double p_value(String shopA, String keyA, String shopB, String keyB) {
        StringPair StringPair = getStringPair(shopA, keyA, shopB, keyB);
        if (matchingKeyPairs.contains(StringPair)) {
            return pValues.get(StringPair);
        } else {
            return 0.0;
        }
    }


    @Override
    public double keySimilarity(String shopA, String keyA, String shopB, String keyB) {
//        StringPair k = getStringPair(shopA,keyA,shopB,keyB);
//        if (matchingKeyPairs.contains(k)) {
//            Map<String, Set<String>> mkps1 = matchingKeyPairsSets.get(shopA + "__" + keyA);
//            Set<String> matchingPairsA = mkps1.get(shopB);
//            Map<String, Set<String>> mkps2 = matchingKeyPairsSets.get(shopB + "__" + keyB);
//            Set<String> matchingPairsB = mkps2.get(shopA);
//            if (matchingPairsA != null && matchingPairsB != null && matchingPairsA.size() == 1 && matchingPairsB.size() == 1) {
//                return 1.0; // they match 100%
//            } else {
//                return msmKeyMatcher.keySimilarity(shopA, keyA, shopB, keyB);
//            }
//        }
//        else{
//            return msmKeyMatcher.keySimilarity(shopA, keyA, shopB, keyB);
//        }
        return msmKeyMatcher.keySimilarity(shopA, keyA, shopB, keyB);
//        if (keyA.equals("Brand")){
//            if (keyB.equals("Brand")){
//                int b = 1;
//            }
        //}

    }

    public static StringPair getStringPair(String shopA, String keyA, String shopB, String keyB) {
        String indexA = shopA + "__" + keyA;
        String indexB = shopB + "__" + keyB;
        return new StringPair(indexA, indexB);
    }
}
