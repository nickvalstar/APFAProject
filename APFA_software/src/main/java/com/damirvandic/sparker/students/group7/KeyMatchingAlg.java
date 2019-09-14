package com.damirvandic.sparker.students.group7;


import com.damirvandic.sparker.core.ProductDesc;
import com.damirvandic.sparker.util.StringPair;
import com.google.common.primitives.Doubles;
import org.apache.commons.math3.stat.inference.KolmogorovSmirnovTest;
import org.apache.commons.math3.stat.inference.TTest;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;


public class KeyMatchingAlg {
    public final Set<StringPair> matchingKeyPairs;
    public final Map<StringPair, Double> pValues;
    private final Logger log = LoggerFactory.getLogger(getClass());
    private final KolmogorovSmirnovTest km = new KolmogorovSmirnovTest();
    private final TTest studentTest = new TTest();
    // Patterns om te kijken of er een nummer instaat, of leestekens.
    private Pattern leestekenDetector = Pattern.compile("([a-zA-Z])+");
    private Pattern capitalLowercase = Pattern.compile("([A-Z])([A-Z]+)\\b");
    private Pattern numberDetector = Pattern.compile("((?:\\d*\\.)?\\d+)");

    public KeyMatchingAlg(Map<String, Object> conf, Set<ProductDesc> data) {
        this.matchingKeyPairs = new HashSet<>();
        this.pValues = new HashMap<>();
        computeAnswers(conf, data);
    }

    private void computeAnswers(Map<String, Object> conf, Set<ProductDesc> data) {
        double jaccardSimilarity = (double) conf.get("jaccardSimilarity");

// Aanmaken map met alle unieke keys en hun bijbehorende unieke values, per webshop.
        Map<String, Map<String, ValuesWrapper>> shopKeyValuesMap = new HashMap<>();

// Hier wordt de map opgevuld, startend met elke productdesc
        makeKeysMap(data, shopKeyValuesMap);

// return result of this method
        Set<StringPair> ret = new HashSet<>();

        for (String shopA : shopKeyValuesMap.keySet()) {
// Get shopA map
            Map<String, ValuesWrapper> featuresA = shopKeyValuesMap.get(shopA);

            for (String keyA : featuresA.keySet()) {
                ValuesWrapper valuesWrapperA = featuresA.get(keyA);
                for (String shopB : shopKeyValuesMap.keySet()) {
                    if (!shopA.equals(shopB)) {
                        Map<String, ValuesWrapper> featuresB = shopKeyValuesMap.get(shopB);

                        for (String keyB : featuresB.keySet()) {
                            ValuesWrapper valuesWrapperB = featuresB.get(keyB);

                            processKeyPair(jaccardSimilarity, matchingKeyPairs, shopA, keyA, valuesWrapperA, shopB, keyB, valuesWrapperB);
                        }
                    }
                }
            }
        }
        this.matchingKeyPairs.addAll(ret);
    }

    private void processKeyPair(double jaccardSimThreshold,
                                Set<StringPair> matchingKeyPairs,
                                String shopA,
                                String keyA,
                                ValuesWrapper valuesWrapperA,
                                String shopB,
                                String keyB,
                                ValuesWrapper valuesWrapperB) {

        String valueTypeA = determineValueType(valuesWrapperA);
        String valueTypeB = determineValueType(valuesWrapperB);

        if (valueTypeA.equals(valueTypeB)) {
            boolean matches = false;
            if (valueTypeA.equals("DOUBLES")) {
// hier je Doubles similarity
                matches = areMatchingDoubles(matchingKeyPairs, shopA, keyA, valuesWrapperA, shopB, keyB, valuesWrapperB);
            } else if (valueTypeA.equals("STRINGS")) {
// hier je Strings similarity
                matches = areMatchingStrings(jaccardSimThreshold, matchingKeyPairs, shopA, keyA, valuesWrapperA, shopB, keyB, valuesWrapperB);
            } else {
                throw new IllegalArgumentException("Unknown value type: " + valueTypeA);
            }
//if (matches) {
//StringPair keyPair = Group7KeyMatcher.getStringPair(shopA, keyA, shopB, keyB);
//matchingKeyPairs.add(keyPair);
//}
        }
    }

    private String determineValueType(ValuesWrapper valuesWrapper) {
        if (valuesWrapper.productCountStrings > valuesWrapper.productCountDoubles) {
            return "STRINGS";
        } else {
            return "DOUBLES";
        }
    }

    // TODO: finish & optimize
    private boolean areMatchingStrings(double jaccardSimilarity,
                                       Set<StringPair> matchingKeyPairs,
                                       String shopA,
                                       String keyA,
                                       ValuesWrapper valuesWrapperA,
                                       String shopB,
                                       String keyB,
                                       ValuesWrapper valuesWrapperB) {
        double jaccard = similarity(valuesWrapperA.stringValues, valuesWrapperB.stringValues);
        if (jaccard > jaccardSimilarity) {
            if (jaccard < 1.0) {
                int test = 1;
            }
            StringPair stringpair = Group7KeyMatcher.getStringPair(shopA, keyA, shopB, keyB);
            pValues.put(stringpair, jaccard);
            matchingKeyPairs.add(stringpair);
            return true;
        } else {
            return false;
        }
    }


    // TODO: finish & optimize
    private boolean areMatchingDoubles(Set<StringPair> matchingKeyPairs,
                                       String shopA,
                                       String keyA,
                                       ValuesWrapper valuesWrapperA,
                                       String shopB,
                                       String keyB,
                                       ValuesWrapper valuesWrapperB) {
//return true;
        double[] valuesA = Doubles.toArray(valuesWrapperA.doubleValues);
        double[] valuesB = Doubles.toArray(valuesWrapperB.doubleValues);

        if (valuesA.length > 1 && valuesB.length > 1) {


            //double p_value = km.kolmogorovSmirnovTest(valuesA, valuesB, true);

            double p_value = studentTest.tTest(valuesA, valuesB);


            //double p_value = 0.6;
            //double p_value = average...
            if (p_value > 0.11) {
                StringPair keyPair = Group7KeyMatcher.getStringPair(shopA, keyA, shopB, keyB);
                pValues.put(keyPair, p_value);
                matchingKeyPairs.add(keyPair);
                return true;
            } else {
                return false;
            }

        } else {
            return false;
        }
    }

    private void makeKeysMap(Set<ProductDesc> data,
                             Map<String, Map<String, ValuesWrapper>> uniekeKeys) {
        for (ProductDesc p : data) {
// Pak de features
            Map<String, String> features = p.featuresMap;
// Pak de bijhorende shop map
            Map<String, ValuesWrapper> shopMap = getShopMap(uniekeKeys, p);

// Voor elke key uit deze features
            for (String key : features.keySet()) {

// De set met verschillende values
                ValuesWrapper valuesWrapper = getValuesSet(shopMap, key);

// Neem value van bijbehorende key
                String waarde = features.get(key);

//String getallen = "/(?:\d*\.)?\d+/g";

//Pattern pinteger = Pattern.compile("^[a-zA-Z]+([0-9]+).*");

// Matcher matcher = regexX.matcher(waarde);
// waarde = matcher.replaceAll("$1 x $2");
// String[] splits = waarde.split(" x ");
// if(splits.length == 1){
// splits = waarde.split(" +");
// }
                String[] splits = waarde
                        .replace("x", " ")
                        .replace("/", " ")
                        .replace(":", " ")
                        .replace("~", " ")
                        .replace("-", " ").split(" +|;");
                for (String waardeSplitted : splits) {
                    String foundValue = "";
                    Matcher getallen = numberDetector.matcher(waardeSplitted);
                    if (getallen.find()) {
                        foundValue = findGetallen(getallen);

// TODO hier kan je printen
// log.info("value = {}, splits = {}, found value = {}", waarde, Arrays.asList(splits), foundValue);

                        double converted = Double.parseDouble(foundValue);
                        valuesWrapper.doubleValues.add(converted);
                        valuesWrapper.productCountDoubles = valuesWrapper.productCountDoubles + 1;
                    } else {
                        Matcher strings = leestekenDetector.matcher(waardeSplitted);
                        foundValue = findStrings(strings, foundValue);
                        valuesWrapper.stringValues.add(foundValue);
                        valuesWrapper.productCountStrings = valuesWrapper.productCountStrings + 1;
                    }
                }

                shopMap.put(key, valuesWrapper);

//Object clusterID = clusterMap.get(p.title + p.modelID);
//clusterBuilder.assignToCluster(p, clusterID);
            }
        }
    }

// private final Pattern regexX = Pattern.compile("([0-9]+)x([0-9]+)");

    private String findStrings(Matcher strings, String groepen) {
        int count = 0;
        while (strings.find()) {
            count++;
            groepen = groepen + strings.group(0).toLowerCase();

        }
        return groepen;
    }

    private String findGetallen(Matcher getallen) {
        String groepen;
        groepen = getallen.group(0);

        int count = 0;
        while (getallen.find()) {
            count++;
            groepen = groepen + getallen.group(0);

        }
        return groepen;
    }

    private ValuesWrapper getValuesSet(Map<String, ValuesWrapper> shopMap, String key) {
        if (shopMap.containsKey(key)) {
            return shopMap.get(key);
        } else {
            ValuesWrapper ret = new ValuesWrapper(new HashSet<String>(), new HashSet<Double>());
            shopMap.put(key, ret);
            return ret;
        }
    }

    private Map<String, ValuesWrapper> getShopMap(Map<String, Map<String, ValuesWrapper>> uniekeKeys, ProductDesc p) {
        if (uniekeKeys.containsKey(p.shop)) {
            return uniekeKeys.get(p.shop);
        } else {
            Map<String, ValuesWrapper> ret = new HashMap<>();
            uniekeKeys.put(p.shop, ret);
            return ret;
        }
    }

    private double similarity(Set<String> x, Set<String> y) {

        int xSize = x.size();
        int ySize = y.size();
        if (xSize == 0 || ySize == 0) {
            return 0.0;
        }

        Set<String> intersectionXY = new HashSet<>(x);
        intersectionXY.retainAll(y);

        return (double) intersectionXY.size() / (double) (xSize < ySize ? xSize : ySize);
    }

    static class ValuesWrapper {
        private final Set<String> stringValues;
        private final Set<Double> doubleValues;
        private int productCountStrings;
        private int productCountDoubles;

        ValuesWrapper(Set<String> stringValues, Set<Double> doubleValues) {
            this.productCountStrings = 0;
            this.productCountDoubles = 0;
            this.stringValues = stringValues;
            this.doubleValues = doubleValues;
        }


    }

// private static int searchShopNr(String[] shops, String shop) {
// for (int index = 0; index < shops.length; index++) {
// if (shops[index].equals(shop))
// return index; //We found it!!!
// }
// // If we get to the end of the loop, a value has not yet
// // been returned. We did not find the key in this array.
// return -1;
// }
//
// private boolean heeftValue(String key, Map<String, String[]> keyClusters) {
//
// for (String keyName : keyClusters.keySet()) {
// if (Arrays.asList(keyClusters.get(keyName)).contains(key)) {
// return true;
// }
// }
//
// return true;
// }
//
// private ReturningValues getClusterSet(String key, Map<String, String[]> keyClusters) {
//
// for (String keyName : keyClusters.keySet()) {
// if (Arrays.asList(keyClusters.get(keyName)).contains(key)) {
//
// ReturningValues rv = new ReturningValues(keyClusters.get(keyName), keyName);
//
//
// return rv;
// }
// }
//
// return null;
// }
//
// private String compareKeys(String[] clusterSet, String key2, int shop2Nr) {
//
// String[] dubbel = {key2, clusterSet[shop2Nr]};
//
// double scoreHigh = 0.0;
// String keyBest = "";
//
// for (int i = 1; i < 3; i++) {
// double score = 0.0;
// for (String b : clusterSet) {
// if (!b.equals(clusterSet[shop2Nr]) && !b.isEmpty()) {
// score = score + LevenshteinDistance.stringSimilarity(b, dubbel[i]);
// }
// }
// if (score > scoreHigh) {
// scoreHigh = score;
// keyBest = dubbel[i];
// }
// }
//
// return keyBest;
// }
// private Map<String, Object> getClusterMap() {
//
// Map<String, Object> clusterMap = new HashMap<>();
//
// /*Set<Set<ProductDesc>> clusterSets = correctClusters.asJavaSet();
// */
//
// int clusterID = 1;
// for (Set<ProductDesc> cluster : clusterSets) {
// for (ProductDesc p : cluster) {
// clusterMap.put(p.title + p.modelID, clusterID);
// }
// ++clusterID;
// }
// return Collections.unmodifiableMap(clusterMap);
// }
//


// // Maak array met shopnamen
// String[] shopNames = new String[shopKeyValuesMap.size()];
// int hulpCounter = 0;
// for (String shop : shopKeyValuesMap.keySet()) {
// shopNames[hulpCounter] = shop;
// hulpCounter++;
// }

// if (clusterKey.isEmpty()) {
// // maak eerste key + bijhorende array value cellen
// clusterKey = clusterKey0 + clusterTeller;
// String[] clusterNew = new String[4];
// clusterNew[shop1Nr] = key1;
// clusterNew[shop2Nr] = key2;
// comparableKeys.put(clusterKey, clusterNew);
// clusterTeller++;
//
// } else if (!heeftValue(key1, comparableKeys) && !heeftValue(key2, comparableKeys)) {
// // De keys zijn nog niet geclusterd: maak nieuwe key-pair value
// clusterKey = clusterKey0 + clusterTeller;
// String[] clusterNew = new String[4];
// clusterNew[shop1Nr] = key1;
// clusterNew[shop2Nr] = key2;
// comparableKeys.put(clusterKey, clusterNew);
// clusterTeller++;
//
// } else if (heeftValue(key1, comparableKeys) && !heeftValue(key2, comparableKeys)) {
// // Key 1 zit in een cluster: pak de cluster waarin deze key zit
// ReturningValues rv = getClusterSet(key1, comparableKeys);
// String[] clusterSet = rv.getKeySet();
// String clusterHuidig = rv.getKey();
//
//
// if (clusterSet[shop2Nr].isEmpty()) {
// clusterSet[shop2Nr] = key2;
// comparableKeys.put(clusterHuidig, clusterSet);
// } else {
// clusterSet[shop2Nr] = compareKeys(clusterSet, key2, shop2Nr);
//
// }
//
//
// //Set<String> bla = comparableKeys.get(1).get(1);
// }
}
