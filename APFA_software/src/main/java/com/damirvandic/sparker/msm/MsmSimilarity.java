package com.damirvandic.sparker.msm;

import com.damirvandic.sparker.core.ProductDesc;
import com.damirvandic.sparker.core.ProductSimilarity;
import com.damirvandic.sparker.students.nick.NickKeyMatchingAlg;
import com.damirvandic.sparker.util.IntPair;
import com.damirvandic.sparker.util.StringPair;
import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import uk.ac.shef.wit.simmetrics.similaritymetrics.QGramsDistance;

import java.util.*;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class MsmSimilarity implements ProductSimilarity {
    private static final Pattern MW_PATTERN = getMWPattern();
    public static final int DEFAULT_MAX_CACHE_SIZE_SIM = 500000;
    public static final int DEFAULT_MAX_CACHE_SIZE_VAL = 500000;
    private final SimComputer simComputer;

    private static final Cache<IntPair, Float> defaultSimCache() {
        return CacheBuilder.newBuilder().maximumSize(DEFAULT_MAX_CACHE_SIZE_SIM).build();
    }

    private final LoadingCache<StringPair, Float> defaultValCache() {
        return CacheBuilder.newBuilder().maximumSize(DEFAULT_MAX_CACHE_SIZE_VAL).build(new CacheLoader<StringPair, Float>() {
            @Override
            public Float load(StringPair key) throws Exception {
                return qGrammer.getSimilarity(preProcess(key.id_a), preProcess(key.id_b));
            }
        });
    }

    private final boolean brandHeuristic;
    private final TitleAnalyzer titleAnalyzer;
    private final QGramsDistance qGrammer;
    private final int kappa;        //power of key-sim as weights of value-sims
    private final int lambda;        //power of value-sim
    private final double mu;
    private final KeyMatcher keyMatcher;
    private final double beta;
    private final double alpha;
    public final Cache<IntPair, Float> simCache;
    public final LoadingCache<StringPair, Float> valCache;

    public MsmSimilarity(boolean brandHeuristic,
                         Map<String, Double> conf,
                         KeyMatcher keyMatcher,
                         Cache<IntPair, Float> simCache) {
        this.qGrammer = new QGramsDistance();
        this.brandHeuristic = brandHeuristic;
        this.kappa = conf.get("kappa").intValue();
        this.lambda = conf.get("lambda").intValue();
        this.alpha = conf.get("alpha");
        this.beta = conf.get("beta");
        this.titleAnalyzer = new TitleAnalyzerImpl(alpha, beta);
        this.mu = conf.get("mu");
        this.keyMatcher = keyMatcher;
        this.simCache = simCache;
        this.simComputer = simCache == null ? new NonCachedSimComputer() : new CachedSimComputer();
        this.valCache = defaultValCache();
    }

    public MsmSimilarity(boolean brandHeuristic,
                         Map<String, Double> conf) {
        this(brandHeuristic, conf, new MsmKeyMatcher(conf.get("gamma")), defaultSimCache());
    }

    public MsmSimilarity(boolean brandHeuristic,
                         Map<String, Double> conf,
                         KeyMatcher keyMatcher) {
        this(brandHeuristic, conf, keyMatcher, defaultSimCache());
    }


    private static Pattern getMWPattern() {
        String regex = "([a-zA-Z0-9]*(([0-9]+[^0-9^,^ ]+)|([^0-9^,^ ]+[0-9]+))[a-zA-Z0-9]*)";
        Pattern p = Pattern.compile(regex);
        return p;
    }

    /**
     * Calculate percentage of matching model words in two sets of model words.
     * Model words are broader here: also purely numeric words are treated as model words.
     */
    private static double mw(ArrayList<String> C, ArrayList<String> D) {
        int totalWords = C.size() + D.size();
        if (totalWords == 0) return 0.0;
        C.retainAll(D);
        D.retainAll(C);    //note: duplicate words in a set is counted twice as match.
        return (C.size() + D.size()) / (double) totalWords;
    }

    private interface SimComputer {
        double computeSim(final ProductDesc a, final ProductDesc b);
    }

    class CachedSimComputer implements SimComputer {

        @Override
        public double computeSim(final ProductDesc a, final ProductDesc b) {
            IntPair key = new IntPair(a.ID, b.ID);
            try {
                return simCache.get(key, new Callable<Float>() {
                    @Override
                    public Float call() throws Exception {
                        return _computeSim(a, b);
                    }
                });
            } catch (ExecutionException e) {
                throw new RuntimeException("Error occurred computing sim for " + a + " and " + b, e);
            }
        }
    }

    class NonCachedSimComputer implements SimComputer {
        @Override
        public double computeSim(ProductDesc a, ProductDesc b) {
            return _computeSim(a, b);
        }
    }

    private float _computeSim(ProductDesc a, ProductDesc b) {
        if (keyMatcher.toString().equals("NickKeyMatcher")){
            if (brandHeuristic &&  NickKeyMatchingAlg.differentBrands(a.ID,b.ID))
                return 0.0f;
            else
                return NickKeyMatchingAlg.productSimilarity(a, b);
        } else { //default key matcher or other key matchers.
            if (brandHeuristic && titleAnalyzer.differentTitleBrands(a, b)) {
                return 0.0f;
            } else{
                return productSimilarity(a, b);
            }
        }
    }

    @Override
    public double computeSim(ProductDesc a, ProductDesc b) throws RuntimeException {
        return simComputer.computeSim(a, b);
    }

    private float productSimilarity(ProductDesc prodI, ProductDesc prodJ) {
        double sim = 0;
        double avgSim = 0;
        int m = 0;    //number of matches
        double w = 0;    // total weight

        Set<String> iKeys = new HashSet<>(prodI.featuresMap.keySet());
        Set<String> jKeys = new HashSet<>(prodJ.featuresMap.keySet());

        for (String keyI : prodI.featuresMap.keySet()) {
            String qProcessed = preProcess(keyI);
            for (String keyJ : prodJ.featuresMap.keySet()) {
                boolean keyMatches = keyMatcher.keyMatches(prodI.shop, keyI, prodJ.shop, keyJ);
                if (keyMatches) {    //Keys keyI and keyJ are matching
                    //These keys don't have to be checked for model words in the values
                    iKeys.remove(keyI);
                    jKeys.remove(keyJ);

                    // compute similarity between their values
                    String qVal = prodI.featuresMap.get(keyI);
                    String rVal = prodJ.featuresMap.get(keyJ);

                    float valueSim = getValueSimilarity(qVal, rVal);
                    double keySim = keyMatcher.keySimilarity(prodI.shop, keyI, prodJ.shop, keyJ);
                    double weight = Math.pow(keySim, kappa);        //high keySim is weighted more
                    sim = sim + weight * Math.pow(valueSim, lambda);    //low valueSim is punished
                    m++;
                    w += weight;
                }
            }
        }

        // check if we can compute first part of avgSim
        if (w > 0) {
            avgSim = sim / w;
        }

        //ArrayList of values without matching keys (arrayList to retain duplicates)
        ArrayList<String> exMWi = extractModelWords(prodI, iKeys);
        ArrayList<String> exMWj = extractModelWords(prodJ, jKeys);

        double mwPerc = mw(exMWi, exMWj);
        double titleSim = titleAnalyzer.computeSim(prodI, prodJ);
        double hSim;
        if (titleSim == -1) {
            double theta1 = m / (double) Math.min(prodI.featuresMap.size(), prodJ.featuresMap.size());
            double theta2 = 1 - theta1;
            hSim = theta1 * avgSim + theta2 * mwPerc;
        } else {
            double theta1 = (1 - mu) * m / (double) Math.min(prodI.featuresMap.size(), prodJ.featuresMap.size());
            double theta2 = 1 - mu - theta1;
            hSim = theta1 * avgSim + theta2 * mwPerc + mu * titleSim;
        }

        return (float) hSim;
    }

    private float getValueSimilarity(String qVal, String rVal) {
        StringPair key = new StringPair(qVal, rVal);
        return valCache.getUnchecked(key);
    }

    private String preProcess(String str) {
        str = str.replaceAll("\\, |\\. |\\.$|\\(|\\)", " ");    //remove comma and period at end of word (or period at end of whole string)
        return str.toLowerCase(Locale.ENGLISH);
    }

    private ArrayList<String> extractModelWords(ProductDesc prod, Set<String> keys) {
        ArrayList<String> ret = new ArrayList<>();
        for (String q : keys) {
            String qVal = prod.featuresMap.get(q);
            ret.addAll(extractModelWords(qVal));
        }
        return ret;
    }

    private Set<String> extractModelWords(String title) {
        Matcher m = MW_PATTERN.matcher(title);
        HashSet<String> c = new HashSet<>();
        while (m.find()) {
            String s = m.group(1);
            c.add(s);
        }
        return c;
    }
}
