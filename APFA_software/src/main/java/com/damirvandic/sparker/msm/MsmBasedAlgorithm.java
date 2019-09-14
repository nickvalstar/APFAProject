package com.damirvandic.sparker.msm;

import com.damirvandic.sparker.algorithm.JavaAlgorithm;
import com.damirvandic.sparker.blocking.core.BlocksMapping;
import com.damirvandic.sparker.blocking.schemes.AllPairsScheme;
import com.damirvandic.sparker.blocking.schemes.BlockingScheme;
import com.damirvandic.sparker.core.ClusteringProcedure;
import com.damirvandic.sparker.core.Clusters;
import com.damirvandic.sparker.core.ProductDesc;
import com.damirvandic.sparker.core.ProductSimilarity;
import com.damirvandic.sparker.students.group7.Group7KeyMatcher;
import com.damirvandic.sparker.students.nick.NickKeyMatcher;
import com.damirvandic.sparker.students.nick.NickKeyMatchingAlg;
import com.damirvandic.sparker.util.IntPair;
import com.google.common.base.Joiner;
import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import com.google.common.collect.Lists;
import gnu.trove.map.TIntObjectMap;
import gnu.trove.map.hash.TIntObjectHashMap;
import gnu.trove.map.hash.TObjectFloatHashMap;
import gnu.trove.procedure.TObjectFloatProcedure;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.ObjectInputStream;
import java.util.*;

public class MsmBasedAlgorithm extends JavaAlgorithm {
    private static final long DEFAULT_MAX_CACHE_SIZE_SIM = 2000000;
    private final Logger logger = LoggerFactory.getLogger(getClass());
    private final ClusteringProcedure clusteringProcedure;
    private final String name;
    private final Path cachePath;
    private final Cache<IntPair, Float> cache;
    private final FileSystem fs;
    private final BlockingScheme<ProductDesc> blockingScheme;
    private final ProductSimilarity sim;
    private Map<String, Object> currentMsmSimConf;

    public MsmBasedAlgorithm(String name,
                             ClusteringProcedure clusteringProcedure,
                             String cachePath,
                             Map<String, String> hadoopConfig) {
        this(name, clusteringProcedure, cachePath, hadoopConfig, new AllPairsScheme(), null);
    }

    public MsmBasedAlgorithm(String name,
                             ClusteringProcedure clusteringProcedure,
                             String cachePath,
                             Map<String, String> hadoopConfig,
                             BlockingScheme<ProductDesc> blockingScheme,
                             ProductSimilarity sim) {
        this.name = name;
        this.clusteringProcedure = clusteringProcedure;
        this.cachePath = cachePath == null ? null : new Path(cachePath);
        this.fs = initFS(cachePath, hadoopConfig);
        this.cache = defaultSimCache();
        this.blockingScheme = blockingScheme;
        this.sim = sim;
    }

    public MsmBasedAlgorithm(String name,
                             ClusteringProcedure clusteringProcedure,
                             String cachePath,
                             Map<String, String> hadoopConfig,
                             BlockingScheme<ProductDesc> blockingScheme) {
        this(name, clusteringProcedure, cachePath, hadoopConfig, blockingScheme, null);
    }

    private static FileSystem initFS(String cachePath, Map<String, String> config) {
        try {
            if (cachePath != null && config == null) {
                throw new IllegalArgumentException("hadoopConfig must be specified whenever cachePath is specified");
            }
            return config != null ? FileSystem.get(extractHadoopConfig(config)) : null;
        } catch (IOException e) {
            throw new RuntimeException("Error creating Hadoop FS", e);
        }
    }

    private static Configuration extractHadoopConfig(Map<String, String> config) {
        Configuration conf = new Configuration();
        for (Map.Entry<String, String> e : config.entrySet()) {
            conf.set(e.getKey(), e.getValue());
        }
        return conf;
    }

    private static final Cache<IntPair, Float> defaultSimCache() {
        return CacheBuilder.newBuilder().maximumSize(DEFAULT_MAX_CACHE_SIZE_SIM).recordStats().build();
    }

    private static void hadoopError(Exception e) {
        throw new RuntimeException("Error with Hadoop FS", e);
    }

    @Override
    public String name() {
        return name;
    }

    private String componentNames(Map<String, Object> conf) {
        List<String> items = new ArrayList<>();
        for (String k : conf.keySet()) {
            items.add(String.format("%s:%s", k, conf.get(k)));
        }
        return Joiner.on(".").join(items);
    }

    @Override
    public synchronized Clusters computeClusters(Map<String, Object> conf, Set<ProductDesc> data) {
        setupCache(conf);
        ProductSimilarity msm = getProductSimilarity(conf, data);

        // Map[IntPair, Double] similarities
        Map<IntPair, Double> similarities = new HashMap<>();

        logger.info("computing similarities");
        final boolean shopHeuristic = (boolean) conf.get("shopHeuristic");
        int pairsComputed = computeSimilarities(data, shopHeuristic, msm, similarities);

        registerVariable("simsComputed", String.valueOf(pairsComputed));

        registerVariable("longName", name + "." + componentNames(conf));

        registerVariable("blockingScheme", blockingScheme.componentID());

        logger.info("calling clustering procedure");
        return clusteringProcedure.createClusters(conf, similarities, computeIndex(data));
    }

    private ProductSimilarity getProductSimilarity(Map<String, Object> conf, Set<ProductDesc> data) {
        if (sim != null) {
            return sim;
        } else {
            double alpha = (double) conf.get("alpha");
            double beta = (double) conf.get("beta");
            double kappa = (Double) conf.get("kappa");
            double lambda = (Double) conf.get("lambda");
            double gamma = (Double) conf.get("gamma");
            double mu = (Double) conf.get("mu");
            final boolean brandHeuristic = (boolean) conf.get("brandHeuristic");

            KeyMatcher msmKeyMatcher = new MsmKeyMatcher(gamma); // key sim and matching functions component for MSM
            String keyMatcherStr = conf.containsKey("keyMatcher") ? conf.get("keyMatcher").toString() : "msmKeyMatcher";
            KeyMatcher keyMatcher;
            if (keyMatcherStr.equals("Group7KeyMatcher")) {
                keyMatcher = new Group7KeyMatcher(msmKeyMatcher, conf, data);
            } else if (keyMatcherStr.equals("NickKeyMatcher")) {
                keyMatcher = new NickKeyMatcher(msmKeyMatcher, conf, data);
            } else {
                keyMatcher = msmKeyMatcher;
            }
            logger.info("Using key matcher '{}'", keyMatcher);

            Map<String, Double> c = getConfig(alpha, beta, kappa, lambda, gamma, mu);
            return new MsmSimilarity(brandHeuristic, c, keyMatcher, cache);
        }
    }

    private void setupCache(Map<String, Object> conf) {
        Map<String, Object> newMsmSimConfig = MsmConfig.filterCacheEffectiveKeys(conf);
        if (cachePath != null) { // only if we specified a cache path
            if (currentMsmSimConf == null || !currentMsmSimConf.equals(newMsmSimConfig)) {
                // no cache loaded or config is different
                String newConfigStr = Joiner.on(", ").withKeyValueSeparator(":").join(newMsmSimConfig);
                String cacheKeyName = CacheConfKey.get(newMsmSimConfig);
                Path cacheFile = new Path(cachePath, "msmSims__" + cacheKeyName + ".bin");
                try {
                    if (exists(cacheFile)) {
                        logger.info("Loading cache file {} (config {})", cacheFile, newConfigStr);
                        FSDataInputStream in = fs.open(cacheFile);
                        ObjectInputStream objReader = new ObjectInputStream(in);
                        TObjectFloatHashMap<IntPair> simMap = (TObjectFloatHashMap<IntPair>) objReader.readObject();
                        loadCache(simMap);
                        logger.info("Succesfully loaded cache!");
                    } else {
                        logger.info("Could not find cache file {} (config {})", cacheFile, newConfigStr);
                    }
                } catch (IOException | ClassNotFoundException e) {
                    hadoopError(e);
                }
            }
        }
        currentMsmSimConf = newMsmSimConfig;
    }

    private void loadCache(TObjectFloatHashMap<IntPair> simMap) {
        // remove currently loaded sims
        cache.invalidateAll();
        cache.cleanUp();
        simMap.forEachEntry(new TObjectFloatProcedure<IntPair>() { // fast iteration over map
            @Override
            public boolean execute(IntPair pair, float sim) {
                cache.put(pair, sim);
                return true;
            }
        });
    }

    private boolean exists(Path file) throws IOException {
        return fs.exists(file) && fs.isFile(file);
    }

    private int computeSimilarities(Set<ProductDesc> data, boolean shopHeuristic, ProductSimilarity msm, Map<IntPair, Double> similarities) {
        int pairsComputed = 0;
        BlocksMapping<ProductDesc> blocks = blockingScheme.getBlocks(data);
        Set<IntPair> pairs = new HashSet<>();
        for (Collection<ProductDesc> block : blocks.blocksAsCollection()) {
            ArrayList<ProductDesc> blockList = Lists.newArrayList(block);
            final int n = blockList.size();
            for (int i = 0; i < n; i++) {
                ProductDesc a = blockList.get(i);
                for (int j = i + 1; j < n; j++) {
                    ProductDesc b = blockList.get(j);
                    IntPair key = new IntPair(a.ID, b.ID);
                    if (!pairs.contains(key)) {
                        pairs.add(key); // skip this pair next time
                        double sim;
                        if (shopHeuristic && a.shop.equals(b.shop)) {
                            sim = Double.NEGATIVE_INFINITY;
                        } else {
                            ++pairsComputed;
                            sim = msm.computeSim(a, b);
                        }
                        similarities.put(key, sim);
                    }
                }
            }
        }
        return pairsComputed;
    }

    private Map<String, Double> getConfig(double alpha, double beta, double kappa, double lambda, double gamma, double mu) {
        Map<String, Double> ret = new HashMap<>();
        ret.put("alpha", alpha);
        ret.put("beta", beta);
        ret.put("kappa", kappa);
        ret.put("lambda", lambda);
        ret.put("gamma", gamma);
        ret.put("mu", mu);
        return Collections.unmodifiableMap(ret);
    }

    private TIntObjectMap<ProductDesc> computeIndex(Set<ProductDesc> products) {
        TIntObjectMap<ProductDesc> ret = new TIntObjectHashMap<>(products.size());
        for (ProductDesc p : products) {
            ret.put(p.ID, p);
        }
        return ret;
    }
}
