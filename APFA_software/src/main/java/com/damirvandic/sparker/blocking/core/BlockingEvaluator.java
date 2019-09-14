package com.damirvandic.sparker.blocking.core;

import com.damirvandic.sparker.blocking.schemes.BlockingScheme;
import com.damirvandic.sparker.blocking.util.PairsCounter;
import com.damirvandic.sparker.core.Clusters;
import com.damirvandic.sparker.core.ClustersBuilder;
import com.damirvandic.sparker.core.ConnectedComponentsFinder;
import com.damirvandic.sparker.core.ProductDesc;
import com.damirvandic.sparker.data.DataSet;
import com.damirvandic.sparker.eval.*;
import com.damirvandic.sparker.util.IntPair;
import com.google.common.base.Joiner;
import com.google.common.collect.*;
import org.apache.commons.lang3.tuple.ImmutablePair;
import org.apache.commons.lang3.tuple.Pair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;

public class BlockingEvaluator {
    public static final long DEFAULT_SEED = 2566864879360712787l;
    private final Logger log = LoggerFactory.getLogger(getClass());
    private final int nrOfBootstraps;
    private final List<Sample> bootstrapSamples;
    private final Multimap<Integer, Integer> duplicatesMap;
    private final PairsCounter pairsCounter;
    private final ResultsWriter writer;
    private final String dataSetName;

    public BlockingEvaluator(DataSet dataSet, ResultsWriter writer, int nrOfBootstraps, long seed) {
        this.dataSetName = dataSet.name();
        this.duplicatesMap = createDuplicatesIndex(dataSet.clusters(), dataSet.jDescriptions());
        this.writer = writer;
        this.nrOfBootstraps = nrOfBootstraps;
        this.bootstrapSamples = createSamples(dataSet, seed);
        this.pairsCounter = new PairsCounter();
    }

    public BlockingEvaluator(DataSet dataSet, ResultsWriter writer, int nrOfBootstraps) {
        this(dataSet, writer, nrOfBootstraps, DEFAULT_SEED);
    }

    public BlockingEvaluator(DataSet dataSet, int nrOfBootstraps) {
        this(dataSet, null, nrOfBootstraps);
    }

    private List<Sample> createSamples(DataSet dataSet, long seed) {
        ImmutableList.Builder<Sample> builder = new ImmutableList.Builder<>();
        Sampler sampler = new SamplerImpl(seed);
        for (int i = 0; i < nrOfBootstraps; i++) {
            builder.add(sampler.sample(dataSet));
        }
        return builder.build();
    }

    private Multimap<Integer, Integer> createDuplicatesIndex(Clusters clusters, Set<ProductDesc> productDescs) {
        Multimap<Integer, Integer> ret = HashMultimap.create(productDescs.size(), 4); // 4 shops approx.
        for (Set<ProductDesc> cluster : clusters.asJavaSet()) {
            ProductDesc[] clusterArray = cluster.toArray(new ProductDesc[cluster.size()]);
            for (int i = 0; i < clusterArray.length; i++) {
                ProductDesc a = clusterArray[i];
                for (int j = i + 1; j < clusterArray.length; j++) {
                    ProductDesc b = clusterArray[j];
                    ret.put(a.ID, b.ID);
                    ret.put(b.ID, a.ID);
                }
            }

        }
        return ret;
    }

    public <T> BlockingResults run(Collection<BlockingScheme<T>> blockingSchemes) {
        log.info("Running {} schemes:\n{}", blockingSchemes.size(), Joiner.on('\n').join(blockingSchemes));

        final int schemesCount = blockingSchemes.size();
        BlockingScheme[] schemesArray = blockingSchemes.toArray(new BlockingScheme[blockingSchemes.size()]);
        BlockingResultsBuilder builder = new BlockingResultsBuilder();

        for (int i = 0; i < schemesArray.length; i++) {
            BlockingScheme scheme = schemesArray[i];
            for (int bootstrapNr = 0; bootstrapNr < nrOfBootstraps; bootstrapNr++) {
                Sample sample = bootstrapSamples.get(bootstrapNr);
                BlockingResult result = process(scheme, sample);
                builder.addResult(bootstrapNr, scheme, result);
                if (writer != null) {
                    writer.write(bootstrapNr, scheme, result);
                }
                log.debug("Done with bootstrap {}/{} ({})", bootstrapNr, nrOfBootstraps, scheme);
            }
            log.info("[{}] Done with scheme {}   ({}/{})", dataSetName, scheme, i, schemesCount);
            scheme.clearCache();
        }
        return builder.build();
    }

    private BlockingResult process(BlockingScheme<ProductDesc> scheme, Sample sample) {
        long start = System.nanoTime();
        BlocksMapping<ProductDesc> blockMappings = scheme.getBlocks(sample.jTest());
        long end = System.nanoTime();
        return evaluate(blockMappings, sample.jTest(), sample.testClusters(), (end - start) / (long) 1e6);
    }

    private BlockingResult evaluate(BlocksMapping<ProductDesc> blockMappings, Set<ProductDesc> productDescs, Clusters correctClusters, long millis) {
        Pair<Integer, Integer> origCombinationCounts = pairsCounter.combCountsForBlocks(ImmutableSet.of(productDescs));
        int origCombCount = origCombinationCounts.getRight();

        List<Block> higherThanOne = new ArrayList<>();
        for (Block block : blockMappings.blockToEntities.keySet()) {
            if (blockMappings.blockToEntities.get(block).size() > 1) {
                higherThanOne.add(block);
            }
        }
        Pair<Integer, Integer> reducedCombinationCounts = pairsCounter.combCountsForBlocks(blockMappings.blocksAsCollection());
        int reducedCombCount = reducedCombinationCounts.getRight();

        Results clusterResults = computeClusterResults(blockMappings, correctClusters);

        int totalDuplicates = findDuplicates(Collections.<Collection<ProductDesc>>singleton(productDescs)).size();
        int foundDuplicates = clusterResults.TP(); // TP = found duplicates, findDuplicates() does not count transitive duplicate relationships

        int blocksCount = blockMappings.blockCount();

        return new BlockingResult(
                clusterResults,
                blocksCount,
                origCombCount,
                reducedCombCount,
                totalDuplicates,
                foundDuplicates,
                millis);
    }

    private Set<IntPair> findDuplicates(Collection<Collection<ProductDesc>> blocks) {
        Set<IntPair> ret = new HashSet<>();
        for (Collection<ProductDesc> block : blocks) {
            ProductDesc[] blockArray = block.toArray(new ProductDesc[block.size()]);
            for (int i = 0; i < blockArray.length; i++) {
                ProductDesc a = blockArray[i];
                for (int j = i + 1; j < blockArray.length; j++) {
                    ProductDesc b = blockArray[j];
                    if (duplicatesMap.containsEntry(a.ID, b.ID)) { // or <b.ID,a.ID>
                        ret.add(new IntPair(a.ID, b.ID));
                    }
                }
            }
        }
        return ret;
    }

    private Results computeClusterResults(BlocksMapping<ProductDesc> blocksMap, Clusters correctClusters) {
        Collection<Collection<ProductDesc>> blocks = blocksMap.blocksAsCollection();
        Clusters clusters = getClusters(blocks);
        return new ClustersEvalImpl().evaluate(clusters, correctClusters);
    }


    private Clusters getClusters(Collection<Collection<ProductDesc>> groups) {
        Set<Pair<ProductDesc, ProductDesc>> edges = new HashSet<>();
        for (Collection<ProductDesc> group : groups) {
            ProductDesc[] productDescs = group.toArray(new ProductDesc[group.size()]);
            // add edge to themselves to include all clusters in result
            for (ProductDesc p : group) {
                edges.add(new ImmutablePair<>(p, p));
            }
            for (int i = 0; i < productDescs.length; i++) {
                ProductDesc a = productDescs[i];
                for (int j = i + 1; j < productDescs.length; j++) {
                    ProductDesc b = productDescs[j];
                    if (a.modelID.equals(b.modelID)) {
                        edges.add(new ImmutablePair<>(a, b));
                    }
                }
            }
        }
        Set<Set<ProductDesc>> clusterSets = new ConnectedComponentsFinder<>(edges).findClusters();
        return new ClustersBuilder().fromSets(clusterSets);
    }

    class BlockingResultsBuilder {

        private final Table<Integer, BlockingScheme, BlockingResult> results;

        BlockingResultsBuilder() {
            results = HashBasedTable.create();
        }

        public BlockingResults build() {
            return new BlockingResults(results);
        }

        public void addResult(int bootstrapNr, BlockingScheme scheme, BlockingResult result) {
            this.results.put(bootstrapNr, scheme, result);
        }
    }
}
