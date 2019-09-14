package com.damirvandic.sparker.blocking.core;

import com.damirvandic.sparker.eval.Results;
import com.google.common.base.Joiner;
import com.google.common.collect.Lists;

import java.util.ArrayList;

public class BlockingResult {
    private final Results clusterResults;
    private final int blocksCount;
    private final int origCombCount;
    private final int reducedCombCount;
    private final int totalDuplicates;
    private final int foundDuplicates;
    private final long duration;
    private final double pairsCompleteness;
    private final double pairsQualityExclDupl;
    private final double percComputedExcDupl;

    public BlockingResult(Results clusterResults,
                          int blocksCount,
                          int origCombCount,
                          int reducedCombCount,
                          int totalDuplicates,
                          int foundDuplicates,
                          long millis) {
        this.clusterResults = clusterResults;
        this.blocksCount = blocksCount;
        this.origCombCount = origCombCount;
        this.reducedCombCount = reducedCombCount;
        this.totalDuplicates = totalDuplicates;
        this.foundDuplicates = foundDuplicates;

        // computed
        this.pairsCompleteness = foundDuplicates / (double) totalDuplicates;
        this.pairsQualityExclDupl = foundDuplicates / (double) reducedCombCount;
        this.percComputedExcDupl = reducedCombCount / (double) origCombCount;
        this.duration = millis;
    }

    public static String printColumnNames(String sep) {
        ArrayList<String> cols = Lists.newArrayList(
                "blocksCount",
                "origCombCount",
                "reducedCombCount",
                "totalDuplicates",
                "foundDuplicates",
                "duration",
                "pairsQuality",
                "percComputed",
                "pairsCompleteness",
                "F1");
        return Joiner.on(sep).join(cols);
    }

    public String printRow(String sep) {
        ArrayList<String> cols = Lists.newArrayList(
                "" + blocksCount,
                "" + origCombCount,
                "" + reducedCombCount,
                "" + totalDuplicates,
                "" + foundDuplicates,
                "" + duration,
                String.format("%.8f", pairsQualityExclDupl),
                String.format("%.8f", percComputedExcDupl),
                String.format("%.8f", pairsCompleteness),
                String.format("%.8f", clusterResults.f1()));
        return Joiner.on(sep).join(cols);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        BlockingResult that = (BlockingResult) o;

        if (blocksCount != that.blocksCount) return false;
        if (duration != that.duration) return false;
        if (foundDuplicates != that.foundDuplicates) return false;
        if (reducedCombCount != that.reducedCombCount) return false;
        if (origCombCount != that.origCombCount) return false;
        if (Double.compare(that.pairsCompleteness, pairsCompleteness) != 0) return false;
        if (Double.compare(that.pairsQualityExclDupl, pairsQualityExclDupl) != 0) return false;
        if (Double.compare(that.percComputedExcDupl, percComputedExcDupl) != 0) return false;
        if (totalDuplicates != that.totalDuplicates) return false;
        if (!clusterResults.equals(that.clusterResults)) return false;

        return true;
    }

    @Override
    public int hashCode() {
        int result;
        long temp;
        result = clusterResults.hashCode();
        result = 31 * result + blocksCount;
        result = 31 * result + origCombCount;
        result = 31 * result + reducedCombCount;
        result = 31 * result + totalDuplicates;
        result = 31 * result + foundDuplicates;
        result = 31 * result + (int) (duration ^ (duration >>> 32));
        temp = Double.doubleToLongBits(pairsCompleteness);
        result = 31 * result + (int) (temp ^ (temp >>> 32));
        result = 31 * result + (int) (temp ^ (temp >>> 32));
        temp = Double.doubleToLongBits(pairsQualityExclDupl);
        result = 31 * result + (int) (temp ^ (temp >>> 32));
        result = 31 * result + (int) (temp ^ (temp >>> 32));
        temp = Double.doubleToLongBits(percComputedExcDupl);
        result = 31 * result + (int) (temp ^ (temp >>> 32));
        return result;
    }

    public Results getClusterResults() {
        return clusterResults;
    }

    @Override
    public String toString() {
        return String.format("[F1=%.3f, PC=%.3f, PQ=%.3f, %.3f%%]", clusterResults.f1() * 100, pairsCompleteness * 100, pairsQualityExclDupl * 100, percComputedExcDupl * 100);
    }
}
