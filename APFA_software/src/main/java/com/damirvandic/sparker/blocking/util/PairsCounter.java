package com.damirvandic.sparker.blocking.util;

import com.damirvandic.sparker.core.ProductDesc;
import com.damirvandic.sparker.util.IntPair;
import com.google.common.collect.Lists;
import org.apache.commons.lang3.tuple.ImmutablePair;
import org.apache.commons.lang3.tuple.Pair;

import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

public class PairsCounter {
    /**
     * @param blocks
     * @return count including duplicate comparisons (left) and count excluding duplicates (right)
     */
    public Pair<Integer, Integer> combCountsForBlocks(Collection<? extends Collection<ProductDesc>> blocks) {
        Set<IntPair> visitedPairs = new HashSet<>();
        int count_including_duplicates = computeIncluding(blocks, visitedPairs);
        int count_excluding_duplicates = visitedPairs.size();
        return new ImmutablePair<>(count_including_duplicates, count_excluding_duplicates);
    }

    private int computeIncluding(Collection<? extends Collection<ProductDesc>> blocks, Set<IntPair> visitedPairs) {
        int count_including_duplicates = 0;
        for (Collection<ProductDesc> block : blocks) {
            List<ProductDesc> list = Lists.newArrayList(block);
            final int n = list.size();
            for (int i = 0; i < n; i++) {
                ProductDesc a = list.get(i);
                for (int j = i + 1; j < n; j++) {
                    ProductDesc b = list.get(j);
                    if (!a.shop.equals(b.shop)) {
                        ++count_including_duplicates;
                        visitedPairs.add(new IntPair(a.ID, b.ID));
                    }
                }
            }
        }
        return count_including_duplicates;
    }
}
