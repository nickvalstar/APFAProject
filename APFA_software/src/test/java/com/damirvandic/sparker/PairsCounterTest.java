package com.damirvandic.sparker;

import com.damirvandic.sparker.blocking.util.PairsCounter;
import com.damirvandic.sparker.core.ProductDesc;
import com.google.common.base.Function;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterables;
import org.apache.commons.lang3.tuple.Pair;
import org.junit.Test;

import javax.annotation.Nullable;
import java.util.Set;

import static org.junit.Assert.assertEquals;

public class PairsCounterTest {
    private ImmutableSet<Set<ProductDesc>> transform(Set<Set<String>> input) {
        return ImmutableSet.copyOf(Iterables.transform(input, new Function<Set<String>, Set<ProductDesc>>() {
            @Nullable
            @Override
            public Set<ProductDesc> apply(Set<String> input) {
                return ImmutableSet.copyOf(Iterables.transform(input, new Function<String, ProductDesc>() {
                    @Nullable
                    @Override
                    public ProductDesc apply(@Nullable String input) {
                        return ProdUtil.ps(input);
                    }
                }));
            }
        }));
    }

    @Test
    public void testComplexCounting() {
        Set<String> b1 = ImmutableSet.of("a1", "a2");
        Set<String> b2 = ImmutableSet.of("a1", "b2", "b3");
        Set<String> b3 = ImmutableSet.of("a1", "c1", "b3");
        Set<Set<String>> input = ImmutableSet.of(b1, b2, b3);
        PairsCounter counter = new PairsCounter();
        Pair<Integer, Integer> counts = counter.combCountsForBlocks(transform(input));
        assertEquals(5, counts.getLeft().intValue());
        assertEquals(4, counts.getRight().intValue());
    }


}
