package com.damirvandic.sparker.blocking.tokenizers;

import com.damirvandic.sparker.blocking.core.Block;
import com.damirvandic.sparker.blocking.util.BlockingUtils;
import com.damirvandic.sparker.util.CombinationsFinder;
import com.google.common.collect.ImmutableSet;

import java.util.ArrayList;
import java.util.List;
import java.util.Set;

public class CombinationsTokenizerTransformer implements TokenizerTransformer {
    private final int combinationCount;

    public CombinationsTokenizerTransformer(int combinationCount) {
        this.combinationCount = combinationCount;
    }

    @Override
    public Set<Block> transform(Set<Block> originalBlocks) {
        List<Block> blocks = new ArrayList<>(originalBlocks);
        ImmutableSet.Builder<Block> builder = ImmutableSet.builder();
        Set<Set<Block>> combinations = CombinationsFinder.getCombinationsFor(blocks, combinationCount);
        for (Set<Block> newBlocks : combinations) {
            builder.add(BlockingUtils.newBlockFrom(newBlocks));
        }
        return builder.build();
    }

    @Override
    public String componentID() {
        return "cbs" + combinationCount;
    }
}
