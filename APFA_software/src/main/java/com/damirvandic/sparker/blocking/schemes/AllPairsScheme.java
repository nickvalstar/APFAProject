package com.damirvandic.sparker.blocking.schemes;


import com.damirvandic.sparker.blocking.core.Block;
import com.damirvandic.sparker.blocking.core.BlocksMapping;
import com.damirvandic.sparker.core.ProductDesc;

import java.util.Set;

public class AllPairsScheme implements BlockingScheme<ProductDesc> {
    private static final Block block = new Block("THE_ONE_BLOCK");

    @Override
    public BlocksMapping<ProductDesc> getBlocks(Set<ProductDesc> data) {
        BlocksMapping.Builder<ProductDesc> builder = BlocksMapping.builder();
        for (ProductDesc p : data) {
            builder.add(p, block);
        }
        return builder.build();
    }

    @Override
    public void clearCache() {

    }

    @Override
    public String componentID() {
        return "ap";
    }
}
