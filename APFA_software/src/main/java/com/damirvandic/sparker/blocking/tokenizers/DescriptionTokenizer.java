package com.damirvandic.sparker.blocking.tokenizers;

import com.damirvandic.sparker.blocking.core.Block;
import com.damirvandic.sparker.core.ProductDesc;
import com.google.common.collect.ImmutableSet;

import java.util.Set;

public class DescriptionTokenizer extends AbstractCachingTokenizer<ProductDesc> {
    private final Tokenizer<String> tokenizer;

    public DescriptionTokenizer(Tokenizer<String> tokenizer) {
        super(0, 0); // no cache
        this.tokenizer = tokenizer;
    }

    @Override
    protected Set<Block> extractBlocks(ProductDesc p) {
        ImmutableSet.Builder<Block> builder = ImmutableSet.builder();
        for (String value : p.featuresMap.values()) {
            builder.addAll(tokenizer.tokenize(value));
        }
        return builder.build();
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        DescriptionTokenizer that = (DescriptionTokenizer) o;

        if (!tokenizer.equals(that.tokenizer)) return false;

        return true;
    }

    @Override
    public int hashCode() {
        return tokenizer.hashCode();
    }

    @Override
    public String toString() {
        return componentID();
    }

    @Override
    public String componentID() {
        return "desc:" + tokenizer.componentID();
    }
}
