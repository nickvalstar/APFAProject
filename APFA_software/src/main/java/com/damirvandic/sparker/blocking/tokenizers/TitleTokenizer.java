package com.damirvandic.sparker.blocking.tokenizers;

import com.damirvandic.sparker.blocking.core.Block;
import com.damirvandic.sparker.core.ProductDesc;

import java.util.Set;

public class TitleTokenizer extends AbstractCachingTokenizer<ProductDesc> {
    private final Tokenizer<String> tokenizer;

    public TitleTokenizer(Tokenizer<String> tokenizer) {
        super(0, 0); // no cache
        this.tokenizer = tokenizer;
    }

    @Override
    protected Set<Block> extractBlocks(ProductDesc p) {
        return tokenizer.tokenize(p.title);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        TitleTokenizer that = (TitleTokenizer) o;

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
        return "t:" + tokenizer.componentID();
    }
}
