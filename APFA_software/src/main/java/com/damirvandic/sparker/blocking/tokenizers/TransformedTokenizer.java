package com.damirvandic.sparker.blocking.tokenizers;

import com.damirvandic.sparker.blocking.core.Block;

import java.util.Set;

public class TransformedTokenizer<T> extends AbstractCachingTokenizer<T> {
    private final TokenizerTransformer transformer;
    private final Tokenizer<T> tokenizer;

    public TransformedTokenizer(Tokenizer<T> tokenizer, TokenizerTransformer transformer) {
        super(0, 0); // no cache
        this.tokenizer = tokenizer;
        this.transformer = transformer;
    }

    @Override
    protected Set<Block> extractBlocks(T value) {
        return transformer.transform(tokenizer.tokenize(value));
    }

    @Override
    public String componentID() {
        return tokenizer.componentID() + "." + transformer.componentID();
    }
}
