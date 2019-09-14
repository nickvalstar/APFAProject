package com.damirvandic.sparker.blocking.schemes;

import com.damirvandic.sparker.blocking.core.Block;
import com.damirvandic.sparker.blocking.tokenizers.Tokenizer;

import java.util.Set;

public class TokenBlockingScheme<T> extends AbstractBlockingScheme<T> {
    private final Tokenizer<T> tokenizer;

    public TokenBlockingScheme(Tokenizer<T> tokenizer) {
        this.tokenizer = tokenizer;
    }

    @Override
    public Set<Block> getBlocks(T p) {
        return tokenizer.tokenize(p);
    }

    @Override
    public String componentID() {
        return tokenizer.componentID();
    }

    @Override
    public String toString() {
        return componentID();
    }
}
