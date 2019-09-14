package com.damirvandic.sparker.blocking.tokenizers;

import com.damirvandic.sparker.blocking.core.Block;
import com.damirvandic.sparker.blocking.core.Component;

import java.util.Set;

public interface Tokenizer<T> extends Component {
    Set<Block> tokenize(T obj);

    Tokenizer pipe(TokenizerTransformer t);

    void clearCache();
}
