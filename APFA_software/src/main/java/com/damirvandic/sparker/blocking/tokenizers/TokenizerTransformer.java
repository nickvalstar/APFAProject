package com.damirvandic.sparker.blocking.tokenizers;

import com.damirvandic.sparker.blocking.core.Block;
import com.damirvandic.sparker.blocking.core.Component;

import java.util.Set;

public interface TokenizerTransformer extends Component {
    Set<Block> transform(Set<Block> originalBlocks);
}
