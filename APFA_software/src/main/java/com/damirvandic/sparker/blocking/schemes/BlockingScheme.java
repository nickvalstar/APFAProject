package com.damirvandic.sparker.blocking.schemes;

import com.damirvandic.sparker.blocking.core.BlocksMapping;
import com.damirvandic.sparker.blocking.core.Component;

import java.util.Set;

public interface BlockingScheme<T> extends Component {
    BlocksMapping<T> getBlocks(Set<T> data);

    void clearCache();
}
