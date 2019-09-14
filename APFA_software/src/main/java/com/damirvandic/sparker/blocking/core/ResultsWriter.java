package com.damirvandic.sparker.blocking.core;

import com.damirvandic.sparker.blocking.schemes.BlockingScheme;

public interface ResultsWriter {
    void write(int bootstrapNr, BlockingScheme scheme, BlockingResult result);

    void close();
}
