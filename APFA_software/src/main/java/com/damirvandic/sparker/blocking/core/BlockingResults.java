package com.damirvandic.sparker.blocking.core;

import com.damirvandic.sparker.blocking.schemes.BlockingScheme;
import com.google.common.collect.ImmutableTable;
import com.google.common.collect.Table;

public class BlockingResults {
    public final ImmutableTable<Integer, BlockingScheme, BlockingResult> resultsTable;

    public BlockingResults(Table<Integer, BlockingScheme, BlockingResult> results) {
        this.resultsTable = ImmutableTable.copyOf(results);
    }

    @Override
    public String toString() {
        return resultsTable.toString();
    }
}
