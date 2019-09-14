package com.damirvandic.sparker.students.group6;

import com.damirvandic.sparker.blocking.core.BlockingEvaluator;
import com.damirvandic.sparker.blocking.core.BlockingResults;
import com.damirvandic.sparker.blocking.core.ResultsFileWriter;
import com.damirvandic.sparker.blocking.core.ResultsWriter;
import com.damirvandic.sparker.blocking.schemes.BlockingScheme;
import com.damirvandic.sparker.core.ProductDesc;
import com.damirvandic.sparker.data.DataSet;
import com.damirvandic.sparker.data.Reader;
import com.google.common.collect.ImmutableSet;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.util.Set;
import java.util.concurrent.ExecutionException;

public class Group6BlockingEvaluationRunner {
    private static final Logger log = LoggerFactory.getLogger(Group6BlockingEvaluationRunner.class);

    public static void main(String[] args) throws InterruptedException, IOException, ExecutionException {
        final long start = System.currentTimeMillis();
        final int nrOfBootstraps = 1;

        DataSet dataSet_a = Reader.readDataSet("TV's 4 shops", "./hdfs/TVs-all-merged.json");
        ResultsWriter writer_a = new ResultsFileWriter(new File("./group6_blocking_results.csv"));
        log.info("Running data set '{}' with {} products", dataSet_a.name(), dataSet_a.jDescriptions().size());
        BlockingResults results_a = new BlockingEvaluator(dataSet_a, writer_a, nrOfBootstraps).run(getSchemes());
        long end_a = System.currentTimeMillis();
        writer_a.close();
        System.out.printf("DONE in %d ms\n", end_a - start);
        System.out.println(results_a);
    }

    private static Set<BlockingScheme<ProductDesc>> getSchemes() throws ExecutionException {
        ImmutableSet.Builder<BlockingScheme<ProductDesc>> builder = ImmutableSet.builder();
        // here you can add all combinations
        double minhashSize = 0.5;
        for (double lshThreshold = 0.05; lshThreshold <= 0.95; lshThreshold += 0.05) {
            builder.add(new Group6BlockingScheme(lshThreshold, minhashSize));
        }
        return builder.build();
    }
}
