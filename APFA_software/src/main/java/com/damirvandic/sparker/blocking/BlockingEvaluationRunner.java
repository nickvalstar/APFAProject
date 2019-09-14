package com.damirvandic.sparker.blocking;

import com.damirvandic.sparker.blocking.core.BlockingEvaluator;
import com.damirvandic.sparker.blocking.core.BlockingResults;
import com.damirvandic.sparker.blocking.core.ResultsFileWriter;
import com.damirvandic.sparker.blocking.core.ResultsWriter;
import com.damirvandic.sparker.blocking.schemes.BlockingScheme;
import com.damirvandic.sparker.blocking.schemes.BlockingSchemes;
import com.damirvandic.sparker.blocking.schemes.TokenBlockingScheme;
import com.damirvandic.sparker.blocking.tokenizers.*;
import com.damirvandic.sparker.core.ProductDesc;
import com.damirvandic.sparker.data.DataSet;
import com.damirvandic.sparker.data.Reader;
import com.google.common.collect.ImmutableSet;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import uk.ac.shef.wit.simmetrics.tokenisers.TokeniserQGram2;
import uk.ac.shef.wit.simmetrics.tokenisers.TokeniserQGram3;

import java.io.File;
import java.io.IOException;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

public class BlockingEvaluationRunner {
    private static final Logger log = LoggerFactory.getLogger(BlockingEvaluationRunner.class);

    public static void main(String[] args) throws InterruptedException, IOException, ExecutionException {
        final long start = System.currentTimeMillis();
        final int nrOfBootstraps = 40;

        ExecutorService taskExecutor = Executors.newFixedThreadPool(4);

        taskExecutor.execute(new Runnable() {
            @Override
            public void run() {
                try {
                    DataSet dataSet_a = Reader.readDataSet("TV's 4 shops", "./hdfs/TVs-all-merged.json");
                    ResultsWriter writer_a = new ResultsFileWriter(new File("./results_4shops_extra.csv"));
                    log.info("Running data set '{}' with {} products", dataSet_a.name(), dataSet_a.jDescriptions().size());
                    BlockingResults results_a = new BlockingEvaluator(dataSet_a, writer_a, nrOfBootstraps).run(getSchemesExtra());
                    long end_a = System.currentTimeMillis();
                    writer_a.close();
                    System.out.printf("DONE in %d ms\n", end_a - start);
                    System.out.println(results_a);
                } catch (IOException | ExecutionException e) {
                    e.printStackTrace();
                }
            }
        });

//        taskExecutor.execute(new Runnable() {
//            @Override
//            public void run() {
//                try {
//                    DataSet dataSet_b = Reader.readDataSet("Abt-Buy", "./hdfs/Abt-Buy/dataset.json");
//                    ResultsWriter writer_b = new ResultsFileWriter(new File("./results_abt-buy.csv"));
//                    log.info("Running data set '{}' with {} products", dataSet_b.name(), dataSet_b.jDescriptions().size());
//                    BlockingResults results_b = new BlockingEvaluator(dataSet_b, writer_b, nrOfBootstraps).run(getSchemes());
//                    long end_b = System.currentTimeMillis();
//                    writer_b.close();
//                    System.out.printf("DONE in %d ms\n", end_b - start);
//                    System.out.println(results_b);
//                } catch (IOException | ExecutionException e) {
//                    e.printStackTrace();
//                }
//            }
//        });
//
//        taskExecutor.execute(new Runnable() {
//            @Override
//            public void run() {
//                try {
//                    DataSet dataSet_c = Reader.readDataSet("Amazon-GoogleProducts", "./hdfs/Amazon-GoogleProducts/dataset.json");
//                    ResultsWriter writer_c = new ResultsFileWriter(new File("./results_amazon-google.csv"));
//                    log.info("Running data set '{}' with {} products", dataSet_c.name(), dataSet_c.jDescriptions().size());
//                    BlockingResults results_c = new BlockingEvaluator(dataSet_c, writer_c, nrOfBootstraps).run(getSchemes());
//                    long end_c = System.currentTimeMillis();
//                    writer_c.close();
//                    System.out.printf("DONE in %d ms\n", end_c - start);
//                    System.out.println(results_c);
//                } catch (IOException | ExecutionException e) {
//                    e.printStackTrace();
//                }
//            }
//        });

        taskExecutor.shutdown();
        try {
            taskExecutor.awaitTermination(Long.MAX_VALUE, TimeUnit.NANOSECONDS);
        } catch (InterruptedException e) {
            throw e;
        }
    }

    private static Set<BlockingScheme<ProductDesc>> getSchemesExtra() throws ExecutionException {
        ImmutableSet.Builder<BlockingScheme<ProductDesc>> builder = ImmutableSet.builder();
        builder.add(new TokenBlockingScheme<>(new TitleTokenizer(new QgramTokenizer(new TokeniserQGram2(), "qg2"))));
        builder.add(new TokenBlockingScheme<>(new DescriptionTokenizer(new QgramTokenizer(new TokeniserQGram2(), "qg2"))));

        builder.add(new TokenBlockingScheme<>(new TitleTokenizer(new QgramTokenizer(new TokeniserQGram3(), "qg3"))));
        builder.add(new TokenBlockingScheme<>(new DescriptionTokenizer(new QgramTokenizer(new TokeniserQGram3(), "qg3"))));

        builder.add(BlockingSchemes.union(new TokenBlockingScheme<>(new TitleTokenizer(new QgramTokenizer(new TokeniserQGram2(), "qg2"))), new TokenBlockingScheme<>(new DescriptionTokenizer(new QgramTokenizer(new TokeniserQGram2(), "qg2")))));
        builder.add(BlockingSchemes.union(new TokenBlockingScheme<>(new TitleTokenizer(new QgramTokenizer(new TokeniserQGram2(), "qg2"))), new TokenBlockingScheme<>(new TitleTokenizer(new QgramTokenizer(new TokeniserQGram3(), "qg3")))));
        builder.add(BlockingSchemes.union(new TokenBlockingScheme<>(new TitleTokenizer(new QgramTokenizer(new TokeniserQGram2(), "qg2"))), new TokenBlockingScheme<>(new DescriptionTokenizer(new QgramTokenizer(new TokeniserQGram3(), "qg3")))));

        builder.add(BlockingSchemes.union(new TokenBlockingScheme<>(new DescriptionTokenizer(new QgramTokenizer(new TokeniserQGram2(), "qg2"))), new TokenBlockingScheme<>(new TitleTokenizer(new QgramTokenizer(new TokeniserQGram3(), "qg3")))));
        builder.add(BlockingSchemes.union(new TokenBlockingScheme<>(new DescriptionTokenizer(new QgramTokenizer(new TokeniserQGram2(), "qg2"))), new TokenBlockingScheme<>(new DescriptionTokenizer(new QgramTokenizer(new TokeniserQGram3(), "qg3")))));

        builder.add(BlockingSchemes.union(new TokenBlockingScheme<>(new TitleTokenizer(new QgramTokenizer(new TokeniserQGram3(), "qg3"))),new TokenBlockingScheme<>(new DescriptionTokenizer(new QgramTokenizer(new TokeniserQGram3(), "qg3")))));

        return builder.build();
    }

    private static Set<BlockingScheme<ProductDesc>> getSchemes() throws ExecutionException {
        ImmutableSet.Builder<BlockingScheme<ProductDesc>> builder = ImmutableSet.builder();
        int k = 6;
//        title[cl,mw]
        builder.add(t_cl());
        builder.add(t_mw());
        // title[cl|comb]
        for (int i = 2; i <= k; i++) {
            builder.add(t_cl_comb(i));
        }
        // title[mw|comb]
        for (int i = 2; i <= k; i++) {
            builder.add(t_mw_comb(i));
        }
//
        // desc[cl,mw]
        builder.add(desc_cl());
        builder.add(desc_mw());

//        desc[cl|comb] // to slow !
//        for (int i = 2; i <= k; i++) {
//            builder.add(desc_cl_comb(i));
//        }

        // desc[mw|comb]
        for (int i = 2; i <= k; i++) {
            builder.add(desc_mw_comb(i));
        }

        // UNIONS
        // title[cl] + desc[cl,mw,mw|comb]
        builder.add(BlockingSchemes.union(t_cl(), desc_cl()));
        builder.add(BlockingSchemes.union(t_cl(), desc_mw()));
        for (int i = 2; i <= k; i++) {
            builder.add(BlockingSchemes.union(t_cl(), desc_mw_comb(i)));
        }

        // title[mw] + desc[cl,mw,mw|comb]
        builder.add(BlockingSchemes.union(t_mw(), desc_cl()));
        builder.add(BlockingSchemes.union(t_mw(), desc_mw()));
        for (int i = 2; i <= k; i++) { // desc[mw|comb]
            builder.add(BlockingSchemes.union(t_mw(), desc_mw_comb(i)));
        }

        // title[cl|comb] + desc[cl,mw,mw|comb]
        for (int i = 2; i <= k; i++) {
            builder.add(BlockingSchemes.union(t_cl_comb(i), desc_cl()));
            builder.add(BlockingSchemes.union(t_cl_comb(i), desc_mw()));
            for (int j = 2; j <= k; j++) { // desc[mw|comb]
                builder.add(BlockingSchemes.union(t_cl_comb(i), desc_mw_comb(j)));
            }
        }

        // title[mw|comb] + desc[cl,mw,mw|comb]
        for (int i = 2; i <= k; i++) {
            builder.add(BlockingSchemes.union(t_mw_comb(i), desc_cl()));
            builder.add(BlockingSchemes.union(t_mw_comb(i), desc_mw()));
            for (int j = 2; j <= k; j++) { // desc[mw|comb]
                builder.add(BlockingSchemes.union(t_mw_comb(i), desc_mw_comb(j)));
            }
        }

        // PRODUCTS
        // title[cl] * desc[cl,mw,mw|comb]
        builder.add(BlockingSchemes.product(t_cl(), desc_cl()));
        builder.add(BlockingSchemes.product(t_cl(), desc_mw()));
        for (int i = 2; i <= k; i++) {
            builder.add(BlockingSchemes.product(t_cl(), desc_mw_comb(i)));
        }

        // title[mw] * desc[cl,mw,mw|comb]
        builder.add(BlockingSchemes.product(t_mw(), desc_cl()));
        builder.add(BlockingSchemes.product(t_mw(), desc_mw()));
        for (int i = 2; i <= k; i++) { // desc[mw|comb]
            builder.add(BlockingSchemes.product(t_mw(), desc_mw_comb(i)));
        }

        // title[cl|comb] * desc[cl,mw,mw|comb]
        int k_special_t = 4;
        int k_special_desc = 6;

        for (int i = 2; i <= k_special_t; i++) {
            builder.add(BlockingSchemes.product(t_cl_comb(i), desc_cl()));
            builder.add(BlockingSchemes.product(t_cl_comb(i), desc_mw()));
            for (int j = 2; j <= k_special_desc; j++) { // desc[mw|comb]
                builder.add(BlockingSchemes.product(t_cl_comb(i), desc_mw_comb(j)));
            }
        }

        // title[mw|comb] * desc[cl,mw,mw|comb]
        for (int i = 2; i <= k; i++) {
            builder.add(BlockingSchemes.product(t_mw_comb(i), desc_cl()));
            builder.add(BlockingSchemes.product(t_mw_comb(i), desc_mw()));
            for (int j = 2; j <= k; j++) { // desc[mw|comb]
                builder.add(BlockingSchemes.product(t_mw_comb(i), desc_mw_comb(j)));
            }
        }

        return builder.build();
    }

    private static Set<BlockingScheme<ProductDesc>> getPrewarmSchemes() throws ExecutionException {
        ImmutableSet.Builder<BlockingScheme<ProductDesc>> builder = ImmutableSet.builder();
        builder.add(t_mw());
        builder.add(t_cl());
        builder.add(desc_cl());
        builder.add(desc_mw());
        return builder.build();
    }

    private static BlockingScheme<ProductDesc> desc_mw_comb(final int i) throws ExecutionException {
        return new TokenBlockingScheme<>(new DescriptionTokenizer(new TransformedTokenizer<>(new ModelWordsTokenizer(), new CombinationsTokenizerTransformer(i))));
    }

    private static BlockingScheme<ProductDesc> desc_cl_comb(final int i) throws ExecutionException {
        return new TokenBlockingScheme<>(new DescriptionTokenizer(new TransformedTokenizer<>(new CleaningTokenizer(), new CombinationsTokenizerTransformer(i))));
    }

    private static BlockingScheme<ProductDesc> desc_mw() throws ExecutionException {
        return new TokenBlockingScheme<>(new DescriptionTokenizer(new ModelWordsTokenizer()));
    }

    private static BlockingScheme<ProductDesc> desc_cl() throws ExecutionException {
        return new TokenBlockingScheme<>(new DescriptionTokenizer(new CleaningTokenizer()));
    }

    private static BlockingScheme<ProductDesc> t_mw_comb(final int i) throws ExecutionException {
        return new TokenBlockingScheme<>(new TitleTokenizer(new TransformedTokenizer<>(new ModelWordsTokenizer(), new CombinationsTokenizerTransformer(i))));
    }

    private static BlockingScheme<ProductDesc> t_cl_comb(final int i) throws ExecutionException {
        return new TokenBlockingScheme<>(new TitleTokenizer(new TransformedTokenizer<>(new CleaningTokenizer(), new CombinationsTokenizerTransformer(i))));
    }

    private static BlockingScheme<ProductDesc> t_mw() throws ExecutionException {
        return new TokenBlockingScheme<>(new TitleTokenizer(new ModelWordsTokenizer()));
    }

    private static BlockingScheme<ProductDesc> t_cl() throws ExecutionException {
        return new TokenBlockingScheme<>(new TitleTokenizer(new CleaningTokenizer()));
    }
}
