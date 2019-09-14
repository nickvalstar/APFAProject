package com.damirvandic.sparker.algorithms;

import com.damirvandic.sparker.algorithm.Algorithm;
import com.damirvandic.sparker.algorithm.AlgorithmFactory;
import com.damirvandic.sparker.blocking.schemes.AllPairsScheme;
import com.damirvandic.sparker.blocking.schemes.BlockingSchemes;
import com.damirvandic.sparker.blocking.schemes.TokenBlockingScheme;
import com.damirvandic.sparker.blocking.tokenizers.*;
import com.damirvandic.sparker.core.PerfectSimilarity;
import com.damirvandic.sparker.marnix.FitnessBasedClusteringProcedure;
import com.damirvandic.sparker.msm.MsmBasedAlgorithm;
import com.damirvandic.sparker.msm.MsmClusteringProcedure;

import java.util.Map;

public class Algorithms {
    public static class PerfectMSMClustering implements AlgorithmFactory {
        @Override
        public Algorithm build(String cachingPath, Map<String, String> hadoopConfig) {
            return new MsmBasedAlgorithm("perf.msms", new MsmClusteringProcedure(), cachingPath, hadoopConfig, new AllPairsScheme(), new PerfectSimilarity());
        }
    }

    public static class PerfectFitnessBasedClustering implements AlgorithmFactory {
        @Override
        public Algorithm build(String cachingPath, Map<String, String> hadoopConfig) {
            return new MsmBasedAlgorithm("perf.fitness", new FitnessBasedClusteringProcedure(), cachingPath, hadoopConfig, new AllPairsScheme(), new PerfectSimilarity());
        }
    }

    public static class all_Factory extends AbstractBlockingBasedMsmFactory {
        public all_Factory() {
            super(new AllPairsScheme(), new MsmClusteringProcedure());
        }
    }

    public static class t_cl6_Factory extends AbstractBlockingBasedMsmFactory {
        public t_cl6_Factory() {
            super(
                    new TokenBlockingScheme<>(
                            new TransformedTokenizer<>(
                                    new TitleTokenizer(new CleaningTokenizer()),
                                    new CombinationsTokenizerTransformer(6))),
                    new MsmClusteringProcedure());
        }
    }

    public static class t_cl5_Factory extends AbstractBlockingBasedMsmFactory {
        public t_cl5_Factory() {
            super(
                    new TokenBlockingScheme<>(
                            new TransformedTokenizer<>(
                                    new TitleTokenizer(new CleaningTokenizer()),
                                    new CombinationsTokenizerTransformer(5))),
                    new MsmClusteringProcedure());
        }
    }

    public static class t_cl6_plus_desc_mw2_Factory extends AbstractBlockingBasedMsmFactory {
        public t_cl6_plus_desc_mw2_Factory() {
            super(
                    BlockingSchemes.union(
                            new TokenBlockingScheme<>(
                                    new TransformedTokenizer<>(
                                            new TitleTokenizer(new CleaningTokenizer()),
                                            new CombinationsTokenizerTransformer(6))
                            ),
                            new TokenBlockingScheme<>(
                                    new TransformedTokenizer<>(
                                            new DescriptionTokenizer(new ModelWordsTokenizer()),
                                            new CombinationsTokenizerTransformer(2)))),
                    new MsmClusteringProcedure());
        }
    }

    public static class t_cl5_plus_desc_mw2_Factory extends AbstractBlockingBasedMsmFactory {
        public t_cl5_plus_desc_mw2_Factory() {
            super(
                    BlockingSchemes.union(
                            new TokenBlockingScheme<>(
                                    new TransformedTokenizer<>(
                                            new TitleTokenizer(new CleaningTokenizer()),
                                            new CombinationsTokenizerTransformer(5)
                                    )
                            ),
                            new TokenBlockingScheme<>(
                                    new TransformedTokenizer<>(
                                            new DescriptionTokenizer(new ModelWordsTokenizer()),
                                            new CombinationsTokenizerTransformer(2))
                            )
                    ),
                    new MsmClusteringProcedure());
        }
    }

    public static class t_cl5_plus_desc_mw3_Factory extends AbstractBlockingBasedMsmFactory {
        public t_cl5_plus_desc_mw3_Factory() {
            super(
                    BlockingSchemes.union(
                            new TokenBlockingScheme<>(
                                    new TransformedTokenizer<>(
                                            new TitleTokenizer(new CleaningTokenizer()),
                                            new CombinationsTokenizerTransformer(5)
                                    )
                            ),
                            new TokenBlockingScheme<>(
                                    new TransformedTokenizer<>(
                                            new DescriptionTokenizer(new ModelWordsTokenizer()),
                                            new CombinationsTokenizerTransformer(3))
                            )
                    ),
                    new MsmClusteringProcedure());
        }
    }

    public static class t_mw3_Factory extends AbstractBlockingBasedMsmFactory {
        public t_mw3_Factory() {
            super(
                    new TokenBlockingScheme<>(
                            new TransformedTokenizer<>(
                                    new TitleTokenizer(new ModelWordsTokenizer()),
                                    new CombinationsTokenizerTransformer(3))),
                    new MsmClusteringProcedure());
        }
    }

    public static class t_mw2_Factory extends AbstractBlockingBasedMsmFactory {
        public t_mw2_Factory() {
            super(
                    new TokenBlockingScheme<>(
                            new TransformedTokenizer<>(
                                    new TitleTokenizer(new ModelWordsTokenizer()),
                                    new CombinationsTokenizerTransformer(2))),
                    new MsmClusteringProcedure());
        }
    }

    public static class t_mw3_x_desc_cl_Factory extends AbstractBlockingBasedMsmFactory {
        public t_mw3_x_desc_cl_Factory() {
            super(
                    BlockingSchemes.product(
                            new TokenBlockingScheme<>(
                                    new TransformedTokenizer<>(
                                            new TitleTokenizer(new ModelWordsTokenizer()),
                                            new CombinationsTokenizerTransformer(3))
                            ),
                            new TokenBlockingScheme<>(
                                    new DescriptionTokenizer(new CleaningTokenizer()))),
                    new MsmClusteringProcedure());
        }
    }

    public static class t_mw2_x_desc_cl_Factory extends AbstractBlockingBasedMsmFactory {
        public t_mw2_x_desc_cl_Factory() {
            super(
                    BlockingSchemes.product(
                            new TokenBlockingScheme<>(
                                    new TransformedTokenizer<>(
                                            new TitleTokenizer(new ModelWordsTokenizer()),
                                            new CombinationsTokenizerTransformer(2))
                            ),
                            new TokenBlockingScheme<>(
                                    new DescriptionTokenizer(new CleaningTokenizer()))),
                    new MsmClusteringProcedure());
        }
    }

    public static class t_mw2_x_desc_mw_Factory extends AbstractBlockingBasedMsmFactory {
        public t_mw2_x_desc_mw_Factory() {
            super(
                    BlockingSchemes.product(
                            new TokenBlockingScheme<>(
                                    new TransformedTokenizer<>(
                                            new TitleTokenizer(new ModelWordsTokenizer()),
                                            new CombinationsTokenizerTransformer(2))
                            ),
                            new TokenBlockingScheme<>(
                                    new DescriptionTokenizer(new ModelWordsTokenizer()))),
                    new MsmClusteringProcedure());
        }
    }

    public static class t_mw3_x_desc_mw_Factory extends AbstractBlockingBasedMsmFactory {
        public t_mw3_x_desc_mw_Factory() {
            super(
                    BlockingSchemes.product(
                            new TokenBlockingScheme<>(
                                    new TransformedTokenizer<>(
                                            new TitleTokenizer(new ModelWordsTokenizer()),
                                            new CombinationsTokenizerTransformer(3))
                            ),
                            new TokenBlockingScheme<>(
                                    new DescriptionTokenizer(new ModelWordsTokenizer()))),
                    new MsmClusteringProcedure());
        }
    }

    public static class t_mw3_plus_desc_mw2_Factory extends AbstractBlockingBasedMsmFactory {
        public t_mw3_plus_desc_mw2_Factory() {
            super(
                    BlockingSchemes.union(
                            new TokenBlockingScheme<>(
                                    new TransformedTokenizer<>(
                                            new TitleTokenizer(new ModelWordsTokenizer()),
                                            new CombinationsTokenizerTransformer(3))
                            ),
                            new TokenBlockingScheme<>(
                                    new TransformedTokenizer<>(
                                            new DescriptionTokenizer(new ModelWordsTokenizer()),
                                            new CombinationsTokenizerTransformer(2))
                            )
                    ),
                    new MsmClusteringProcedure());
        }
    }

    public static class t_mw3_x_desc_mw5_Factory extends AbstractBlockingBasedMsmFactory {
        public t_mw3_x_desc_mw5_Factory() {
            super(
                    BlockingSchemes.product(
                            new TokenBlockingScheme<>(
                                    new TransformedTokenizer<>(
                                            new TitleTokenizer(new ModelWordsTokenizer()),
                                            new CombinationsTokenizerTransformer(3))
                            ),
                            new TokenBlockingScheme<>(
                                    new TransformedTokenizer<>(
                                            new DescriptionTokenizer(new ModelWordsTokenizer()),
                                            new CombinationsTokenizerTransformer(5))
                            )
                    ),
                    new MsmClusteringProcedure());
        }
    }

    public static class t_mw3_x_desc_mw6_Factory extends AbstractBlockingBasedMsmFactory {
        public t_mw3_x_desc_mw6_Factory() {
            super(
                    BlockingSchemes.product(
                            new TokenBlockingScheme<>(
                                    new TransformedTokenizer<>(
                                            new TitleTokenizer(new ModelWordsTokenizer()),
                                            new CombinationsTokenizerTransformer(3))
                            ),
                            new TokenBlockingScheme<>(
                                    new TransformedTokenizer<>(
                                            new DescriptionTokenizer(new ModelWordsTokenizer()),
                                            new CombinationsTokenizerTransformer(6))
                            )
                    ),
                    new MsmClusteringProcedure());
        }
    }

    public static class t_mw2_plus_desc_mw2_Factory extends AbstractBlockingBasedMsmFactory {
        public t_mw2_plus_desc_mw2_Factory() {
            super(
                    BlockingSchemes.union(
                            new TokenBlockingScheme<>(
                                    new TransformedTokenizer<>(
                                            new TitleTokenizer(new ModelWordsTokenizer()),
                                            new CombinationsTokenizerTransformer(2))
                            ),
                            new TokenBlockingScheme<>(
                                    new TransformedTokenizer<>(
                                            new DescriptionTokenizer(new ModelWordsTokenizer()),
                                            new CombinationsTokenizerTransformer(2))
                            )
                    ),
                    new MsmClusteringProcedure());
        }
    }

}
