package com.damirvandic.sparker.marnix;

import com.damirvandic.sparker.core.SparkRunner;

public class MarnixRunner {
    public static void main(String[] args) {
        if(args.length == 0){
            args = new String[]{
//                    "-p:alpha=d 0.57",
                    "-p:alpha=d 0.6",
                    "-p:beta=d 0.0",
//                    "-p:gamma=d 0.70",
                    "-p:gamma=d 0.72",
                    "-p:epsilon=d 0.53",
//                    "-p:mu=d 0.61",
                    "-p:mu=d 0.65",
                    "-p:kappa=d 1.0",
                    "-p:lambda=d 1.0",
                    "-p:shopHeuristic=b true",
                    "-p:brandHeuristic=b true",
                    "-p:fitnessAlpha=d 0.1 0.3 0.5 0.75 1.0 3.0 6.0",
                    "-p:edgeThreshold=d 0.47 0.48 0.49 0.5 0.51 0.52 0.53",
                    "-p:method=s wtn",
                    "-p:fitnessFunction=s lanc",
//                    "-p:useBrands=b true",
                    "-b",
                    "5",
//                "com.damirvandic.sparker.algorithms.HCSingleClusteringMsmFactory",
//                "com.damirvandic.sparker.algorithms.MsmClusteringBasedMsmFactory",
                    "com.damirvandic.sparker.marnix.MarnixMsmFactory",
//                    "com.damirvandic.sparker.algorithms.Algorithms$PerfectFitnessBasedClustering",
                    "TV's",
                    "./hdfs/TVs-all-merged.json",
                    "./hdfs/results",
                    "../caches/TVs"
            };
        }
        SparkRunner runner = new SparkRunner(args);
        runner.run();
        runner.stop();
    }
}
