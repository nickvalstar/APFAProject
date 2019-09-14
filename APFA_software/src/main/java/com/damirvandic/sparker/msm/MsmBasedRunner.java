package com.damirvandic.sparker.msm;

import com.damirvandic.sparker.core.SparkRunner;

public class MsmBasedRunner {
    public static void main(String[] args) {
        if(args.length == 0){
            args = new String[]{
                    "-p:alpha=d 0.57",
                    "-p:beta=d 0.0",
                    "-p:gamma=d 0.70",
                    "-p:epsilon=d 0.53",
                    "-p:mu=d 0.61",
                    "-p:kappa=d 1.0",
                    "-p:lambda=d 1.0",
                    "-p:shopHeuristic=b true",
                    "-p:brandHeuristic=b true",
                    "-b",
                    "5",
//                "com.damirvandic.sparker.algorithms.HCSingleClusteringMsmFactory",
//                "com.damirvandic.sparker.algorithms.MsmClusteringBasedMsmFactory",
                    "com.damirvandic.sparker.algorithms.Algorithms$t_mw3_x_desc_mw6_Factory",
//                    "com.damirvandic.sparker.algorithms.Algorithms$PerfectMSMClustering",
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
