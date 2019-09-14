package com.damirvandic.sparker.students.group6;

import com.damirvandic.sparker.core.SparkRunner;

public class Group6Runner {
    public static void main(String[] args) {
        args = new String[]{
                // msm parameters
                "-p:alpha=d 0.6",
                "-p:beta=d 0.0",
                "-p:gamma=d 0.72",
                "-p:epsilon=d 0.53",
                "-p:mu=d 0.65",
                "-p:kappa=d 1.0",
                "-p:lambda=d 1.0",
                "-p:shopHeuristic=b true",
                "-p:brandHeuristic=b true",
                // your parameters
                "-b",
                "5",
                "com.damirvandic.sparker.students.group6.Algorithms$Group6_40_50",
                "TV's",
                "./hdfs/TVs-all-merged.json",
                "./hdfs/results",
                "../caches/TVs"
        };
        SparkRunner runner = new SparkRunner(args);
        runner.run();
        runner.stop();
    }
    // alpha                [0.20 : 0.10 : 0.80]                      // coefficient for computing similarity between keys
    // beta                 [0.60 : 0.05 : 0.90]                      // threshold for type classification
    // gamma                [0.60 : 0.05 : 0.90]                      // threshold for key-clustering
    // delta                [0.10 : 0.05 : 0.50]                      // threshold for product-clustering
    // lshThreshold         [0.10 : 0.05 : 0.90]                      // threshold for similarity in LSH
    // minHashSize          [0.05 : 0.05 : 0.50]
}