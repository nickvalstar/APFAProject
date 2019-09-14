package com.damirvandic.sparker.students.group7;

import com.damirvandic.sparker.core.SparkRunner;

public class Group7Runner {
    public static void main(String[] args) {

        args = new String[]{
                "-p:alpha=d 0.6",
                "-p:beta=d 0.0",
                "-p:gamma=d 0.72",
                "-p:epsilon=d 0.53",
                "-p:mu=d 0.65",
                "-p:kappa=d 1.0",
                "-p:lambda=d 1.0",
                "-p:shopHeuristic=b true",
                "-p:brandHeuristic=b true",
                "-p:jaccardSimilarity=d 0.5",
                "-p:keyMatcher=s Group7KeyMatcher",
                "-b",
                "5",
                "com.damirvandic.sparker.students.group7.Group7Factory",
                "TV's",
                "./hdfs/TVs-all-merged.json",
                "./hdfs/results",
                "../nocaching"
        };
        SparkRunner runner = new SparkRunner(args);
        runner.run();
        runner.stop();
    }
}
