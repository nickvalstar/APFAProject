package com.damirvandic.sparker.students.nick;

import com.damirvandic.sparker.core.SparkRunner;

public class NickRunner {
    public static void main(String[] args) {
        if(args.length == 0){
            args = new String[]{
                    "-p:aligned_min=d 4",
                    "-p:epsilon=d 0.2 ",
                    "-p:keys_rest_weight=d 0.2 ",
                    "-p:mu=d 0.6",
                    "-p:title_min=d 2 ",
                    "-p:title_rest_weight=d 0.65 ",
                    "-p:epsilon2=d 999",
                    "-p:shopHeuristic=b true",
                    "-p:brandHeuristic=b true",
                    "-p:keyMatcher=s NickKeyMatcher",
                    //i dont use these below (except nr of bootstraps):
                    "-p:alpha=d 0.57",
                    "-p:beta=d 0.0",
                    "-p:gamma=d 0.70",
                    "-p:kappa=d 1.0",
                    "-p:lambda=d 1.0",
                    "-b",
                    "1",
                    "com.damirvandic.sparker.students.nick.NickFactory",
                    "TV's",
                    "./hdfs/TVs-all-merged.json",
                    "./hdfs/results",
                    "../caches/nocaching"
            };
        }
        SparkRunner runner = new SparkRunner(args);
        runner.run();
        runner.stop();
    }
}
