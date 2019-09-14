package com.damirvandic.sparker.students.nick;

import com.damirvandic.sparker.core.ProductDesc;
import com.damirvandic.sparker.msm.KeyMatcher;
import com.damirvandic.sparker.util.StringPair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.util.*;

public class NickKeyMatcher implements KeyMatcher {
    private final Logger log = LoggerFactory.getLogger(getClass());
    public static Map<String,Map> golden_alignments = new HashMap();

    @Override
    public String toString() {
        return "NickKeyMatcher";
    }

    public NickKeyMatcher(KeyMatcher msmKeyMatcher, Map<String, Object> conf, Set<ProductDesc> data) {
        create_golden_alignments("./hdfs/IAA2.csv");
        log.info("golden standard created");
        new NickKeyMatchingAlg(conf, data);
        log.info("KeyMatcher created");
    }


    @Override
    public boolean keyMatches(String shopA, String keyA, String shopB, String keyB) {
        //not using this method anymore.
        return false;
    }



    @Override
    public double keySimilarity(String shopA, String keyA, String shopB, String keyB) {
        //not using this method anymore.
        return -1;
    }

    public static boolean is_aligned(Map<String, Map> map, String w1, String w2, String k1, String k2){
        boolean is_aligned = false;
        if (!map.containsKey(w1) || !map.get(w1).containsKey(w2)){
            return(is_aligned);
        }
        Map<String, String> tempmap = (Map<String, String>) map.get(w1).get(w2);
        if (tempmap.containsKey(k1))
            is_aligned = tempmap.get(k1).equals(k2);
        return(is_aligned);
    }

    public static void create_golden_alignments(String filename){
        BufferedReader reader = null;
        String line;
        try {
            reader = new BufferedReader(new FileReader(filename));
            Scanner scanner = null;
            try {
                while ((line = reader.readLine()) != null) {
                    scanner = new Scanner(line);
                    scanner.useDelimiter(";");
                    String w1 = scanner.next();
                    String k1 = scanner.next();
                    String w2 = scanner.next();
                    String k2 = scanner.next();

                    NickKeyMatchingAlg.addMatchingKey(golden_alignments, w1, w2, k1, k2);
                }
                //close reader
                reader.close();
            } catch (IOException e) {
                e.printStackTrace();
            }
        } catch (FileNotFoundException e) {
          e.printStackTrace();
        }
    }
}
