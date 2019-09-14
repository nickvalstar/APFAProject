package com.damirvandic.sparker.marnix;

import com.damirvandic.sparker.core.ClusteringProcedure;
import com.damirvandic.sparker.core.Clusters;
import com.damirvandic.sparker.core.ProductDesc;
import com.damirvandic.sparker.util.IntPair;
import gnu.trove.map.TIntObjectMap;
import gnu.trove.map.hash.TIntObjectHashMap;
import java.util.*;

/**
 * Created by Marnix on 28-12-2014.
 */
public class TestSpectralClusteringProcedure {

    public static void main(String[] args) {
        ClusteringProcedure clustering = new SpectralClusteringProcedure();
        TIntObjectMap<ProductDesc> data = getTestProductData();
        Clusters clusters = clustering.createClusters(getTestConf(), getTestSim(), data);
        for (Set<ProductDesc> cluster : clusters.asJavaSet()) {
            System.out.println(cluster);
        }
    }

    private static Map<IntPair, Double> getTestSim() {
        Map<IntPair, Double> ret = new HashMap<>();
//        ret.put(new IntPair(1,2), 0.9); // volgorde maakt niet uit van IDs
        ret.put(new IntPair(0, 1), 0.1);
        ret.put(new IntPair(0, 2), 0.1);
        ret.put(new IntPair(0, 3), 0.15);
        ret.put(new IntPair(0, 4), 0.05);
        ret.put(new IntPair(0, 5), 0.12);
        ret.put(new IntPair(0, 6), 0.15);
        ret.put(new IntPair(0, 7), 0.05);
        ret.put(new IntPair(1, 2), 0.16);
        ret.put(new IntPair(1, 3), 0.3);
        ret.put(new IntPair(1, 4), 0.12);
        ret.put(new IntPair(1, 5), 0.9);
        ret.put(new IntPair(1, 6), 0.12);
        ret.put(new IntPair(1, 7), 0.15);
        ret.put(new IntPair(2, 3), 0.4);
        ret.put(new IntPair(2, 4), 0.12);
        ret.put(new IntPair(2, 5), 0.15);
        ret.put(new IntPair(2, 6), 0.22);
        ret.put(new IntPair(2, 7), 0.85);
        ret.put(new IntPair(3, 4), 0.09);
        ret.put(new IntPair(3, 5), 0.1);
        ret.put(new IntPair(3, 6), 0.06);
        ret.put(new IntPair(3, 7), 0.03);
        ret.put(new IntPair(4, 5), 0.15);
        ret.put(new IntPair(4, 6), 0.11);
        ret.put(new IntPair(4, 7), 0.12);
        ret.put(new IntPair(5, 6), 0.15);
        ret.put(new IntPair(5, 7), 0.13);
        ret.put(new IntPair(6, 7), 0.2);
        return ret;
    }

    private static TIntObjectMap<ProductDesc> getTestProductData() {
        TIntObjectMap<ProductDesc> ret = new TIntObjectHashMap<>();
        ret.put(0, product(0, "bestbuy.com/prod0"));
        ret.put(1, product(1, "bestbuy.com/prod1"));
        ret.put(2, product(2, "bestbuy.com/prod2"));
        ret.put(3, product(3, "bestbuy.com/prod3"));
        ret.put(4, product(4, "amazon.com/prod4"));
        ret.put(5, product(5, "amazon.com/prod5"));
        ret.put(6, product(6, "amazon.com/prod6"));
        ret.put(7, product(7, "amazon.com/prod7"));
        return ret;
    }

    private static ProductDesc product(int id, String shop) {
        return new ProductDesc(id, "prod" + id, "", shop, new HashMap<String, String>());
    }

    private static Map<String, Object> getTestConf() {
        Map<String, Object> ret = new HashMap<>();
        ret.put("alpha", 0.2);
//        ret.put("edgeThreshold", 0.6);
        ret.put("method", "divmerge");
        ret.put("useWebShop", true);
        return ret;
    }



}
