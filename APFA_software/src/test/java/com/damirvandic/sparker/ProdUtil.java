package com.damirvandic.sparker;

import com.damirvandic.sparker.core.ProductDesc;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

public class ProdUtil {

    public static final AtomicInteger counter = new AtomicInteger();
    public static final Map<String, String> EMPTY = new HashMap<>();
    private static LoadingCache<String, Integer> cacheCounter = CacheBuilder.newBuilder().build(new CacheLoader<String, Integer>() {
        @Override
        public Integer load(String prodName) throws Exception {
            return counter.getAndIncrement();
        }
    });

    public static ProductDesc p(int id) {
        String name = "p" + id;
        return new ProductDesc(id, name, "model_" + name, "http://myshop.com/" + name, EMPTY);
    }

    public static ProductDesc ps(String shopPrefixedName) {
        String shopURL = String.format("http://%s.com", shopPrefixedName.charAt(0));
        String prodName = shopPrefixedName;
        return new ProductDesc(cacheCounter.getUnchecked(prodName), prodName, "model_" + prodName, shopURL, EMPTY);
    }
}
