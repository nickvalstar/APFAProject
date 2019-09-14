package com.damirvandic.sparker.msm;

import com.google.common.base.Predicate;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Maps;

import java.util.List;
import java.util.Map;

public class MsmConfig {
    private static final List<String> selectedKeys = ImmutableList.of("alpha", "beta", "kappa", "lambda", "gamma", "mu", "brandHeuristic");
    public static final Predicate<String> KEY_PREDICATE = new Predicate<String>() {
        @Override
        public boolean apply(String input) {
            return selectedKeys.contains(input);
        }
    };

    public static Map<String, Object> filterCacheEffectiveKeys(Map<String, Object> conf) {
        return Maps.filterKeys(conf, KEY_PREDICATE);
    }
}
