package com.damirvandic.sparker.blocking.tokenizers;

import com.damirvandic.sparker.blocking.core.Block;
import com.google.common.cache.LoadingCache;
import com.google.common.collect.ImmutableSet;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Set;

public abstract class AbstractCachingTokenizer<T> implements Tokenizer<T> {
    public static final int DEFAULT_CACHE_CLEAN_INTERVAL = 100000;
    public static final int DEFAULT_MAX_CACHE_SIZE = 1000000;
    private final Logger log = LoggerFactory.getLogger(getClass());
    private final LoadingCache<T, Set<Block>> cache;
    private final int cacheCleanInterval;
    private int tick;

    protected AbstractCachingTokenizer(int maxCacheSize, int cacheCleanInterval) {
        this.cacheCleanInterval = cacheCleanInterval;
        this.tick = 0;
        this.cache = null; // TODO: temp
//        this.cache = maxCacheSize > 0 ? CacheBuilder.newBuilder()
//                .maximumSize(maxCacheSize)
//                .recordStats()
//                .build(
//                        new CacheLoader<T, Set<Block>>() {
//                            @Override
//                            public Set<Block> load(T obj) {
//                                return extractBlocks(obj);
//                            }
//                        }) : null;
    }

    protected AbstractCachingTokenizer() {
        this(DEFAULT_MAX_CACHE_SIZE, DEFAULT_CACHE_CLEAN_INTERVAL);
    }

    @Override
    public Tokenizer pipe(final TokenizerTransformer t) {
        return new TransformedTokenizer<>(this, t);
    }

    @Override
    public Set<Block> tokenize(T obj) {
        Set<Block> ret = cache != null ? cache.getUnchecked(obj) : extractBlocks(obj);
        if (cache != null && ++tick % cacheCleanInterval == 0) {
            cache.cleanUp();
            log.debug("Cache stats: " + cache.stats());
            tick = 0;
        }
        return ret;
    }

    protected abstract Set<Block> extractBlocks(T obj);

    protected Set<Block> toBlocks(String[] values) {
        ImmutableSet.Builder<Block> builder = ImmutableSet.builder();
        for (String value : values) {
            builder.add(new Block(value));
        }
        return builder.build();
    }

    protected Set<Block> toBlocks(List<String> values) {
        ImmutableSet.Builder<Block> builder = ImmutableSet.builder();
        for (String value : values) {
            builder.add(new Block(value));
        }
        return builder.build();
    }

    @Override
    public void clearCache() {
        if (cache != null) {
            log.info("Clearing cache for {}, stats: {}", componentID(), cache.stats());
            cache.invalidateAll();
            cache.cleanUp();
        }
    }
}
