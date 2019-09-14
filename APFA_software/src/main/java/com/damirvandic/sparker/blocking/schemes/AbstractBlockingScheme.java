package com.damirvandic.sparker.blocking.schemes;

import com.damirvandic.sparker.blocking.core.Block;
import com.damirvandic.sparker.blocking.core.BlocksMapping;
import com.google.common.base.Preconditions;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Set;

public abstract class AbstractBlockingScheme<T> implements BlockingScheme<T> {
    public static final Block EMPTY_BLOCK_ID = new Block("__empty_block__");
    public static final int DEFAULT_MAX_CACHE_SIZE = 10000;
    private final Logger log = LoggerFactory.getLogger(getClass());
    private final LoadingCache<T, Set<Block>> cache;

    protected AbstractBlockingScheme() {
        this(DEFAULT_MAX_CACHE_SIZE);
    }

    protected AbstractBlockingScheme(int maxCacheSize) {
        Preconditions.checkArgument(maxCacheSize > 0); //TODO: temp check
        this.cache = maxCacheSize == 0 ? null : CacheBuilder.newBuilder()
                .maximumSize(maxCacheSize)
                .recordStats()
                .build(new CacheLoader<T, Set<Block>>() {
                    @Override
                    public Set<Block> load(T p) {
                        return getBlocks(p);
                    }
                });
    }

    @Override
    public BlocksMapping<T> getBlocks(Set<T> data) {
        BlocksMapping.Builder<T> builder = BlocksMapping.builder();
        boolean cachePresent = cache != null;
        for (T entity : data) {
            Set<Block> blocks = cachePresent ? cache.getUnchecked(entity) : getBlocks(entity);
            if (blocks.isEmpty()) {
                builder.add(entity, EMPTY_BLOCK_ID);
            } else {
                for (Block block : blocks) {
                    builder.add(entity, block);
                }
            }
        }

        BlocksMapping<T> ret = builder.build();
        String componentID = componentID();
        log.debug("Scheme '{}' produced {} blocks for {} entities", componentID, ret.blockCount(), data.size());
        if (cachePresent) {
            cache.cleanUp();
            log.debug("Cache stats for scheme '{}': {}", componentID, cache.stats());
        }
        return ret;
    }

    @Override
    public void clearCache() {
        if (cache != null) {
            log.info("Clearing cache for {}, stats: {}", componentID(), cache.stats());
            cache.invalidateAll();
            cache.cleanUp();
        }
    }

    public abstract Set<Block> getBlocks(T entity);
}
